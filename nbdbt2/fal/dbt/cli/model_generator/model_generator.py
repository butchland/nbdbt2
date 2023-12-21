import ast
from functools import partial
import re
from typing import Callable, Iterable, List, TypeVar
from pathlib import Path
from fal.dbt.fal_script import python_from_file

from fal.dbt.integration.parse import get_fal_models_dirs, load_dbt_project_contract
from fal.dbt.cli.model_generator.module_check import (
    generate_dbt_dependencies,
    write_to_model_check,
)

from fal.dbt.integration.logger import LOGGER

from fal.dbt.telemetry import telemetry

SQL_MODEL_TEMPLATE = """
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED __checksum__

Script dependencies:

__deps__

*/

SELECT * FROM {{ this }}
"""

GENERATED_DIR = Path("fal")

CHECKSUM_REGEX = re.compile(r"FAL_GENERATED ([_\d\w]+)")


def generate_python_dbt_models(project_dir: str, args_vars: str):
    fal_models_paths = _get_fal_models_paths(project_dir, args_vars)
    dbt_models_paths = list(
        map(Path, load_dbt_project_contract(project_dir).model_paths or [])
    )

    base_dbt_models_dir = Path(project_dir, dbt_models_paths[0])

    old_generated_sqls = list(
        _process_models_paths(dbt_models_paths, _find_fal_generated_models)
    )

    fal_python_models_and_sqls = list(
        _process_models_paths(
            fal_models_paths,
            partial(_find_fal_python_models_and_sql_target, base_dbt_models_dir),
        )
    )

    python_paths: List[Path] = []
    fal_target_sqls: List[Path] = []
    if fal_python_models_and_sqls:
        python_paths, fal_target_sqls, _ = map(list, zip(*fal_python_models_and_sqls))

    _delete_old_generated_sqls(old_generated_sqls, fal_target_sqls)

    for py_path, sql_path, old_checksum in fal_python_models_and_sqls:
        new_checksum = _generate_sql_for_fal_python_model(py_path, sql_path)
        if not old_checksum or new_checksum != old_checksum:
            LOGGER.warn(
                f"File '{sql_path.relative_to(project_dir)}' was generated from '{py_path.relative_to(project_dir)}'.\n"
                "Please do not modify it directly. We recommend committing it to your repository."
            )

    if python_paths:
        telemetry.log_api(
            action="python_models_generated",
            additional_props={"models": len(python_paths)},
        )

    return {path.stem: path for path in python_paths}


def _get_fal_models_paths(project_dir: str, args_vars: str):
    models_paths = get_fal_models_dirs(project_dir, args_vars)
    project_path = Path(project_dir)
    return list(map(project_path.joinpath, models_paths))


def _generate_sql_for_fal_python_model(py_path: Path, sql_path: Path):
    source_code = python_from_file(py_path)
    module = ast.parse(source_code, str(py_path), "exec")

    # Fails if it does not have write_to_model
    write_to_model_check(module)

    dbt_deps = generate_dbt_dependencies(module)

    sql_contents = SQL_MODEL_TEMPLATE.replace("__deps__", dbt_deps)
    checksum, _ = _checksum(sql_contents)
    sql_contents = sql_contents.replace("__checksum__", checksum)

    sql_path.parent.mkdir(parents=True, exist_ok=True)
    with open(sql_path, "w") as file:
        file.write(sql_contents)

    return checksum


# TODO: unit tests
def _check_path_safe_to_write(sql_path: Path, py_path: Path):
    if sql_path.exists():
        with open(sql_path, "r") as file:
            contents = file.read()
            checksum, found = _checksum(contents)
            if not found or checksum != found:
                LOGGER.debug(
                    f"Existing file calculated checksum: {checksum}\nFound checksum: {found}"
                )
                raise RuntimeError(
                    f"File '{sql_path}' not generated by fal would be "
                    f"overwritten by generated model of '{py_path}'. Please rename or remove."
                )
            return checksum


T = TypeVar("T")


def _process_models_paths(
    models_paths: Iterable[Path], func: Callable[[Path], Iterable[T]]
) -> Iterable[T]:
    for models_path in models_paths:
        yield from func(models_path)


def _find_fal_python_models_and_sql_target(base_sql_path: Path, models_path: Path):
    for py_path in _find_python_files(models_path):
        sql_path = _sql_path_from_python_path(
            base_sql_path, py_path.relative_to(models_path)
        )
        old_checksum = _check_path_safe_to_write(sql_path, py_path)
        yield py_path, sql_path, old_checksum


def _sql_path_from_python_path(base_sql_path: Path, relative_py_path: Path):
    return base_sql_path / GENERATED_DIR / relative_py_path.with_suffix(".sql")


def _find_fal_generated_models(models_path: Path):
    fal_path = models_path / GENERATED_DIR
    return (file for file in fal_path.glob("**/*.sql") if _is_fal_generated(file))


def _delete_old_generated_sqls(old_models: Iterable[Path], new_models: Iterable[Path]):
    for sql_path in old_models:
        if sql_path not in new_models:
            sql_path.unlink()


def _is_fal_generated(file_path):
    with open(file_path) as file:
        return CHECKSUM_REGEX.search(file.read())


def _checksum(contents: str):
    import hashlib

    found = CHECKSUM_REGEX.search(contents)
    to_check = CHECKSUM_REGEX.sub("FAL_GENERATED", contents.strip())
    return (
        hashlib.md5(to_check.encode("utf-8")).hexdigest(),
        found.group(1) if found else None,
    )


def _find_python_files(models_path: Path) -> List[Path]:
    files = []
    files.extend(models_path.rglob("*.py"))
    files.extend(models_path.rglob("*.ipynb"))

    return [p for p in files if p.is_file()]
