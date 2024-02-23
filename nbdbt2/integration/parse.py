import os
from dataclasses import dataclass
import glob
from pathlib import Path
from typing import Any, List, Dict, Optional, Union, TYPE_CHECKING

from dbt.contracts.project import Project as ProjectContract
from dbt.config import RuntimeConfig, PartialProject
from dbt.config.utils import parse_cli_vars as dbt_parse_cli_vars
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResultsArtifact, FreshnessExecutionResultArtifact
from dbt.contracts.project import UserConfig
from dbt.config.profile import read_user_config

from dbt.exceptions import IncompatibleSchemaError, DbtRuntimeError


from .logger import LOGGER
from .utils.yaml_helper import load_yaml


class ParseError(Exception):
    pass


def get_dbt_user_config(profiles_dir: str) -> UserConfig:
    return read_user_config(profiles_dir)


@dataclass
class RuntimeArgs:
    project_dir: str
    profiles_dir: str
    threads: Optional[int]
    single_threaded: bool
    profile: Optional[str]
    target: Optional[str]
    vars: Dict[str, str]


def load_dbt_project_contract(project_dir: str) -> ProjectContract:
    # partial_project = Project.partial_load(project_dir)
    partial_project = PartialProject.from_project_root(project_dir, verify_version=False)
    contract = ProjectContract.from_dict(partial_project.project_dict)
    if not hasattr(contract, "model_paths") or contract.model_paths is None:
        setattr(contract, "model_paths", contract.source_paths)
    if not hasattr(contract, "seed_paths") or contract.seed_paths is None:
        setattr(contract, "seed_paths", contract.data_paths)
    return contract


def get_dbt_config(
    *,
    project_dir: str,
    profiles_dir: str,
    profile_target: Optional[str] = None,
    threads: Optional[int] = None,
    profile: Optional[str] = None,
    args_vars: str = "{}",
) -> RuntimeConfig:
    # Construct a phony config
    import os

    vars = get_vars_dict(project_dir, args_vars)
    args = RuntimeArgs(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        threads=threads,
        single_threaded=False,
        profile=profile,
        target=profile_target,
        vars=vars,
    )

    if project_dir and not "PYTEST_CURRENT_TEST" in os.environ:
        # HACK: initializing dbt-fal requires cwd to be project_dir
        # TODO: this doesn't work in pytest + Github Actions
        owd = os.getcwd()
        os.chdir(project_dir)
        config = RuntimeConfig.from_args(args)
        os.chdir(owd)
    else:
        config = RuntimeConfig.from_args(args)

    # HACK: issue in dbt-core 1.5.0 https://github.com/dbt-labs/dbt-core/issues/7465
    env_target_path = os.getenv("DBT_TARGET_PATH")
    if env_target_path:
        config.target_path = env_target_path
    # TODO: should we check flags too?

    return config


def get_vars_dict(project_dir: str, args_vars: str) -> Dict[str, Any]:
    project_contract = load_dbt_project_contract(project_dir)

    # NOTE: This happens usually inside unit tests
    vars = (project_contract is not None and project_contract.vars) or {}
    cli_vars = parse_cli_vars(args_vars)

    # cli_vars have higher priority
    return {**vars, **cli_vars}

def parse_cli_vars(args_vars: str) -> Dict[str, Any]:
    if not args_vars:
        return {}

    try:
        return dbt_parse_cli_vars(args_vars)
    except DbtRuntimeError as exc:
        raise ParseError(exc)



def get_dbt_manifest(config) -> Manifest:
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)


def get_dbt_sources_artifact(project_dir: str, config: RuntimeConfig):
    # Changed in dbt 1.5.0 to use path relative to CWD instead of path relative to project_dir
    sources_path = os.path.join(config.target_path, "sources.json")
    try:
        return FreshnessExecutionResultArtifact.read_and_check_versions(sources_path)

    except IncompatibleSchemaError as exc:
        # TODO: add test for this case
        exc.add_filename(sources_path)
        raise
    except DbtRuntimeError as exc:
        LOGGER.warn(f"Could not read dbt sources artifact: {exc}")
        return None


def get_dbt_results(project_dir: str, config: RuntimeConfig) -> Optional[RunResultsArtifact]:
    # Changed in dbt 1.5.0 to use path relative to CWD instead of path relative to project_dir
    results_path = os.path.join(config.target_path, "run_results.json")
    try:
        return RunResultsArtifact.read_and_check_versions(results_path)

    except IncompatibleSchemaError as exc:
        # TODO: add test for this case
        exc.add_filename(results_path)
        raise
    except DbtRuntimeError as exc:
        LOGGER.warn("Could not read dbt run_results artifact")
        return None


def get_scripts_list(scripts_dir: str) -> List[str]:
    scripts_path = Path(scripts_dir)
    return list(map(str, [*scripts_path.rglob("*.py"), *scripts_path.rglob("*.ipynb")]))


def get_global_script_configs(source_dirs: List[Path]) -> Dict[str, List[str]]:
    global_scripts = {"before": [], "after": []}
    for source_dir in source_dirs:
        # Scan directories for .yml files
        schema_files = glob.glob(os.path.join(source_dir, "**.yml"), recursive=True)
        # Scan directories for .yaml files
        schema_files += glob.glob(os.path.join(source_dir, "**.yaml"), recursive=True)
        for file in schema_files:
            schema_yml = load_yaml(file)
            if schema_yml is not None:
                fal_config = schema_yml.get("fal", None)
                if fal_config is not None:
                    # sometimes `scripts` can *be* there and still be None
                    script_paths = fal_config.get("scripts") or []
                    if isinstance(script_paths, list):
                        global_scripts["after"] += script_paths
                    else:
                        global_scripts["before"] += script_paths.get("before") or []
                        global_scripts["after"] += script_paths.get("after") or []
            else:
                raise ParseError("Error parsing the schema file " + file)

    return global_scripts

def normalize_path(base: str, path: Union[Path, str]):
    real_base = os.path.realpath(os.path.normpath(base))
    return Path(os.path.realpath(os.path.join(real_base, path)))


def normalize_paths(
    base: str, paths: Union[List[Path], List[str], List[Union[Path, str]]]
):
    return list(map(lambda path: normalize_path(base, path), paths))
