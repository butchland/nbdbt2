-- AUTOGENERATED! DO NOT EDIT! File to edit: nbs/00_dbt_cellmagic.ipynb (unless otherwise specified).

select *
from {{ ref('my_second_dbt_model') }}
where id is not null
