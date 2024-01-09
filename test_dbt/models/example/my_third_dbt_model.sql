
-- Use the `ref` function to select from other models

select *
from {{ source('my_sources', 'my_sample_source') }}

