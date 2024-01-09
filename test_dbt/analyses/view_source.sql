select *
from {{ source('my_sources', 'my_sample_source') }}