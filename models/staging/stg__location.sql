with
 source as (
    select
        locationid as pk_location
        , name as location_name
        , availability
    from {{ source('adventure_works_seeds','location') }}
)

select *
from source     