with source as (
    select
        territoryid as pk_salesterritory
        , name as territory_name
        , countryregioncode
        , "group" as territory_group
        , salesytd
        , saleslastyear
    from {{ source('adventure_works_seeds', 'salesterritory') }}
)

select *
from source