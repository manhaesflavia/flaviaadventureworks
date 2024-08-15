with source as (
    select
         stateprovinceid as pk_stateprovince
         , countryregioncode as fk_countryregioncode
         , territoryid as fk_territory
         , name as stateprovince_name
    from {{ source('adventure_works_seeds','stateprovince') }}
)

select *
from source