with
 source as (
    select
      countryregioncode
      , name as country_name

    from {{ source('adventure_works_seeds','countryregion') }}
)

select *
from source