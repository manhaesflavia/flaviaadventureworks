with
 source as (
    select
        billofmaterialsid as pk_billofmaterials
        , productassemblyid as fk_product
        , componentid
        , cast(startdate as timestamp) as start__date
        , cast(enddate as timestamp) as end__date
        , unitmeasurecode
        , bomlevel
        , perassemblyqty
    from {{ source('adventure_works_seeds','billofmaterials') }}
)

select *
from source