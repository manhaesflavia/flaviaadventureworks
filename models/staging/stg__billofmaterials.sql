with
 source as (
    select
        billofmaterialsid as pk_billofmaterials
        , productassemblyid as fk_product
        , componentid
        , startdate
        , enddate
        , unitmeasurecode
        , bomlevel
        , perassemblyqty
    from {{ source('adventure_works_seeds','billofmaterials') }}
)

select *
from source