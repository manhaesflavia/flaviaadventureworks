with source as (
    select
        salesorderdetailid as pk_salesorderdetail
        , salesorderid as fk_salesorder
        , productid as fk_product
        , orderqty
        , unitprice
        , unitpricediscount
    from {{ source('adventure_works_seeds','salesorderdetail') }}
)

select *
from source