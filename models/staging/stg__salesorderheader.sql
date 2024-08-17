with source as (
    select
        salesorderid as pk_salesorder
        , customerid as fk_customer
        , shiptoaddressid as fk_address
        , territoryid as fk_salesterritory
        , creditcardid as fk_creditcard
        , case
            when status = 1 then 'In process'
            when status = 2 then 'Approved'
            when status = 3 then 'Backordered'
            when status = 4 then 'Rejected'
            when status = 5 then 'Shipped'
            when status = 6 then 'Canceled'
        end as order_status
        , cast(orderdate as date) as orderdate
        , subtotal
        , taxamt
        , freight
    from {{ source('adventure_works_seeds', 'salesorderheader') }}
)

select *
from source