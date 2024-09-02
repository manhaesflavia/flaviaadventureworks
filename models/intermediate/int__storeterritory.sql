with store_salesperson as (
    select
        store.fk_customer
        , store.store_name
        , salesperson.pk_sales_person
        , salesperson.fk_territory
    from
        {{ ref('stg__store') }} store
    left join
        {{ ref('stg__salesperson') }} salesperson
    on
        store.fk_person = salesperson.pk_sales_person
),

store_salesperson_territory as (
    select
        store_salesperson.fk_customer
        , store_salesperson.store_name
        , store_salesperson.pk_sales_person
        , salesterritory.pk_salesterritory
        , salesterritory.territory_name
        , salesterritory.fk_countryregioncode
        , salesterritory.territory_group
    from
        store_salesperson
    left join
        {{ ref('stg__salesterritory') }} salesterritory
    on
        store_salesperson.fk_territory = salesterritory.pk_salesterritory
)

select 
    fk_customer
    , store_name
    , pk_sales_person
    , pk_salesterritory
    , territory_name
    , fk_countryregioncode
    , territory_group
from 
    store_salesperson_territory