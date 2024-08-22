with customers as (
    select
        sk_customer
        , pk_customer
    from {{ ref('dim__customer') }}
),

creditcards as (
    select
        sk_creditcard
        , fk_creditcard
    from {{ ref('dim__creditcard') }}
),

products as (
    select
        sk_product
        , pk_product
    from {{ ref('dim__product') }}
),

sales_reasons as (
    select
        sk_salesreason
        , fk_salesorder
        , salesreason_name
    from {{ ref('dim__salesreason') }}
),

salesterritory as (
    select
        sk_salesterritory
        , fk_salesterritory
    from {{ ref('dim__salesterritory') }}
),

salesorderdetails as (
    select
        stg_salesorderdetail.fk_salesorder
        , stg_salesorderdetail.pk_salesorderdetail
        , products.sk_product
        , stg_salesorderdetail.fk_product
        , stg_salesorderdetail.orderqty
        , stg_salesorderdetail.unitprice
        , stg_salesorderdetail.unitpricediscount
        , stg_salesorderdetail.unitprice * stg_salesorderdetail.orderqty as gross_revenue
    from {{ ref('stg__salesorderdetail') }} as stg_salesorderdetail
    left join products on stg_salesorderdetail.fk_product = products.pk_product -- Ajuste no join
),

salesorderheaders as (
    select
        stg_salesorderheader.pk_salesorder
        , customers.sk_customer
        , stg_salesorderheader.fk_customer
        , stg_salesorderheader.fk_creditcard
        , creditcards.sk_creditcard
        , salesterritory.sk_salesterritory
        , stg_salesorderheader.order_date
        , stg_salesorderheader.order_status
    from {{ ref('stg__salesorderheader') }} as stg_salesorderheader
    left join customers on stg_salesorderheader.fk_customer = customers.pk_customer
    left join creditcards on stg_salesorderheader.fk_creditcard = creditcards.fk_creditcard
    left join salesterritory on stg_salesorderheader.fk_salesterritory = salesterritory.fk_salesterritory
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key (
            ['salesorderdetails.fk_salesorder', 'salesorderdetails.pk_salesorderdetail']
        ) }} as sk_factorder
        , salesorderdetails.fk_salesorder
        , salesorderdetails.sk_product as fk_product
        , salesorderheaders.fk_customer
        , salesorderheaders.fk_creditcard
        , salesorderheaders.sk_salesterritory
        , salesorderdetails.unitprice
        , salesorderdetails.orderqty
        , salesorderdetails.unitpricediscount
        , salesorderdetails.gross_revenue
        , salesorderheaders.order_date
        , salesorderheaders.order_status
        , sales_reasons.sk_salesreason -- Adicionando a SK da razão de vendas
        , case
            when sales_reasons.salesreason_name is null then 'Unknown'
            else sales_reasons.salesreason_name
        end as sales_reason_name          
    from salesorderdetails
    left join salesorderheaders 
        on salesorderdetails.fk_salesorder = salesorderheaders.pk_salesorder
    left join sales_reasons 
        on salesorderdetails.fk_salesorder = sales_reasons.fk_salesorder
)

select *
from final