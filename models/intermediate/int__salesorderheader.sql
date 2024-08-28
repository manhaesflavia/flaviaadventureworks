with int__salesorder as (
    select 
        {{ dbt_utils.generate_surrogate_key([
            'od.pk_salesorderdetail',
            'od.fk_salesorder'
        ]) }} as sk_salesorderdetail
        , od.pk_salesorderdetail
        , od.fk_salesorder
        , od.fk_product
        , od.orderqty
        , od.unitprice
        , od.unitpricediscount
        , oh.pk_salesorder
        , oh.fk_customer
        , oh.fk_address
        , oh.fk_salesterritory
        , oh.fk_creditcard
        , oh.is_onlineorderflag
        , oh.order_status
        , oh.order_date
        , oh.totaldue
        , oh.subtotal
        , oh.taxamt
        , oh.freight
        -- cálculo do valor total da compra por produto
        , od.unitprice * od.orderqty as total_order_value
    from 
        {{ ref('stg__salesorderdetail') }} as od
    left join 
        {{ ref('stg__salesorderheader') }} as oh
    on 
        od.fk_salesorder = oh.pk_salesorder
)

select * from int__salesorder