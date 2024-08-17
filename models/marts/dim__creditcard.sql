with stg__salesorderheader as (
    select 
        distinct(fk_creditcard)
    from {{ ref('stg__salesorderheader') }}
    where fk_creditcard is not null
)

, stg__creditcard as (
    select *
    from 
        {{ ref('stg__creditcard') }}
)

, transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['stg__salesorderheader.fk_creditcard']
            ) }} as sk_creditcard
        , stg__salesorderheader.fk_creditcard
        , stg__creditcard.cardtype
    from stg__salesorderheader 
    left join stg__creditcard 
        on stg__salesorderheader.fk_creditcard = stg__creditcard.pk_creditcard
)

select * 
from transformed