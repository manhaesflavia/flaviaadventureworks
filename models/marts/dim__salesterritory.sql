with stg__salesorderheader as (
    select 
        distinct(fk_salesterritory)
    from {{ ref('stg__salesorderheader') }}
    where fk_salesterritory is not null
)

, stg__salesterritory as (
    select *
    from {{ ref('stg__salesterritory') }}
)

, transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['stg__salesorderheader.fk_salesterritory']
            ) }} as sk_salesterritory
        , stg__salesorderheader.fk_salesterritory
        , stg__salesterritory.*
    from stg__salesorderheader 
    left join stg__salesterritory 
        on stg__salesorderheader.fk_salesterritory = stg__salesterritory.pk_salesterritory
)

select * 
from transformed