with stg__salesorderheadersalesreason as (
    select *
    from {{ ref('stg__salesorderheadersalesreason') }}
),

stg__salesreason as (
    select *
    from {{ ref('stg__salesreason') }}
),

joined_data as (
    select *
    from stg__salesorderheadersalesreason
    left join stg__salesreason
        on stg__salesorderheadersalesreason.fk_salesreason = stg__salesreason.pk_salesreason
),

aggregated_data as (
    select
        fk_salesorder as salesorderid,
        string_agg(salesreason_name, ', ') as reason_name_aggregated
    from joined_data
    group by fk_salesorder
),

transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['fk_salesorder', 'fk_salesreason']
        ) }} as sk_salesreason,
        jd.fk_salesorder,
        ad.reason_name_aggregated, 
        jd.reasontype
    from joined_data jd
    left join aggregated_data ad
        on jd.fk_salesorder = ad.salesorderid
)

select *
from transformed