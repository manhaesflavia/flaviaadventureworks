with stg_customer as (
    select 
        *
    from {{ ref('stg__customer') }}
)

, int_personinformation as (
    select 
        *
    from {{ ref('int__personinformation') }}
)

, transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['stg_customer.pk_customer', 'int_personinformation.fk_addresstype']
        ) }} as sk_customer
        , stg_customer.pk_customer
        , stg_customer.fk_person
        , stg_customer.fk_salesterritory
        , int_personinformation.full_name
        , int_personinformation.city
        , int_personinformation.fk_addresstype
        , int_personinformation.fk_stateprovince
        , int_personinformation.is_emailpromotion
        , int_personinformation.persontype
    from stg_customer
    left join int_personinformation 
        on stg_customer.fk_person = int_personinformation.pk_person
)

, stg_stateprovince as (
    select
         stateprovinceid as pk_stateprovince
         , countryregioncode as fk_countryregioncode
         , territoryid as fk_territory
         , name as stateprovince_name
    from {{ source('adventure_works_seeds', 'stateprovince') }}
)

select 
    transformed.*
    , sp.fk_countryregioncode
    , sp.stateprovince_name
from transformed
left join stg_stateprovince as sp
    on transformed.fk_stateprovince = sp.pk_stateprovince