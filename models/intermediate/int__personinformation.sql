with stg__businessentityaddress as (
    select
        fk_person
        , pk_address
        , fk_addresstype
    from {{ ref('stg__businessentityaddress') }}
)

, stg__address as (
    select
        pk_address
        , fk_stateprovince
        , city
    from {{ ref('stg__address') }}
)

, stg__person as ( 
    select
        pk_person
        , persontype
        , full_name
        , is_emailpromotion
    from {{ ref('stg__person') }}
)

, int__personinformation as (
    select
        p.pk_person
        , p.persontype
        , p.full_name
        , p.is_emailpromotion
        , bea.pk_address
        , bea.fk_addresstype
        , addr.fk_stateprovince
        , addr.city
        , row_number() over (partition by p.pk_person order by bea.fk_addresstype) as rn
    from stg__person p
    left join stg__businessentityaddress bea
        on p.pk_person = bea.fk_person
    left join stg__address addr
        on bea.pk_address = addr.pk_address
)

select
    pk_person
    , persontype
    , full_name
    , is_emailpromotion
    , pk_address
    , fk_addresstype
    , fk_stateprovince
    , city
from int__personinformation
where rn = 1
order by fk_addresstype