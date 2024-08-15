with stg__businessentityaddress as (
    select
        pk_person
        , pk_address
        , pk_addresstype
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
        , bea.pk_addresstype
        , addr.fk_stateprovince
        , addr.city
    from stg__person p
    left join stg__businessentityaddress bea
        on p.pk_person = bea.pk_person
    left join stg__address addr
        on bea.pk_address = addr.pk_address
)

select * from int__personinformation
