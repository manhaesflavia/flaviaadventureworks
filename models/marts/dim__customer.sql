with customer_orders as (
    select
        c.pk_customer
        , c.fk_person
    from {{ ref('stg__customer') }} c
    right join {{ ref('stg__salesorderheader') }} soh
        on c.pk_customer = soh.fk_customer
    where soh.fk_customer is not null
    group by c.pk_customer, c.fk_person
)

, customer_person_info as (
    select
        co.pk_customer
        , co.fk_person
        , pi.full_name
        , pi.city
        , pi.fk_addresstype
        , pi.fk_stateprovince
        , pi.is_emailpromotion
        , pi.persontype
    from customer_orders co
    left join {{ ref('int__personinformation') }} pi
        on co.fk_person = pi.pk_person
)

, transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['pk_customer']
        ) }} as sk_customer
        , pk_customer
        , fk_person
        , full_name
        , city
        , fk_addresstype
        , is_emailpromotion
        , persontype
    from customer_person_info
)

select *
from transformed