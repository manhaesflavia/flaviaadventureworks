with
 source as (
    select
      customerid as pk_customer
		, personid as fk_person
		, storeid as fk_store
		, territoryid as fk_salesterritory
    from {{ source('adventure_works_seeds','customer') }}
)

select *
from source