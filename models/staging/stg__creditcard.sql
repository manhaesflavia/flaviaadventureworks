with
 source as (
    select
    creditcardid as pk_creditcard
    , cardtype

    from {{ source('adventure_works_seeds','creditcard') }}
)

select *
from source