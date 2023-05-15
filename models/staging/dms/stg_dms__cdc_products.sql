{{
    config(
        materialized = 'view'
    )
}}

with

source as (
    select * from {{ source('dms', 'cdc')}}
),

dedup as (
    select *
    from (
      select
        *,
        row_number() over (
          partition by product_id order by last_update_time desc
        ) as row_num
      from source
    )
    where row_num = 1
),

renamed as (
    select
      op,
      cast(product_id as int) "product_id",
      category,
      product_name,
      cast(quantity_available as int) "quantity",
      cast(from_iso8601_timestamp(last_update_time) as timestamp(6)) "last_update_time"
    from dedup
)

select * from renamed