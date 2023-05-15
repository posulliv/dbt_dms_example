{{
    config(
        materialized = 'view'
    )
}}

with

source as (
    select * from {{ source('dms', 'fullload')}}
),

renamed as (
    select
      cast(product_id as int) "product_id",
      category,
      product_name,
      cast(quantity_available as int) "quantity",
      cast(from_iso8601_timestamp(last_update_time) as timestamp(6)) "last_update_time"
    from source
)

select * from renamed