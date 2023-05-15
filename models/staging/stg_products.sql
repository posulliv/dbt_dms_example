{{
    config(
        materialized = 'view'
    )
}}

select 
  {{ 
    dbt_utils.star(
      from=ref('stg_products_merged'),
      except=["to_delete"]
    )
  }}
from
  {{ ref('stg_products_merged') }}
where
  to_delete = false