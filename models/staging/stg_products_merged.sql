-- depends_on: {{ ref('stg_dms__cdc_products') }}

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'product_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}

{% if is_incremental() %}

with

updates as (
    select 
      {{
        dbt_utils.star(
            from=ref('stg_dms__cdc_products'),
            except=["op"]
        )
      }},
      false "to_delete"
    from 
      {{ ref('stg_dms__cdc_products') }}
    where op = 'U'
),

deletes as (
    select 
      {{
        dbt_utils.star(
            from=ref('stg_dms__cdc_products'),
            except=["op"]
        )
      }},
      true "to_delete"
    from 
      {{ ref('stg_dms__cdc_products') }}
    where op = 'D'
),

inserts as (
    select 
      {{
        dbt_utils.star(
            from=ref('stg_dms__cdc_products'),
            except=["op"]
        )
      }},
      false "to_delete"
    from 
      {{ ref('stg_dms__cdc_products') }}
    where op = 'I'
)

select * from updates union all select * from inserts union all select * from deletes

{% else %}

select 
  {{ dbt_utils.star(from=ref('stg_dms__full_load_products')) }},
  false "to_delete"
from 
  {{ ref('stg_dms__full_load_products') }}

{% endif %}