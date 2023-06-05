-- depends_on: {{ ref('stg_dms__cdc_products') }}

{{
    config(
        materialized         = 'incremental',
        properties           = {
            "partitioning" : "ARRAY['month(last_update_time)']"
        },
        unique_key           = 'product_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
        post_hook            = [
            "DELETE FROM {{ this }} WHERE to_delete = true",
            "ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d')",
            "ANALYZE {{ this }}"
        ]
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
    and last_update_time > (
        select max(last_update_time)
        from {{ this }}
    )
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
    and last_update_time > (
        select max(last_update_time)
        from {{ this }}
    )
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
    and last_update_time > (
        select max(last_update_time)
        from {{ this }}
    )
)

select * from updates union all select * from inserts union all select * from deletes

{% else %}

select 
  {{ dbt_utils.star(from=ref('stg_dms__full_load_products')) }},
  false "to_delete"
from 
  {{ ref('stg_dms__full_load_products') }}

{% endif %}