-- depends_on: {{ ref('stg_dms__products') }}

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

with

updates as (
    select 
      {{
        dbt_utils.star(
            from=ref('stg_dms__products'),
            except=["op"]
        )
      }},
      false "to_delete"
    from 
      {{ ref('stg_dms__products') }}
    where op = 'U' 
    {% if is_incremental() %}
    and last_update_time > (
        select max(last_update_time)
        from {{ this }}
    )
    {% endif %}
),

deletes as (
    select 
      {{
        dbt_utils.star(
            from=ref('stg_dms__products'),
            except=["op"]
        )
      }},
      true "to_delete"
    from 
      {{ ref('stg_dms__products') }}
    where op = 'D'
    {% if is_incremental() %}
    and last_update_time > (
        select max(last_update_time)
        from {{ this }}
    )
    {% endif %}
),

inserts as (
    select 
      {{
        dbt_utils.star(
            from=ref('stg_dms__products'),
            except=["op"]
        )
      }},
      false "to_delete"
    from 
      {{ ref('stg_dms__products') }}
    where op = 'I'
    {% if is_incremental() %}
    and last_update_time > (
        select max(last_update_time)
        from {{ this }}
    )
    {% endif %}
)

select * from updates union all select * from inserts union all select * from deletes
