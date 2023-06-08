-- depends_on: {{ ref('stg_dms__products') }}

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'product_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
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
