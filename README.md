# Overview

In this example, we are going to use data from Amazon DMS to demonstrate
how to use `dbt` and Trino with Iceberg for CDC on a data lake.

The data from DMS is in CSV format for this example. We will have 1 file
for the initial load that has the following contents:

```
I,100,Furniture,Product 1,25,2022-03-01T09:51:39.340396Z
I,101,Cosmetic,Product 2,20,2022-03-01T10:14:58.597216Z
I,102,Furniture,Product 3,30,2022-03-01T11:51:40.417052Z
I,103,Electronics,Product 4,10,2022-03-01T11:51:40.519832Z
I,104,Electronics,Product 5,50,2022-03-01T11:58:00.512679Z
```

Then we will have multiple CSV files for CDC records. The records scattered
through those files are:

```
I,105,Furniture,Product 5,45,2022-03-02T09:51:39.340396Z
I,106,Electronics,Product 6,10,2022-03-02T09:52:39.340396Z
U,102,Furniture,Product 3,29,2022-03-02T11:53:40.417052Z
U,102,Furniture,Product 3,28,2022-03-02T11:55:40.417052Z
D,103,Electronics,Product 4,10,2022-03-02T11:56:40.519832Z
```

# DMS Source Models

We are going to have 2 models for the source data from DMS:

* `full_load_products`
* `cdc_products`

These models read from tables created from the CSV files. The tables created
from the CSV files can either be created manually or via schema discovery.

The `cdc_products` model must ensure that only the latest update for a row
will be applied. This is achieved by just selecting the last operation for
each row in this CTE:

```
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
```

# Staging Models

We want to use an efficient incremental materialization for the `stg_products`
model.

The most efficient with Trino and Iceberg is to have `dbt` generate a `MERGE`
statement. We are targeting generating a `MERGE` statement such as:

```
MERGE INTO stg_products_merged spm
USING (SELECT * FROM cdc_products) cdc
    ON spm.id = cdc.id
WHEN MATCHED AND cdc.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

To have `dbt` generate a `MERGE` statement we set `incremental_strategy` to
the value `merge`. We also need to make sure the table is create from the 
full load table when doing the initial load or a full refresh.

So first to create the model from the full load table when doing a full refresh
we do:

```
{% if is_incremental() %}

...

{% else %}

select 
  {{ dbt_utils.star(from=ref('stg_dms__full_load_products')) }}
from 
  {{ ref('stg_dms__full_load_products') }}

{% endif %}
```

The `{% else %}` portion takes care of the full refresh.

Now based on [recommendations](https://docs.getdbt.com/guides/migration/tools/migrating-from-stored-procedures/5-merges)
from dbt documentations on how to a `MERGE` statement, we first need to write 
a CTE for inserts, updates, and deletes:

```
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
```

You can see the logic for the updates, inserts, and deletes CTEs is the same
except for the op value. This could be extracted to a macro.

Finally we want to union the CTEs. `dbt` will create a temporary view that is
joined in the generated `MERGE` statement. The generated `MERGE` will look
something like:

```
merge into stg_products_merged as DBT_INTERNAL_DEST
using stg_products_merged__dbt_tmp as DBT_INTERNAL_SOURCE
on (
  DBT_INTERNAL_SOURCE.product_id = DBT_INTERNAL_DEST.product_id
)
when matched then update set ...
when not matched then insert ...
```

There are some further considersations:

1. The model assumes that the `stg_dms__cdc_products` model has no 
   duplicated data. This allows us to use `union all` instead of `union`
   which will result in more efficient query execution in Trino.
2. We can make the `MERGE` statement more efficient and read less data
   from the target table by including `incremental_predicates` in our
   model config. A common strategy is to only read data for the last 7
   days from the target table.

For deletes, they can be handled in 2 ways which we will discuss next.

## Soft Deletes

`dbt` recommends following a soft delete approach where no data is
actually deleted from a model. This is implemented by somehow marking the
rows that should be considered deleted.

The CDC data produced by Amazon DMS has an operation type in the `op` field
associated with each change operation. Using this field, we can identify
insert, update, and delete operations.

First lets define the config for our model:

```
-- depends_on: {{ ref('stg_dms__cdc_products') }}

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'product_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns'
    )
}}
```

This is going to be an incremental model and use the `merge` strategy which means
a `MERGE` statement will be generated.

First we create a CTE for each CDC operation:

```
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
```

Now we have all updates, inserts, and deletes that have occurred since the last
incremental run of our model.

To implement the soft delete approach, we add an additional field named
`to_delete`. This is only set to `false` in the CTE for the delete operations.

The final part of our model is to union all the operations:

```
select * from updates union all select * from inserts union all select * from deletes
```

Now we can build another model `stg_products` which references this model that
filters out any data marked as deleted:

```
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
```

Since this model is materialized as a view, there is no additional overhead to
using this approach.

## Hard Deletes

If there is a requirement to actually delete data from a model, we can achieve
this with a `post_hook` in our model. We have a model named `stg_products_hard_delete`
that shows this approach.

With this approach our model is the exact same with the additional `to_delete`
field. We just change our model config:

```
-- depends_on: {{ ref('stg_dms__cdc_products') }}

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'product_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
        post_hook            = [
            "DELETE FROM {{ this }} WHERE to_delete = true"
        ]
    )
}}
```

We now have a `post_hook` that issues a `DELETE` statement to remove any data
that is marked as deleted.

This `DELETE` statement can be made faster by adding an extra predicate like:

```
DELETE FROM {{ this }} WHERE to_delete = true AND last_update_time > date_add('day', -7, current_timestamp)
```

# Trino and Iceberg Considerations

The table properties for an Iceberg table can be specified in the `properties`
map of the `config` macro for a model in `dbt`.

The operations referenced above can be called directly from a `dbt` model in
a `post_hook`. For example, this model calls the `expire_snapshots` procedure
and `analyze` function after the model is built in a `post_hook`:

```
{{
    config(
        materialized         = 'incremental',
        properties           = {
            "partitioning" : "ARRAY['month(last_update_time)']",
            "sorted_by"    : "ARRAY['last_update_time']"
        },
        unique_key           = 'product_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
        post_hook            = [
            "ALTER TABLE {{ this }} EXECUTE expire_snapshots(retention_threshold => '7d')",
            "ANALYZE {{ this }}"
        ]
    )
}}
```