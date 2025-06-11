{{ config(materialized="table") }}

with

    sales_nj001 as (
        -- 1. Select from the stg_nj_001 model
        select
            month,
            location_id,
            movie_id,
            sum(tickets_sold) as tickets_sold,
            sum(revenue) as revenue
        from {{ ref("stg_nj_001") }}
        group by 1, 2, 3
    ),

    sales_nj002 as (
        -- 2. Select from the stg_nj_002 model
        -- No aggregation needed here as it's already monthly
        select month, location_id, movie_id, tickets_sold, revenue
        from {{ ref("stg_nj_002") }}
    ),

    sales_nj003 as (
        -- 3. Select from the stg_nj_003 model
        select
            month,
            location_id,
            movie_id,
            sum(tickets_sold) as tickets_sold,
            sum(revenue) as revenue
        from {{ ref("stg_nj_003") }}
        group by 1, 2, 3
    ),

    unioned_sales as (
        -- 4. Union all three sources together
        select *
        from sales_nj001
        union all
        select *
        from sales_nj002
        union all
        select *
        from sales_nj003
    )

-- Final selection from the unioned data
select *
from unioned_sales
