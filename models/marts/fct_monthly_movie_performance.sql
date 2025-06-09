{{ config(materialized="table") }}

with

    monthly_sales as (select * from {{ ref("int_monthly_sales") }}),

    monthly_costs as (select * from {{ ref("stg_invoices") }}),

    movie_catalogue as (select * from {{ ref("stg_movie_catalogue") }})

select
    -- Key columns
    sales.month,
    sales.location_id,
    sales.movie_id,

    -- Movie details from the catalogue
    catalogue.movie_title,
    catalogue.genre,
    catalogue.studio,

    -- Metrics
    sales.tickets_sold,
    sales.revenue,
    coalesce(costs.rental_cost, 0) as rental_cost,
    (sales.revenue - coalesce(costs.rental_cost, 0)) as profit

from monthly_sales as sales
left join
    -- This join needs all 3 keys to get the cost for a specific movie in a specific
    -- location for a specific month
    monthly_costs as costs
    on sales.month = costs.month
    and sales.location_id = costs.location_id
    and sales.movie_id = costs.movie_id
left join movie_catalogue as catalogue on sales.movie_id = catalogue.movie_id
