with
    source as (

        -- Select data from the raw source for NJ_002
        select * from {{ source("silver_screen_raw", "NJ_002") }}

    ),

    aggregated_monthly as (

        select
            movie_id,

            -- Create the 'month' column by truncating the daily date
            date_trunc('month', date) as month,

            -- Add a literal location_id for this source
            'NJ_002' as location_id,

            -- Sum the daily amounts to get monthly totals
            sum(ticket_amount) as tickets_sold,
            sum(total_earned) as revenue

        from source

        -- Group by all non-aggregated columns to make the SUM work correctly
        group by 1, 2, 3  -- This groups by movie_id, month, and location_id

    )

-- Final selection from the aggregated data
select *
from aggregated_monthly
