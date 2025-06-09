with
    source as (

        -- Select data from the raw source for NJ_001
        select * from {{ source("silver_screen_raw", "NJ_001") }}

    ),

    renamed_and_casted as (

        select
            -- Keep transaction_id for debugging and future reusability
            transaction_id,

            movie_id,

            -- Create the 'month' column as requested, truncating the timestamp
            date_trunc('month', timestamp) as month,

            -- Add a literal location_id for this source
            'NJ_001' as location_id,

            -- Rename columns to our standard naming convention
            ticket_amount as tickets_sold,
            transaction_total as revenue

        from source

    )

-- Final selection from the transformed data
select *
from renamed_and_casted
