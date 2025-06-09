with
    source as (

        -- Select data from the raw source for NJ_003
        select * from {{ source("silver_screen_raw", "NJ_003") }}

    ),

    renamed_and_casted003 as (

        select
            -- Keep transaction_id for debugging and future reusability
            transaction_id,

            details as movie_id,

            -- Create the 'month' column as requested, truncating the timestamp
            date_trunc('month', timestamp) as month,

            -- Add a literal location_id for this source
            'NJ_003' as location_id,

            -- Rename columns to our standard naming convention
            amount as tickets_sold,
            total_value as revenue

        from source
        where product_type = 'ticket' and details is not null  -- This ensures we only process tickets with a valid movie_id.

    )

-- Final selection from the transformed data
select *
from renamed_and_casted003
