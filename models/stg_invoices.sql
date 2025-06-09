WITH source AS (
    SELECT * FROM {{ source('silver_screen_raw', 'INVOICES') }} 
)

SELECT
    -- Replaced dbt_utils.generate_surrogate_key with a standard MD5 hash.
    -- We concatenate the key columns with a separator to create a unique string.
    MD5(CONCAT_WS('-', MONTH, LOCATION_ID, MOVIE_ID)) as invoice_surrogate_key,
    
    MONTH,
    LOCATION_ID,
    MOVIE_ID,
    
    -- Since studio and other attributes should be the same within the group, we use MIN/MAX
    MAX(studio) as studio,
    MAX(release_date) as release_date,
    
    -- Sum the costs to get the correct monthly total
    SUM(total_invoice_sum) as rental_cost

FROM source
GROUP BY
    MONTH,
    LOCATION_ID,
    MOVIE_ID