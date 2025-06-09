WITH source AS (

    -- Select data from the raw source for invoices
    SELECT * FROM {{ source('silver_screen_raw', 'INVOICES') }}

)

SELECT
    CAST(month AS DATE) as month,
    location_id,
    movie_id,
    
    -- Sum the invoice amounts to get the correct monthly rental cost
    SUM(total_invoice_sum) as rental_cost

FROM source
GROUP BY
    month,
    location_id,
    movie_id