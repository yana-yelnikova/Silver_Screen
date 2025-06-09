with source as (select * from {{ source("silver_screen_raw", "INVOICES") }})

select
    -- Replaced dbt_utils.generate_surrogate_key with a standard MD5 hash.
    -- We concatenate the key columns with a separator to create a unique string.
    md5(concat_ws('-', month, location_id, movie_id)) as invoice_surrogate_key,

    month,
    location_id,
    movie_id,

    -- Since studio and other attributes should be the same within the group, we use
    -- MIN/MAX
    max(studio) as studio,
    max(release_date) as release_date,

    -- Sum the costs to get the correct monthly total
    sum(total_invoice_sum) as rental_cost

from source
group by month, location_id, movie_id
