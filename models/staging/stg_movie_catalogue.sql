with source as (select * from {{ source("silver_screen_raw", "MOVIE_CATALOGUE") }})

select
    movie_id,
    movie_title,
    release_date,
    -- Replace missing genre values with 'Unknown' as planned
    coalesce(genre, 'Unknown') as genre,
    studio
-- We select only the columns needed for the final report
from source
