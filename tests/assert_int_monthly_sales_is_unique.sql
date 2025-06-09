-- This test checks if there are any duplicate rows based on the combination of
-- month, location_id, and movie_id in the int_monthly_sales model.
-- If this query returns any rows, the test will fail.

select month, location_id, movie_id, count(*) as duplicate_count
from {{ ref("int_monthly_sales") }}
group by 1, 2, 3
having count(*) > 1
