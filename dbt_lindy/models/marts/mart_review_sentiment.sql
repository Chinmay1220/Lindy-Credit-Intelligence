select
    platform,
    sentiment,
    complaint_category,
    plan_type,
    count(review_id)        as total_reviews,
    round(avg(rating), 2)   as avg_rating
from {{ ref('stg_reviews') }}
group by platform, sentiment, complaint_category, plan_type
order by platform, total_reviews desc