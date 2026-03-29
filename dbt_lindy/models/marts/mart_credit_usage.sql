select
    workflow_type,
    count(workflow_id)                                        as total_workflows,
    sum(credits_used)                                         as total_credits,
    round(avg(credits_used), 2)                               as avg_credits_per_workflow,
    sum(wasted_credits)                                       as total_wasted_credits,
    round(sum(wasted_credits) * 1.0 / nullif(sum(credits_used), 0) * 100, 2) as pct_wasted
from {{ ref('int_workflow_performance') }}
group by workflow_type
order by total_credits desc