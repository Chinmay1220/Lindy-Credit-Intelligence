select
    user_id,
    plan_type,
    monthly_credits,
    total_credits_used,
    credits_wasted,
    pct_credits_used,
    pct_credits_wasted,
    failed_workflows,
    total_workflows,
    failure_rate,
    churn_risk
from {{ ref('int_churn_signals') }}
order by
    case churn_risk when 'high' then 1 when 'medium' then 2 else 3 end,
    pct_credits_wasted desc