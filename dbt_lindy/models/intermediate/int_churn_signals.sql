select
    uch.user_id,
    uch.plan_type,
    uch.monthly_credits,
    uch.total_credits_used,
    uch.credits_wasted,
    uch.pct_credits_used,
    uch.pct_credits_wasted,
    count(case when w.status = 'failed' then 1 end) as failed_workflows,
    count(w.workflow_id)                             as total_workflows,
    round(count(case when w.status = 'failed' then 1 end) * 1.0
          / nullif(count(w.workflow_id), 0) * 100, 2) as failure_rate,
    case
        when uch.pct_credits_used > 80
         and uch.pct_credits_wasted > 30  then 'high'
        when uch.pct_credits_used > 50
         and uch.pct_credits_wasted > 20  then 'medium'
        else 'low'
    end as churn_risk
from {{ ref('int_user_credit_health') }} uch
left join {{ ref('stg_workflow_events') }} w on uch.user_id = w.user_id
group by
    uch.user_id, uch.plan_type, uch.monthly_credits,
    uch.total_credits_used, uch.credits_wasted,
    uch.pct_credits_used, uch.pct_credits_wasted