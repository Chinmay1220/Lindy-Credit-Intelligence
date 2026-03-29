select
    w.workflow_id,
    w.user_id,
    w.workflow_type,
    w.status,
    w.steps_total,
    w.steps_completed,
    w.failure_reason,
    w.duration_seconds,
    w.created_at,
    c.credits_used,
    case when w.status = 'failed' then c.credits_used else 0 end as wasted_credits
from {{ ref('stg_workflow_events') }} w
left join {{ ref('stg_credit_transactions') }} c
    on w.workflow_id = c.workflow_id