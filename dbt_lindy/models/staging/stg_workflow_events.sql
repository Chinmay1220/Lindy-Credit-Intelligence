select
    workflow_id,
    user_id,
    workflow_type,
    status,
    steps_total,
    steps_completed,
    failure_reason,
    created_at,
    duration_seconds
from LINDY_CREDIT_INTELLIGENCE.RAW.RAW_WORKFLOW_EVENTS