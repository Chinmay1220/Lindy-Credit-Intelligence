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
from raw_workflow_events