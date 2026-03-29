select
    workflow_type,
    failure_reason,
    count(workflow_id)                                               as total_workflows,
    sum(case when status = 'success' then 1 else 0 end)              as successful,
    sum(case when status = 'failed'  then 1 else 0 end)              as failed,
    round(sum(case when status = 'success' then 1 else 0 end) * 1.0
          / nullif(count(workflow_id), 0) * 100, 2)                  as success_rate,
    round(avg(steps_completed) * 1.0 / nullif(avg(steps_total), 0) * 100, 2) as avg_step_completion_rate,
    round(avg(duration_seconds), 2)                                  as avg_duration_seconds
from {{ ref('int_workflow_performance') }}
group by workflow_type, failure_reason
order by failed desc