select
    c.user_id,
    u.plan_type,
    u.monthly_credits,
    sum(c.credits_used)                                          as total_credits_used,
    sum(case when c.status = 'failed' then c.credits_used else 0 end) as credits_wasted,
    count(c.transaction_id)                                      as total_transactions,
    round(sum(c.credits_used) * 1.0 / u.monthly_credits * 100, 2) as pct_credits_used,
    round(sum(case when c.status = 'failed' then c.credits_used else 0 end) * 1.0
          / nullif(sum(c.credits_used), 0) * 100, 2)            as pct_credits_wasted
from {{ ref('stg_credit_transactions') }} c
left join {{ ref('stg_users') }} u on c.user_id = u.user_id
group by c.user_id, u.plan_type, u.monthly_credits