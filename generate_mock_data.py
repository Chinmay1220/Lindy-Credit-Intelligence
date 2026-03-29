import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────
DB = "data/lindy_mock.db"
NUM_USERS = 200
NUM_DAYS  = 90
random.seed(42)

PLAN_TYPES       = ["free", "pro", "business"]
WORKFLOW_TYPES   = ["email_followup", "meeting_summary", "crm_update",
                    "lead_research", "calendar_scheduling", "document_processing"]
FAILURE_REASONS  = ["timeout", "api_error", "credit_exhausted",
                    "invalid_input", "integration_failure"]
CREDIT_COSTS     = {"email_followup": 10, "meeting_summary": 25,
                    "crm_update": 15, "lead_research": 40,
                    "calendar_scheduling": 8, "document_processing": 30}
PLAN_CREDITS     = {"free": 400, "pro": 5000, "business": 15000}

# ── Helpers ─────────────────────────────────────────────
def rand_date(start_days_ago=90, end_days_ago=0):
    delta = random.randint(end_days_ago, start_days_ago)
    return datetime.now() - timedelta(days=delta)

# ── 1. Users ─────────────────────────────────────────────
def gen_users():
    rows = []
    for i in range(1, NUM_USERS + 1):
        plan   = random.choices(PLAN_TYPES, weights=[0.3, 0.5, 0.2])[0]
        signup = rand_date(90, 30)
        rows.append({
            "user_id":        f"u_{i:04d}",
            "plan_type":      plan,
            "monthly_credits": PLAN_CREDITS[plan],
            "signup_date":    signup.strftime("%Y-%m-%d"),
            "country":        random.choice(["US","UK","IN","CA","AU","DE"]),
        })
    return pd.DataFrame(rows)

# ── 2. Workflow events ────────────────────────────────────
def gen_workflow_events(users):
    rows = []
    wf_id = 1
    for _, u in users.iterrows():
        n_workflows = random.randint(5, 80)
        for _ in range(n_workflows):
            wf_type   = random.choice(WORKFLOW_TYPES)
            # pro/business users run more complex workflows → higher failure rate
            fail_prob = 0.35 if u["plan_type"] == "free" else 0.25
            success   = random.random() > fail_prob
            steps     = random.randint(1, 6)
            completed = steps if success else random.randint(1, steps)
            rows.append({
                "workflow_id":       f"wf_{wf_id:06d}",
                "user_id":           u["user_id"],
                "workflow_type":     wf_type,
                "status":            "success" if success else "failed",
                "steps_total":       steps,
                "steps_completed":   completed,
                "failure_reason":    None if success else random.choice(FAILURE_REASONS),
                "created_at":        rand_date(90, 0).strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds":  random.randint(2, 120),
            })
            wf_id += 1
    return pd.DataFrame(rows)

# ── 3. Credit transactions ────────────────────────────────
def gen_credit_transactions(workflow_events):
    rows = []
    tx_id = 1
    for _, wf in workflow_events.iterrows():
        base_cost    = CREDIT_COSTS[wf["workflow_type"]]
        # failed workflows still burn credits (the core user complaint)
        credits_used = base_cost if wf["status"] == "success" \
                       else int(base_cost * random.uniform(0.4, 0.9))
        rows.append({
            "transaction_id": f"tx_{tx_id:06d}",
            "user_id":        wf["user_id"],
            "workflow_id":    wf["workflow_id"],
            "credits_used":   credits_used,
            "workflow_type":  wf["workflow_type"],
            "status":         wf["status"],
            "created_at":     wf["created_at"],
        })
        tx_id += 1
    return pd.DataFrame(rows)

# ── 4. Reviews ────────────────────────────────────────────
def gen_reviews():
    platforms  = ["trustpilot", "g2", "capterra", "app_store"]
    categories = ["billing", "reliability", "performance",
                  "ease_of_use", "customer_support", "pricing"]
    sentiments = ["positive", "negative", "neutral"]
    rows = []
    for i in range(1, 151):
        platform   = random.choice(platforms)
        # trustpilot skews negative, g2 skews positive (matches real data)
        sentiment  = random.choices(
            sentiments,
            weights=[0.2, 0.6, 0.2] if platform == "trustpilot" else [0.7, 0.1, 0.2]
        )[0]
        rows.append({
            "review_id":       f"rv_{i:04d}",
            "platform":        platform,
            "rating":          random.randint(1, 3) if sentiment == "negative"
                               else random.randint(4, 5),
            "sentiment":       sentiment,
            "complaint_category": random.choice(categories),
            "plan_type":       random.choice(PLAN_TYPES),
            "review_date":     rand_date(180, 0).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)

# ── Write to SQLite ───────────────────────────────────────
def main():
    conn = sqlite3.connect(DB)

    users    = gen_users()
    wf       = gen_workflow_events(users)
    credits  = gen_credit_transactions(wf)
    reviews  = gen_reviews()

    users.to_sql("raw_users",                con=conn, if_exists="replace", index=False)
    wf.to_sql("raw_workflow_events",         con=conn, if_exists="replace", index=False)
    credits.to_sql("raw_credit_transactions",con=conn, if_exists="replace", index=False)
    reviews.to_sql("raw_reviews",            con=conn, if_exists="replace", index=False)

    conn.close()

    print(f"✅ Database created: {DB}")
    print(f"   Users:               {len(users)}")
    print(f"   Workflow events:     {len(wf)}")
    print(f"   Credit transactions: {len(credits)}")
    print(f"   Reviews:             {len(reviews)}")

if __name__ == "__main__":
    main()