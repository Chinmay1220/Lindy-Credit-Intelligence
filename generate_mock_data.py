import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import random
from datetime import datetime, timedelta

# ── Snowflake Config ─────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account":   "ZVIAMOM-IN09435",
    "user":      "CSAW",
    "password":  "Qwertasdfg@1469",
    "role":      "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database":  "LINDY_CREDIT_INTELLIGENCE",
    "schema":    "RAW",
}

# ── Mock Data Config ─────────────────────────────────────
NUM_USERS = 200
random.seed(42)

PLAN_TYPES     = ["free", "pro", "business"]
WORKFLOW_TYPES = ["email_followup", "meeting_summary", "crm_update",
                  "lead_research", "calendar_scheduling", "document_processing"]
FAILURE_REASONS= ["timeout", "api_error", "credit_exhausted",
                  "invalid_input", "integration_failure"]
CREDIT_COSTS   = {"email_followup": 10, "meeting_summary": 25,
                  "crm_update": 15, "lead_research": 40,
                  "calendar_scheduling": 8, "document_processing": 30}
PLAN_CREDITS   = {"free": 400, "pro": 5000, "business": 15000}

def rand_date(start=90, end=0):
    return datetime.now() - timedelta(days=random.randint(end, start))

def gen_users():
    rows = []
    for i in range(1, NUM_USERS + 1):
        plan = random.choices(PLAN_TYPES, weights=[0.3, 0.5, 0.2])[0]
        rows.append({
            "USER_ID":         f"u_{i:04d}",
            "PLAN_TYPE":       plan,
            "MONTHLY_CREDITS": PLAN_CREDITS[plan],
            "SIGNUP_DATE":     rand_date(90, 30).strftime("%Y-%m-%d"),
            "COUNTRY":         random.choice(["US","UK","IN","CA","AU","DE"]),
        })
    return pd.DataFrame(rows)

def gen_workflow_events(users):
    rows, wf_id = [], 1
    for _, u in users.iterrows():
        for _ in range(random.randint(5, 80)):
            wf_type = random.choice(WORKFLOW_TYPES)
            success = random.random() > 0.25
            steps   = random.randint(1, 6)
            rows.append({
                "WORKFLOW_ID":     f"wf_{wf_id:06d}",
                "USER_ID":         u["USER_ID"],
                "WORKFLOW_TYPE":   wf_type,
                "STATUS":          "success" if success else "failed",
                "STEPS_TOTAL":     steps,
                "STEPS_COMPLETED": steps if success else random.randint(1, steps),
                "FAILURE_REASON":  None if success else random.choice(FAILURE_REASONS),
                "CREATED_AT":      rand_date(90, 0).strftime("%Y-%m-%d %H:%M:%S"),
                "DURATION_SECONDS":random.randint(2, 120),
            })
            wf_id += 1
    return pd.DataFrame(rows)

def gen_credit_transactions(wf):
    rows = []
    for i, row in enumerate(wf.itertuples(), 1):
        base = CREDIT_COSTS[row.WORKFLOW_TYPE]
        rows.append({
            "TRANSACTION_ID": f"tx_{i:06d}",
            "USER_ID":        row.USER_ID,
            "WORKFLOW_ID":    row.WORKFLOW_ID,
            "CREDITS_USED":   base if row.STATUS == "success" else int(base * random.uniform(0.4, 0.9)),
            "WORKFLOW_TYPE":  row.WORKFLOW_TYPE,
            "STATUS":         row.STATUS,
            "CREATED_AT":     row.CREATED_AT,
        })
    return pd.DataFrame(rows)

def gen_reviews():
    rows = []
    platforms  = ["trustpilot", "g2", "capterra", "app_store"]
    categories = ["billing", "reliability", "performance",
                  "ease_of_use", "customer_support", "pricing"]
    for i in range(1, 151):
        platform  = random.choice(platforms)
        sentiment = random.choices(
            ["positive","negative","neutral"],
            weights=[0.2,0.6,0.2] if platform=="trustpilot" else [0.7,0.1,0.2])[0]
        rows.append({
            "REVIEW_ID":          f"rv_{i:04d}",
            "PLATFORM":           platform,
            "RATING":             random.randint(1,3) if sentiment=="negative" else random.randint(4,5),
            "SENTIMENT":          sentiment,
            "COMPLAINT_CATEGORY": random.choice(categories),
            "PLAN_TYPE":          random.choice(PLAN_TYPES),
            "REVIEW_DATE":        rand_date(180, 0).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)

def main():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    users = gen_users()
    wf    = gen_workflow_events(users)
    cr    = gen_credit_transactions(wf)
    rv    = gen_reviews()

    tables = [
        ("RAW_USERS",                users),
        ("RAW_WORKFLOW_EVENTS",      wf),
        ("RAW_CREDIT_TRANSACTIONS",  cr),
        ("RAW_REVIEWS",              rv),
    ]

    for name, df in tables:
        print(f"Loading {name} ({len(df):,} rows)...")
        conn.cursor().execute(f"DROP TABLE IF EXISTS LINDY_CREDIT_INTELLIGENCE.RAW.{name}")
        success, chunks, rows, _ = write_pandas(
            conn, df, name,
            database="LINDY_CREDIT_INTELLIGENCE",
            schema="RAW",
            auto_create_table=True,
            overwrite=True
        )
        print(f"  ✅ {name} — {rows:,} rows loaded")

    conn.close()
    print("\n✅ All tables loaded into Snowflake successfully!")

if __name__ == "__main__":
    main()