# ⚡ Lindy Credit Intelligence

> A mock analytics pipeline and dashboard built to surface Lindy's biggest retention and revenue risks — credit waste, workflow failures, churn signals, and revenue at risk.
>
> **Built to demonstrate what a data engineer would ship in week one at Lindy.**

🔗 **[Live Dashboard](https://lindy-credit-intelligence-59ygkdcvfhjwwg7rc5fy2m.streamlit.app)**

---

## The Problem

Lindy users churn because credit consumption is opaque. Users don't know where their credits go, which workflows burn the most, and why failures happen. They just see their balance drop — and cancel.

This is the **#1 complaint** across Trustpilot, G2, Capterra, and Reddit reviews.

Three questions nobody at Lindy can currently answer with data:

1. **Which workflows are burning the most credits?** Without event-level tracking, users and the product team are flying blind.
2. **Which workflows fail most — and what does that cost?** Failed workflows still burn credits. Nobody is measuring how much is wasted.
3. **Which users are about to cancel?** High credit burn + high failure rate = churn signal. Without a pipeline tracking this, there's no early warning system.

This project builds the data infrastructure to answer all three.

---

## What I Built

A full end-to-end data pipeline — from raw event simulation to a live Streamlit dashboard — running on Snowflake and dbt.

### Architecture

```
generate_mock_data.py
        ↓
Snowflake (RAW schema)        ← 4 raw tables simulating Lindy's internal telemetry
        ↓
dbt Staging Models            ← clean and standardize raw data
        ↓
dbt Intermediate Models       ← join tables, apply business logic
        ↓
dbt Mart Models               ← analytics-ready views
        ↓
Streamlit Dashboard           ← live at streamlit.app
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Warehouse | Snowflake |
| Data Transformation | dbt |
| Orchestration | Python |
| Dashboard | Streamlit |
| Visualization | Plotly |
| Version Control | GitHub |
| Deployment | Streamlit Cloud |

---

## Project Structure

```
lindy-credit-intelligence/
│
├── generate_mock_data.py          # Generates mock data → loads to Snowflake
│
├── dbt_lindy/                     # Full dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/               # Clean raw tables
│       │   ├── stg_users.sql
│       │   ├── stg_workflow_events.sql
│       │   ├── stg_credit_transactions.sql
│       │   └── stg_reviews.sql
│       ├── intermediate/          # Business logic layer
│       │   ├── int_workflow_performance.sql
│       │   ├── int_user_credit_health.sql
│       │   └── int_churn_signals.sql
│       └── marts/                 # Analytics-ready views
│           ├── mart_credit_usage.sql
│           ├── mart_workflow_reliability.sql
│           ├── mart_churn_risk.sql
│           └── mart_review_sentiment.sql
│
├── dashboard/
│   └── app.py                     # Streamlit dashboard
│
├── requirements.txt
└── README.md
```

---

## Dashboard Tabs

### 💳 Credit Consumption
- Total credits consumed by workflow type
- % of credits wasted on failed workflows
- Daily credit consumption vs waste trend
- **Insight:** Nearly 1 in 5 credits is burned on failed workflows

### 🔧 Workflow Reliability
- Success rate by workflow type
- Failure reason breakdown (pie chart)
- Daily failure rate trend with average baseline
- **Insight:** Identifies which workflow type has the lowest success rate and highest credit waste

### 🚨 Cancellation Risk
- Users segmented by High / Medium / Low cancellation risk
- Credit usage vs waste rate scatter plot (every dot = a user)
- Daily credit waste rate trend
- High-risk user table with progress bar columns
- **Insight:** Identifies users 2-3 weeks before they cancel — creating an intervention window

### ⭐ User Sentiment
- Average star rating by platform (G2 vs Trustpilot vs Capterra)
- Positive vs negative review breakdown
- Monthly sentiment trend
- Top complaint categories
- **Insight:** G2 is 4.9★, Trustpilot is 2.4★ — the gap signals which user segments are struggling

### 💰 Revenue Signals
- Estimated MRR by plan type
- Revenue at risk from high-churn users
- Free trial conversion funnel
- Monthly revenue at risk trend
- **Insight:** Free users hitting 80%+ credit usage are the highest-probability upgrade candidates

---

## Key Metrics

| Metric | Definition |
|---|---|
| Credit Waste Rate | % of total credits burned on failed workflows |
| Workflow Success Rate | % of workflows completed without failure |
| Cancellation Risk Score | High/Medium/Low based on credit burn + failure rate |
| Revenue at Risk | MRR attributable to high-risk users |
| Upgrade Candidates | Free users at 80%+ of monthly credit limit |

---

## Data Model

### Raw Tables (Snowflake — RAW schema)
- `RAW_USERS` — 200 users across free/pro/business plans
- `RAW_WORKFLOW_EVENTS` — ~8,000 workflow executions with success/failure outcomes
- `RAW_CREDIT_TRANSACTIONS` — every credit debit, including credits burned on failed workflows
- `RAW_REVIEWS` — 150 reviews across Trustpilot, G2, Capterra with realistic sentiment distribution

### dbt Layers
**Staging** — standardizes column names, selects needed fields from raw tables

**Intermediate** — joins events with credit transactions, calculates per-user credit health, builds churn signal logic

**Marts** — final analytics-ready views that power each dashboard tab

---

## Running Locally

### Prerequisites
- Python 3.12
- Snowflake account
- dbt-snowflake

### Setup

```bash
# Clone the repo
git clone https://github.com/Chinmay1220/lindy-credit-intelligence.git
cd lindy-credit-intelligence

# Install dependencies
pip install -r requirements.txt

# Create secrets file
mkdir -p dashboard/.streamlit
cat > dashboard/.streamlit/secrets.toml << EOF
[snowflake]
account = "YOUR_ACCOUNT"
user = "YOUR_USER"
password = "YOUR_PASSWORD"
role = "ACCOUNTADMIN"
warehouse = "COMPUTE_WH"
database = "LINDY_CREDIT_INTELLIGENCE"
schema = "RAW"
EOF

# Load mock data to Snowflake
python generate_mock_data.py

# Run dbt models
cd dbt_lindy
py -3.12 -c "from dbt.cli.main import cli; cli()" run

# Run dashboard
cd ..
py -3.12 -m streamlit run dashboard/app.py
```

---

## Why This Matters

This dashboard was built on **publicly available review data** — Trustpilot, G2, Capterra, Reddit — to identify real pain points Lindy users are experiencing today.

With access to Lindy's actual internal event logs and credit transaction tables, this pipeline goes live in week one — no architecture changes needed. The data source changes. The pipeline doesn't.

---

## About

Built by **Chinmay Sawant**  
Data Engineer · Boston, MA  
[LinkedIn](https://www.linkedin.com/in/YOUR_LINKEDIN) · [GitHub](https://github.com/Chinmay1220) · [Portfolio](#)

*Built as a demonstration of what a data engineer would ship in week one at Lindy.*
