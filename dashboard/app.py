import sqlite3
import pandas as pd
import plotly.express as px
import streamlit as st

# ── Config ───────────────────────────────────────────────
DB = r"C:\Users\sawan\OneDrive\Documents\GitHub\Lindy-Credit-Intelligence\data\lindy_mock.db"

# ── SQL Queries (inline, no dbt views needed) ────────────
QUERIES = {
    "mart_credit_usage": """
        select
            w.workflow_type,
            count(w.workflow_id)                                           as total_workflows,
            sum(c.credits_used)                                            as total_credits,
            round(avg(c.credits_used), 2)                                  as avg_credits_per_workflow,
            sum(case when w.status='failed' then c.credits_used else 0 end) as total_wasted_credits,
            round(sum(case when w.status='failed' then c.credits_used else 0 end)
                  * 1.0 / nullif(sum(c.credits_used),0) * 100, 2)         as pct_wasted
        from raw_workflow_events w
        left join raw_credit_transactions c on w.workflow_id = c.workflow_id
        group by w.workflow_type
        order by total_credits desc
    """,
    "mart_workflow_reliability": """
        select
            workflow_type,
            failure_reason,
            count(workflow_id)                                                    as total_workflows,
            sum(case when status='success' then 1 else 0 end)                    as successful,
            sum(case when status='failed'  then 1 else 0 end)                    as failed,
            round(sum(case when status='success' then 1 else 0 end)*1.0
                  / nullif(count(workflow_id),0)*100, 2)                          as success_rate,
            round(avg(duration_seconds), 2)                                       as avg_duration_seconds
        from raw_workflow_events
        group by workflow_type, failure_reason
        order by failed desc
    """,
    "mart_churn_risk": """
        select
            c.user_id,
            u.plan_type,
            u.monthly_credits,
            sum(c.credits_used)                                                        as total_credits_used,
            sum(case when c.status='failed' then c.credits_used else 0 end)            as credits_wasted,
            round(sum(c.credits_used)*1.0/u.monthly_credits*100, 2)                    as pct_credits_used,
            round(sum(case when c.status='failed' then c.credits_used else 0 end)*1.0
                  / nullif(sum(c.credits_used),0)*100, 2)                              as pct_credits_wasted,
            count(case when w.status='failed' then 1 end)                              as failed_workflows,
            count(w.workflow_id)                                                       as total_workflows,
            round(count(case when w.status='failed' then 1 end)*1.0
                  / nullif(count(w.workflow_id),0)*100, 2)                             as failure_rate,
            case
                when round(sum(c.credits_used)*1.0/u.monthly_credits*100,2) > 80
                 and round(sum(case when c.status='failed' then c.credits_used else 0 end)*1.0
                     /nullif(sum(c.credits_used),0)*100,2) > 30 then 'high'
                when round(sum(c.credits_used)*1.0/u.monthly_credits*100,2) > 50
                 and round(sum(case when c.status='failed' then c.credits_used else 0 end)*1.0
                     /nullif(sum(c.credits_used),0)*100,2) > 20 then 'medium'
                else 'low'
            end as churn_risk
        from raw_credit_transactions c
        left join raw_users u on c.user_id = u.user_id
        left join raw_workflow_events w on c.user_id = w.user_id
        group by c.user_id, u.plan_type, u.monthly_credits
        order by pct_credits_wasted desc
    """,
    "mart_review_sentiment": """
        select
            platform,
            sentiment,
            complaint_category,
            plan_type,
            count(review_id)      as total_reviews,
            round(avg(rating), 2) as avg_rating
        from raw_reviews
        group by platform, sentiment, complaint_category, plan_type
        order by platform, total_reviews desc
    """
}

st.set_page_config(
    page_title="Lindy Credit Intelligence",
    page_icon="⚡",
    layout="wide"
)

# ── Data loader ──────────────────────────────────────────
@st.cache_data
def load(name):
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query(QUERIES[name], conn)
    conn.close()
    return df

# ── Header ───────────────────────────────────────────────
st.title("⚡ Lindy Credit Intelligence")
st.caption("A mock analytics dashboard built to surface credit consumption patterns, workflow reliability, and churn risk signals.")
st.divider()

# ── Tabs ─────────────────────────────────────────────────
t1, t2, t3, t4 = st.tabs([
    "💳 Credit Usage",
    "🔧 Workflow Reliability",
    "🚨 Churn Risk",
    "⭐ Review Sentiment"
])

# ────────────────────────────────────────────────────────
# TAB 1 — Credit Usage
# ────────────────────────────────────────────────────────
with t1:
    st.subheader("Credit Usage by Workflow Type")
    df = load("mart_credit_usage")

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Credits Consumed",  f"{df['total_credits'].sum():,.0f}")
    c2.metric("Total Credits Wasted",    f"{df['total_wasted_credits'].sum():,.0f}")
    c3.metric("Overall Waste Rate",
              f"{df['total_wasted_credits'].sum() / df['total_credits'].sum() * 100:.1f}%")

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(df, x="workflow_type", y="total_credits",
                     title="Total Credits by Workflow",
                     color="workflow_type", text_auto=True)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.bar(df, x="workflow_type", y="pct_wasted",
                      title="% Credits Wasted per Workflow Type",
                      color="pct_wasted", color_continuous_scale="Reds",
                      text_auto=True)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("### Detailed Breakdown")
    st.dataframe(df, use_container_width=True)

# ────────────────────────────────────────────────────────
# TAB 2 — Workflow Reliability
# ────────────────────────────────────────────────────────
with t2:
    st.subheader("Workflow Reliability")
    df2 = load("mart_workflow_reliability")

    # summary by workflow type only
    summary = df2.groupby("workflow_type").agg(
        total_workflows=("total_workflows", "sum"),
        successful=("successful", "sum"),
        failed=("failed", "sum"),
        avg_duration=("avg_duration_seconds", "mean")
    ).reset_index()
    summary["success_rate"] = (summary["successful"] / summary["total_workflows"] * 100).round(2)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Workflows",   f"{summary['total_workflows'].sum():,.0f}")
    c2.metric("Total Failed",      f"{summary['failed'].sum():,.0f}")
    c3.metric("Avg Success Rate",  f"{summary['success_rate'].mean():.1f}%")

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        fig3 = px.bar(summary, x="workflow_type", y="success_rate",
                      title="Success Rate by Workflow Type (%)",
                      color="success_rate", color_continuous_scale="Greens",
                      text_auto=True)
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        fail_reasons = df2[df2["failure_reason"].notna()].groupby(
            "failure_reason")["failed"].sum().reset_index()
        fig4 = px.pie(fail_reasons, names="failure_reason", values="failed",
                      title="Failure Breakdown by Reason")
        st.plotly_chart(fig4, use_container_width=True)

    st.markdown("### Full Reliability Table")
    st.dataframe(df2, use_container_width=True)

# ────────────────────────────────────────────────────────
# TAB 3 — Churn Risk
# ────────────────────────────────────────────────────────
with t3:
    st.subheader("Churn Risk Signals")
    df3 = load("mart_churn_risk")

    risk_counts = df3["churn_risk"].value_counts().reset_index()
    risk_counts.columns = ["churn_risk", "count"]

    c1, c2, c3 = st.columns(3)
    c1.metric("🔴 High Risk Users",
              int(df3[df3["churn_risk"] == "high"].shape[0]))
    c2.metric("🟡 Medium Risk Users",
              int(df3[df3["churn_risk"] == "medium"].shape[0]))
    c3.metric("🟢 Low Risk Users",
              int(df3[df3["churn_risk"] == "low"].shape[0]))

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        fig5 = px.pie(risk_counts, names="churn_risk", values="count",
                      title="Churn Risk Distribution",
                      color="churn_risk",
                      color_discrete_map={"high":"red","medium":"orange","low":"green"})
        st.plotly_chart(fig5, use_container_width=True)

    with col2:
        fig6 = px.scatter(df3, x="pct_credits_used", y="pct_credits_wasted",
                          color="churn_risk", hover_data=["user_id","plan_type"],
                          title="Credit Usage vs Waste Rate by User",
                          color_discrete_map={"high":"red","medium":"orange","low":"green"})
        st.plotly_chart(fig6, use_container_width=True)

    st.markdown("### High Risk Users")
    high = df3[df3["churn_risk"] == "high"][[
        "user_id","plan_type","pct_credits_used",
        "pct_credits_wasted","failure_rate","churn_risk"
    ]]
    st.dataframe(high, use_container_width=True)

# ────────────────────────────────────────────────────────
# TAB 4 — Review Sentiment
# ────────────────────────────────────────────────────────
with t4:
    st.subheader("Review Sentiment Analysis")
    df4 = load("mart_review_sentiment")

    avg_rating = df4.groupby("platform")["avg_rating"].mean().reset_index()

    c1, c2 = st.columns(2)
    with c1:
        fig7 = px.bar(avg_rating, x="platform", y="avg_rating",
                      title="Average Rating by Platform",
                      color="platform", text_auto=True)
        fig7.update_layout(showlegend=False)
        st.plotly_chart(fig7, use_container_width=True)

    with c2:
        sentiment_counts = df4.groupby(
            ["platform","sentiment"])["total_reviews"].sum().reset_index()
        fig8 = px.bar(sentiment_counts, x="platform", y="total_reviews",
                      color="sentiment", title="Sentiment by Platform",
                      barmode="group",
                      color_discrete_map={"positive":"green",
                                          "negative":"red","neutral":"gray"})
        st.plotly_chart(fig8, use_container_width=True)

    st.markdown("### Top Complaint Categories")
    complaints = df4.groupby("complaint_category")["total_reviews"].sum()\
                    .reset_index().sort_values("total_reviews", ascending=False)
    fig9 = px.bar(complaints, x="complaint_category", y="total_reviews",
                  title="Reviews by Complaint Category",
                  color="complaint_category", text_auto=True)
    fig9.update_layout(showlegend=False)
    st.plotly_chart(fig9, use_container_width=True)