import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import snowflake.connector
from datetime import datetime, timedelta

# ── Snowflake Config ─────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account":   st.secrets["snowflake"]["account"],
    "user":      st.secrets["snowflake"]["user"],
    "password":  st.secrets["snowflake"]["password"],
    "role":      st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database":  st.secrets["snowflake"]["database"],
    "schema":    st.secrets["snowflake"]["schema"],
}

PLAN_PRICE = {"free": 0, "pro": 49.99, "business": 149.99}
WORKFLOW_LABELS = {
    "email_followup":      "Email Follow-Up",
    "meeting_summary":     "Meeting Summary",
    "crm_update":          "CRM Update",
    "lead_research":       "Lead Research",
    "calendar_scheduling": "Calendar Scheduling",
    "document_processing": "Document Processing",
}
T = "plotly_dark"

st.set_page_config(page_title="Lindy Credit Intelligence", page_icon="⚡", layout="wide")

# ── CSS ──────────────────────────────────────────────────
st.markdown("""
<style>
  .main { background:#0e1117; }
  .block-container { padding-top:1.5rem; }
  .kpi-card {
      background:#1c1f26; border-radius:12px; padding:1.2rem 1.5rem;
      border-left:4px solid #6366f1; margin-bottom:0.5rem;
  }
  .kpi-card.red   { border-left-color:#ef4444; }
  .kpi-card.green { border-left-color:#22c55e; }
  .kpi-card.amber { border-left-color:#f97316; }
  .kpi-card.indigo{ border-left-color:#6366f1; }
  .kpi-label { font-size:0.78rem; color:#9ca3af; margin-bottom:0.2rem; }
  .kpi-value { font-size:1.8rem; font-weight:700; color:#f9fafb; }
  .kpi-delta { font-size:0.78rem; margin-top:0.2rem; }
  .insight-box {
      background:#1e293b; border-left:4px solid #6366f1;
      border-radius:8px; padding:0.9rem 1.2rem; margin:0.8rem 0;
      color:#cbd5e1; font-size:0.9rem;
  }
  .takeaway-box {
      background:#172033; border:1px solid #334155;
      border-radius:10px; padding:1rem 1.4rem; margin-top:1rem;
      color:#94a3b8; font-size:0.88rem;
  }
  .takeaway-box h4 { color:#e2e8f0; margin-bottom:0.4rem; }
  [data-testid="stMetricValue"] { font-size:1.8rem; font-weight:700; }
  .stTabs [data-baseweb="tab"] { font-size:0.92rem; font-weight:600; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ──────────────────────────────────────────────
def kpi(label, value, color="indigo", delta=None):
    delta_html = f'<div class="kpi-delta" style="color:{"#22c55e" if "▲" in str(delta) else "#ef4444"}">{delta}</div>' if delta else ""
    st.markdown(f"""
    <div class="kpi-card {color}">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      {delta_html}
    </div>""", unsafe_allow_html=True)

def insight(text): st.markdown(f'<div class="insight-box">💡 {text}</div>', unsafe_allow_html=True)
def takeaway(title, text): st.markdown(f'<div class="takeaway-box"><h4>📌 {title}</h4>{text}</div>', unsafe_allow_html=True)

# ── Data loader ──────────────────────────────────────────
@st.cache_data
def load_raw():
    conn   = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    users  = pd.read_sql("select * from LINDY_CREDIT_INTELLIGENCE.RAW.RAW_USERS", conn)
    events = pd.read_sql("select * from LINDY_CREDIT_INTELLIGENCE.RAW.RAW_WORKFLOW_EVENTS", conn)
    credits= pd.read_sql("select * from LINDY_CREDIT_INTELLIGENCE.RAW.RAW_CREDIT_TRANSACTIONS", conn)
    reviews= pd.read_sql("select * from LINDY_CREDIT_INTELLIGENCE.RAW.RAW_REVIEWS", conn)
    conn.close()
    for df in [users, events, credits, reviews]:
        df.columns = df.columns.str.lower()
    events["created_at"]   = pd.to_datetime(events["created_at"])
    credits["created_at"]  = pd.to_datetime(credits["created_at"])
    reviews["review_date"] = pd.to_datetime(reviews["review_date"])
    return users, events, credits, reviews

users, events, credits, reviews = load_raw()

# ── Sidebar ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Lindy Credit Intelligence")
    st.markdown("---")
    st.markdown("**Built by**")
    st.markdown("### Chinmay Sawant")
    st.markdown("Data Engineer · Boston, MA")
    st.markdown("[LinkedIn](https://www.linkedin.com/in/chinmay-sawant-9b79a5192) · [GitHub](https://github.com/Chinmay1220) · [Portfolio](https://csprofile.lovable.app)")
    st.markdown("---")
    st.markdown("**About this dashboard**")
    st.markdown("""
    A mock analytics pipeline built to surface Lindy's biggest data blind spots:
    - Credit consumption opacity
    - Workflow reliability gaps
    - Churn risk signals
    - Revenue at risk

    *Built with Python, dbt, Snowflake, and Streamlit.*
    """)
    st.markdown("---")
    st.markdown("**Date Range Filter**")
    max_date = events["created_at"].max().date()
    date_options = {"Last 30 days": 30, "Last 60 days": 60, "Last 90 days": 90, "All time": 999}
    selected_range = st.selectbox("Show data from:", list(date_options.keys()), index=2)
    days = date_options[selected_range]
    cutoff = pd.Timestamp(max_date) - timedelta(days=days)
    st.markdown("---")
    st.caption("This dashboard uses mock data simulating Lindy's internal product telemetry. Built to demonstrate what a data engineer would ship in week one.")

# ── Apply date filter ────────────────────────────────────
ev  = events[events["created_at"]  >= cutoff]
cr  = credits[credits["created_at"] >= cutoff]
rv  = reviews[reviews["review_date"]>= cutoff]

# ── Compute metrics ──────────────────────────────────────
wf_cr = ev.merge(cr, on="workflow_id", how="left", suffixes=("","_c"))
wf_cr["wasted"] = wf_cr.apply(lambda r: r["credits_used"] if r["status"]=="failed" else 0, axis=1)
wf_cr["workflow_type_label"] = wf_cr["workflow_type"].map(WORKFLOW_LABELS).fillna(wf_cr["workflow_type"])

total_credits = wf_cr["credits_used"].sum()
total_wasted  = wf_cr["wasted"].sum()
waste_rate    = total_wasted / total_credits * 100 if total_credits > 0 else 0
total_users   = users["user_id"].nunique()

user_cr = cr.merge(users, on="user_id")
user_cr["wasted"] = user_cr.apply(lambda r: r["credits_used"] if r["status"]=="failed" else 0, axis=1)
user_health = user_cr.groupby(["user_id","plan_type","monthly_credits"]).agg(
    total_used=("credits_used","sum"), total_wasted=("wasted","sum")).reset_index()
user_health["pct_used"]   = (user_health["total_used"]   / user_health["monthly_credits"] * 100).round(2)
user_health["pct_wasted"] = (user_health["total_wasted"] / user_health["total_used"].replace(0,1) * 100).round(2)

wf_failures = ev.groupby("user_id").agg(
    failed_wf=("status", lambda x: (x=="failed").sum()),
    total_wf =("status","count")).reset_index()
user_health = user_health.merge(wf_failures, on="user_id", how="left")
user_health["failure_rate"] = (user_health["failed_wf"] / user_health["total_wf"].replace(0,1) * 100).round(2)
user_health["churn_risk"] = user_health.apply(
    lambda r: "High"   if r["pct_used"] > 80 and r["pct_wasted"] > 30
    else      "Medium" if r["pct_used"] > 50 and r["pct_wasted"] > 20
    else      "Low", axis=1)
user_health["monthly_revenue"] = user_health["plan_type"].map(PLAN_PRICE)
high_risk_users = (user_health["churn_risk"]=="High").sum()

# ── Header ───────────────────────────────────────────────
st.markdown("# ⚡ Lindy Credit Intelligence")
st.markdown("*A mock analytics pipeline surfacing credit consumption patterns, workflow reliability, churn risk, and revenue signals — built to demonstrate what a data engineer would ship in week one at Lindy.*")
st.divider()

# ── Top KPIs ─────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1: kpi("Total Users",                  f"{total_users:,}", "indigo")
with c2: kpi("Total Credits Consumed",       f"{total_credits:,.0f}", "indigo")
with c3: kpi("Overall Credit Waste Rate",    f"{waste_rate:.1f}%", "red", "⚠️ 1 in 5 credits is wasted")
with c4: kpi("High Cancellation Risk Users", f"{high_risk_users:,}", "red", f"▼ {high_risk_users/total_users*100:.1f}% of user base")

st.divider()

# ── Tabs ─────────────────────────────────────────────────
t1, t2, t3, t4, t5 = st.tabs([
    "💳 Credit Consumption",
    "🔧 Workflow Reliability",
    "🚨 Cancellation Risk",
    "⭐ User Sentiment",
    "💰 Revenue Signals"
])

# TAB 1 — Credit Consumption
with t1:
    st.subheader("Where Are Credits Being Spent — and Wasted?")
    st.caption("Credits are Lindy's core monetization unit. Understanding which workflows consume and waste the most is essential for pricing, product, and retention decisions.")
    c1,c2,c3 = st.columns(3)
    with c1: kpi("Total Credits Consumed",          f"{total_credits:,.0f}", "indigo")
    with c2: kpi("Credits Lost to Failures",        f"{total_wasted:,.0f}", "red")
    with c3: kpi("Waste Rate Across All Workflows", f"{waste_rate:.1f}%", "amber")
    st.markdown("---")
    credit_by_wf = wf_cr.groupby("workflow_type_label").agg(
        total_workflows=("workflow_id","count"),
        total_credits=("credits_used","sum"),
        total_wasted=("wasted","sum")).reset_index()
    credit_by_wf["pct_wasted"] = (credit_by_wf["total_wasted"]/credit_by_wf["total_credits"]*100).round(2)
    credit_by_wf = credit_by_wf.sort_values("total_credits", ascending=False)
    top_wf    = credit_by_wf.iloc[0]["workflow_type_label"]
    top_waste = credit_by_wf.sort_values("pct_wasted", ascending=False).iloc[0]["workflow_type_label"]
    insight(f"<b>{top_wf}</b> consumes the most credits overall. <b>{top_waste}</b> has the highest waste rate — every 5th credit spent on it is burned on a failed run.")
    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(credit_by_wf, x="workflow_type_label", y="total_credits",
                     title="Total Credits Consumed by Workflow Type",
                     color="workflow_type_label", text_auto=True, template=T,
                     labels={"workflow_type_label":"Workflow","total_credits":"Credits Consumed"})
        fig.update_layout(showlegend=False, xaxis_tickangle=-20)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig2 = px.bar(credit_by_wf, x="workflow_type_label", y="pct_wasted",
                      title="% of Credits Wasted per Workflow Type",
                      color="pct_wasted", color_continuous_scale="Reds",
                      text_auto=True, template=T,
                      labels={"workflow_type_label":"Workflow","pct_wasted":"% Wasted"})
        fig2.update_layout(xaxis_tickangle=-20)
        st.plotly_chart(fig2, use_container_width=True)
    st.markdown("### Credit Consumption Trend Over Time")
    daily = wf_cr.copy()
    daily["date"] = daily["created_at"].dt.date
    trend = daily.groupby("date").agg(
        credits_consumed=("credits_used","sum"), credits_wasted=("wasted","sum")).reset_index()
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=trend["date"], y=trend["credits_consumed"],
        mode="lines", name="Credits Consumed", line=dict(color="#6366f1", width=2)))
    fig_trend.add_trace(go.Scatter(x=trend["date"], y=trend["credits_wasted"],
        mode="lines", name="Credits Wasted", line=dict(color="#ef4444", width=2, dash="dot")))
    fig_trend.update_layout(template=T, title="Daily Credit Consumption vs Waste",
        xaxis_title="Date", yaxis_title="Credits", legend=dict(orientation="h"))
    st.plotly_chart(fig_trend, use_container_width=True)
    takeaway("What This Means for Lindy",
        "Nearly 1 in 5 credits is being burned on failed workflows. Users on the Pro plan ($49.99/month) don't know this is happening — they just see their balance drop. This is the #1 driver of billing complaints and cancellations. The fix starts with surfacing this data.")
    st.markdown("### Detailed Credit Breakdown by Workflow")
    st.dataframe(credit_by_wf.rename(columns={
        "workflow_type_label":"Workflow Type","total_workflows":"Total Runs",
        "total_credits":"Credits Consumed","total_wasted":"Credits Wasted","pct_wasted":"Waste Rate (%)"}),
        use_container_width=True)

# TAB 2 — Workflow Reliability
with t2:
    st.subheader("Which Workflows Fail Most — and Why?")
    st.caption("Workflow failures burn credits without delivering value. Understanding failure patterns helps engineering prioritize fixes that directly reduce churn.")
    rel = ev.groupby(["workflow_type","failure_reason"]).agg(
        total_workflows=("workflow_id","count"),
        successful=("status", lambda x: (x=="success").sum()),
        failed=("status", lambda x: (x=="failed").sum()),
        avg_duration=("duration_seconds","mean")).reset_index()
    rel["success_rate"] = (rel["successful"]/rel["total_workflows"]*100).round(2)
    rel["workflow_type"] = rel["workflow_type"].map(WORKFLOW_LABELS).fillna(rel["workflow_type"])
    summary = rel.groupby("workflow_type").agg(
        total_workflows=("total_workflows","sum"),
        successful=("successful","sum"),
        failed=("failed","sum"),
        avg_duration=("avg_duration","mean")).reset_index()
    summary["success_rate"] = (summary["successful"]/summary["total_workflows"]*100).round(2)
    worst_wf = summary.sort_values("success_rate").iloc[0]
    c1,c2,c3 = st.columns(3)
    with c1: kpi("Total Workflow Runs",  f"{summary['total_workflows'].sum():,.0f}", "indigo")
    with c2: kpi("Total Failed Runs",    f"{summary['failed'].sum():,.0f}", "red")
    with c3: kpi("Average Success Rate", f"{summary['success_rate'].mean():.1f}%", "amber")
    insight(f"<b>{worst_wf['workflow_type']}</b> has the lowest success rate at <b>{worst_wf['success_rate']}%</b>. Every failed run burns credits with no value delivered to the user.")
    col1, col2 = st.columns(2)
    with col1:
        fig3 = px.bar(summary, x="workflow_type", y="success_rate",
                      title="Success Rate by Workflow Type (%)",
                      color="success_rate", color_continuous_scale="RdYlGn",
                      text_auto=True, template=T,
                      labels={"workflow_type":"Workflow","success_rate":"Success Rate (%)"})
        fig3.update_layout(xaxis_tickangle=-20)
        st.plotly_chart(fig3, use_container_width=True)
    with col2:
        fail_reasons = ev[ev["failure_reason"].notna()].groupby("failure_reason")["workflow_id"]\
            .count().reset_index()
        fail_reasons.columns = ["failure_reason","count"]
        fail_reasons["failure_reason"] = fail_reasons["failure_reason"].str.replace("_"," ").str.title()
        fig4 = px.pie(fail_reasons, names="failure_reason", values="count",
                      title="What Causes Workflow Failures?", template=T)
        fig4.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig4, use_container_width=True)
    st.markdown("### Workflow Failure Rate Trend Over Time")
    ev["date"] = ev["created_at"].dt.date
    daily_rel = ev.groupby("date").agg(
        total=("workflow_id","count"),
        failed=("status", lambda x: (x=="failed").sum())).reset_index()
    daily_rel["failure_rate"] = (daily_rel["failed"]/daily_rel["total"]*100).round(2)
    fig_rt = px.line(daily_rel, x="date", y="failure_rate",
                     title="Daily Workflow Failure Rate (%)", template=T,
                     labels={"date":"Date","failure_rate":"Failure Rate (%)"})
    fig_rt.update_traces(line_color="#ef4444", line_width=2)
    fig_rt.add_hline(y=daily_rel["failure_rate"].mean(), line_dash="dot",
                     line_color="#9ca3af", annotation_text="Avg failure rate")
    st.plotly_chart(fig_rt, use_container_width=True)
    takeaway("What This Means for Lindy",
        "Workflow failures aren't just a product problem — they're a revenue problem. Every failed run costs the user credits and delivers nothing. Without tracking failure rates by workflow type, the engineering team can't prioritize which fixes will have the biggest impact on retention.")
    st.markdown("### Full Reliability Breakdown")
    st.dataframe(rel.rename(columns={
        "workflow_type":"Workflow Type","failure_reason":"Failure Reason",
        "total_workflows":"Total Runs","successful":"Successful","failed":"Failed",
        "success_rate":"Success Rate (%)","avg_duration":"Avg Duration (s)"})\
        .fillna({"Failure Reason":"N/A"}), use_container_width=True)

# TAB 3 — Cancellation Risk
with t3:
    st.subheader("Which Users Are Most Likely to Cancel?")
    st.caption("Users who burn a high % of their monthly credits on failed workflows are the strongest churn signal. This model identifies them before they cancel.")
    c1,c2,c3 = st.columns(3)
    with c1: kpi("🔴 High Cancellation Risk",   f"{(user_health['churn_risk']=='High').sum():,} users",   "red")
    with c2: kpi("🟡 Medium Cancellation Risk", f"{(user_health['churn_risk']=='Medium').sum():,} users", "amber")
    with c3: kpi("🟢 Low Cancellation Risk",    f"{(user_health['churn_risk']=='Low').sum():,} users",    "green")
    high_pro = user_health[(user_health["churn_risk"]=="High") & (user_health["plan_type"]=="pro")].shape[0]
    insight(f"<b>{high_pro} Pro plan users</b> are at high cancellation risk. At $49.99/month each, that's <b>${high_pro*49.99:,.0f}/month</b> in immediate revenue risk.")
    col1, col2 = st.columns(2)
    with col1:
        risk_counts = user_health["churn_risk"].value_counts().reset_index()
        risk_counts.columns = ["churn_risk","count"]
        fig5 = px.pie(risk_counts, names="churn_risk", values="count",
                      title="Cancellation Risk Distribution Across All Users",
                      color="churn_risk", template=T,
                      color_discrete_map={"High":"#ef4444","Medium":"#f97316","Low":"#22c55e"})
        fig5.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig5, use_container_width=True)
    with col2:
        fig6 = px.scatter(user_health, x="pct_used", y="pct_wasted", color="churn_risk",
                          hover_data=["user_id","plan_type"],
                          title="Credit Usage vs Waste Rate — Each Dot Is a User",
                          template=T,
                          labels={"pct_used":"% of Monthly Credits Used",
                                  "pct_wasted":"% of Credits Wasted on Failures",
                                  "churn_risk":"Cancellation Risk"},
                          color_discrete_map={"High":"#ef4444","Medium":"#f97316","Low":"#22c55e"})
        st.plotly_chart(fig6, use_container_width=True)
    st.markdown("### Churn Risk Trend")
    cr_daily = cr.merge(users, on="user_id")
    cr_daily["date"] = cr_daily["created_at"].dt.date
    cr_daily["wasted"] = cr_daily.apply(lambda r: r["credits_used"] if r["status"]=="failed" else 0, axis=1)
    daily_waste = cr_daily.groupby("date").agg(
        credits=("credits_used","sum"), wasted=("wasted","sum")).reset_index()
    daily_waste["waste_rate"] = (daily_waste["wasted"]/daily_waste["credits"]*100).round(2)
    fig_ct = px.area(daily_waste, x="date", y="waste_rate",
                     title="Daily Credit Waste Rate Trend (%)", template=T,
                     labels={"date":"Date","waste_rate":"Waste Rate (%)"})
    fig_ct.update_traces(line_color="#ef4444", fillcolor="rgba(239,68,68,0.15)")
    st.plotly_chart(fig_ct, use_container_width=True)
    takeaway("What This Means for Lindy",
        "Churn doesn't happen overnight — it's a pattern. Users who consistently burn credits on failed workflows lose trust in the product before they cancel. Identifying these users 2-3 weeks early creates an intervention window: a credit refund, a support outreach, or a product fix can save the account.")
    st.markdown("### Users at High Cancellation Risk — Prioritize These")
    high = user_health[user_health["churn_risk"]=="High"][[
        "user_id","plan_type","pct_used","pct_wasted","failure_rate","churn_risk"
    ]].rename(columns={
        "user_id":"User ID","plan_type":"Plan","pct_used":"Credits Used (%)",
        "pct_wasted":"Credits Wasted (%)","failure_rate":"Failure Rate (%)","churn_risk":"Risk Level"})
    st.dataframe(high, use_container_width=True)

# TAB 4 — User Sentiment
with t4:
    st.subheader("What Are Users Saying — and Where?")
    st.caption("Lindy has a 4.9★ on G2 but 2.4★ on Trustpilot. That gap isn't noise — it signals which user segments are struggling. This tab breaks it down.")
    avg_by_platform = rv.groupby("platform")["rating"].mean().reset_index()
    avg_by_platform["platform"]   = avg_by_platform["platform"].str.title()
    avg_by_platform["avg_rating"] = avg_by_platform["rating"].round(2)
    top_complaint = rv.groupby("complaint_category")["review_id"].count()\
        .reset_index().sort_values("review_id", ascending=False).iloc[0]["complaint_category"]\
        .replace("_"," ").title()
    insight(f"<b>{top_complaint}</b> is the most common complaint category across all platforms. Fixing this one issue would have the highest impact on public sentiment.")
    col1, col2 = st.columns(2)
    with col1:
        fig7 = px.bar(avg_by_platform, x="platform", y="avg_rating",
                      title="Average Star Rating by Review Platform",
                      color="platform", text_auto=True, template=T,
                      labels={"platform":"Platform","avg_rating":"Average Rating (★)"})
        fig7.update_layout(showlegend=False, yaxis_range=[0,5])
        st.plotly_chart(fig7, use_container_width=True)
    with col2:
        sent = rv.copy()
        sent["platform"]  = sent["platform"].str.title()
        sent["sentiment"] = sent["sentiment"].str.title()
        sent_agg = sent.groupby(["platform","sentiment"])["review_id"].count().reset_index()
        sent_agg.columns = ["platform","sentiment","count"]
        fig8 = px.bar(sent_agg, x="platform", y="count", color="sentiment", barmode="group",
                      title="Positive vs Negative Reviews by Platform", template=T,
                      labels={"platform":"Platform","count":"Number of Reviews","sentiment":"Sentiment"},
                      color_discrete_map={"Positive":"#22c55e","Negative":"#ef4444","Neutral":"#9ca3af"})
        st.plotly_chart(fig8, use_container_width=True)
    st.markdown("### Sentiment Trend Over Time")
    rv["month"] = pd.to_datetime(rv["review_date"]).dt.to_period("M").astype(str)
    sent_trend = rv.groupby(["month","sentiment"])["review_id"].count().reset_index()
    sent_trend["sentiment"] = sent_trend["sentiment"].str.title()
    fig_st = px.line(sent_trend, x="month", y="review_id", color="sentiment",
                     title="Monthly Review Volume by Sentiment", template=T,
                     labels={"month":"Month","review_id":"Number of Reviews","sentiment":"Sentiment"},
                     color_discrete_map={"Positive":"#22c55e","Negative":"#ef4444","Neutral":"#9ca3af"})
    st.plotly_chart(fig_st, use_container_width=True)
    takeaway("What This Means for Lindy",
        "The G2 vs Trustpilot rating gap tells a clear story: power users love Lindy, everyday users struggle with it. Closing this gap requires fixing the credit transparency problem — not just the product.")
    st.markdown("### Reviews by Complaint Category")
    comp = rv.groupby("complaint_category")["review_id"].count().reset_index()\
        .sort_values("review_id", ascending=False)
    comp["complaint_category"] = comp["complaint_category"].str.replace("_"," ").str.title()
    fig9 = px.bar(comp, x="complaint_category", y="review_id",
                  title="Volume of Reviews by Complaint Category",
                  color="complaint_category", text_auto=True, template=T,
                  labels={"complaint_category":"Category","review_id":"Reviews"})
    fig9.update_layout(showlegend=False)
    st.plotly_chart(fig9, use_container_width=True)

# TAB 5 — Revenue Signals
with t5:
    st.subheader("Where Is Revenue Coming From — and What's at Risk?")
    st.caption("Revenue signals help GTM and leadership understand which plan segments drive the most value, which free users are close to converting, and how much MRR is at risk.")
    total_mrr    = user_health["monthly_revenue"].sum()
    at_risk_rev  = user_health[user_health["churn_risk"]=="High"]["monthly_revenue"].sum()
    upgrade_cands= user_health[(user_health["plan_type"]=="free") & (user_health["pct_used"]>=80)].shape[0]
    c1,c2,c3 = st.columns(3)
    with c1: kpi("Estimated Monthly Revenue",    f"${total_mrr:,.0f}", "green")
    with c2: kpi("Revenue at Risk (High Churn)", f"${at_risk_rev:,.0f}", "red",
                 f"▼ {at_risk_rev/total_mrr*100:.1f}% of MRR" if total_mrr>0 else "")
    with c3: kpi("Free Users Ready to Upgrade",  f"{upgrade_cands:,} users", "amber",
                 f"▲ ${upgrade_cands*49.99:,.0f} potential MRR")
    insight(f"If just <b>50% of upgrade-ready free users</b> convert to Pro, that's <b>${upgrade_cands*0.5*49.99:,.0f}/month</b> in new revenue — with zero new user acquisition needed.")
    col1, col2 = st.columns(2)
    with col1:
        rev_plan = user_health.groupby("plan_type")["monthly_revenue"].sum().reset_index()
        rev_plan["plan_type"] = rev_plan["plan_type"].str.title()
        fig10 = px.pie(rev_plan, names="plan_type", values="monthly_revenue",
                       title="Monthly Revenue Contribution by Plan", template=T,
                       color_discrete_sequence=["#6366f1","#22c55e","#f97316"])
        fig10.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig10, use_container_width=True)
    with col2:
        free_u = user_health[user_health["plan_type"]=="free"].copy()
        free_u["bucket"] = pd.cut(free_u["pct_used"],
            bins=[0,25,50,75,100,float("inf")],
            labels=["0–25%","25–50%","50–75%","75–100%","100%+"], right=False)
        funnel = free_u["bucket"].value_counts().reset_index()
        funnel.columns = ["Credit Usage","Free Users"]
        funnel = funnel.sort_values("Credit Usage")
        fig11 = px.bar(funnel, x="Credit Usage", y="Free Users",
                       title="Free Trial Conversion Funnel — Credit Usage Buckets",
                       color="Credit Usage", text_auto=True, template=T)
        fig11.update_layout(showlegend=False)
        st.plotly_chart(fig11, use_container_width=True)
    st.markdown("### Monthly Revenue at Risk Trend")
    cr_rev = cr.merge(users[["user_id","plan_type"]], on="user_id")
    cr_rev["month"]  = cr_rev["created_at"].dt.to_period("M").astype(str)
    cr_rev["wasted"] = cr_rev.apply(lambda r: r["credits_used"] if r["status"]=="failed" else 0, axis=1)
    cr_rev["revenue_at_risk"] = cr_rev["plan_type"].map(PLAN_PRICE) * \
        (cr_rev["wasted"] / cr_rev.groupby("user_id")["credits_used"].transform("sum"))
    rev_trend = cr_rev.groupby("month")["revenue_at_risk"].sum().reset_index()
    fig12 = px.area(rev_trend, x="month", y="revenue_at_risk",
                    title="Estimated Monthly Revenue at Risk from Credit Waste", template=T,
                    labels={"month":"Month","revenue_at_risk":"Revenue at Risk ($)"})
    fig12.update_traces(line_color="#ef4444", fillcolor="rgba(239,68,68,0.15)")
    st.plotly_chart(fig12, use_container_width=True)
    takeaway("What This Means for Lindy",
        "Revenue at risk isn't a future problem — it's happening now. High-churn users represent real MRR that can be saved with proactive intervention. Free users hitting their credit ceiling are the highest-probability upgrade candidates in the funnel.")
    st.markdown("### Revenue at Risk by Plan")
    risk_rev = user_health[user_health["churn_risk"]=="High"].groupby("plan_type").agg(
        users=("user_id","count"), mrr=("monthly_revenue","sum")).reset_index()
    risk_rev["plan_type"] = risk_rev["plan_type"].str.title()
    risk_rev.columns = ["Plan","At-Risk Users","Monthly Revenue at Risk ($)"]
    st.dataframe(risk_rev, use_container_width=True)
    st.markdown("### Free Users Most Likely to Upgrade")
    upg = user_health[(user_health["plan_type"]=="free") & (user_health["pct_used"]>=80)]\
        [["user_id","pct_used","total_wasted"]]\
        .rename(columns={"user_id":"User ID","pct_used":"Credits Used (%)","total_wasted":"Credits Wasted"})\
        .sort_values("Credits Used (%)", ascending=False)
    st.dataframe(upg, use_container_width=True)