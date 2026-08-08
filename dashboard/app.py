import os
import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

# ----------------------------------------------------------------------------
# CONFIGURATION & DATABASE CONNECTION
# ----------------------------------------------------------------------------
st.set_page_config(
    page_title="Banking Lakehouse Dashboard",
    page_icon="🏦",
    layout="wide"
)

# Use environment variables with fallbacks for seamless local and Docker execution
PG_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "etl-postgres"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    dbname=os.getenv("POSTGRES_DB", "etl_db"),
    user=os.getenv("POSTGRES_USER", "etl_user"),
    password=os.getenv("POSTGRES_PASSWORD", "etl_password"),
)


def get_conn():
    return psycopg2.connect(**PG_CONFIG)


# ----------------------------------------------------------------------------
# DATA LOADERS WITH CACHING (60-second TTL)
# ----------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_dq_check_results() -> pd.DataFrame:
    try:
        conn = get_conn()
        df = pd.read_sql(
            "SELECT * FROM audit.dq_check_results ORDER BY checked_at DESC LIMIT 500", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_business_metrics() -> pd.DataFrame:
    try:
        conn = get_conn()
        df = pd.read_sql("SELECT * FROM business_metrics_daily", conn)
        conn.close()
        return df
    except Exception:
        # Table doesn't exist yet - Gold layer not built. Show empty state instead of crashing.
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_fraud_alerts() -> pd.DataFrame:
    try:
        conn = get_conn()
        df = pd.read_sql(
            "SELECT * FROM audit.fraud_alerts ORDER BY created_at DESC LIMIT 500", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


# ----------------------------------------------------------------------------
# NAVIGATION & SIDEBAR
# ----------------------------------------------------------------------------
st.sidebar.title("Banking Lakehouse")
page = st.sidebar.radio(
    "Select View",
    ["Pipeline Health", "Business Metrics", "Fraud/Risk Monitoring"]
)

# Button to manually clear Streamlit cache if needed
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# ----------------------------------------------------------------------------
# PAGE 1: PIPELINE HEALTH
# ----------------------------------------------------------------------------
if page == "Pipeline Health":
    st.title("Pipeline Health")
    st.caption("Data quality check results across bronze checks and dbt test runs")

    df = load_dq_check_results()

    if df.empty:
        st.warning("No DQ check results found in audit.dq_check_results. Ensure test_dq_local.py or your ingestion DAG has executed.")
        st.stop()

    df["checked_at"] = pd.to_datetime(df["checked_at"])

    latest_run_id = df.sort_values("checked_at", ascending=False)["run_id"].iloc[0]
    latest = df[df["run_id"] == latest_run_id]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Checks Run (latest)", len(latest))
    col2.metric("Passed", int((latest["status"] == "PASS").sum()))
    col3.metric("Failed", int((latest["status"] == "FAIL").sum()))
    pass_rate = (latest["status"] == "PASS").mean() * 100 if len(latest) > 0 else 0
    col4.metric("Pass Rate", f"{pass_rate:.0f}%")

    st.subheader("Latest Run — Check Results")
    st.dataframe(
        latest[["layer", "table_name", "check_name", "status", "failed_count", "row_count", "details"]]
        .sort_values("status")
        .reset_index(drop=True),
        use_container_width=True,
    )

    st.subheader("Failure Trend Over Time")
    trend = (
        df.groupby([df["checked_at"].dt.date.rename("check_date"), "status"])
        .size()
        .reset_index(name="count")
    )
    fig = px.bar(
        trend,
        x="check_date",
        y="count",
        color="status",
        barmode="stack",
        color_discrete_map={"PASS": "#2ca02c", "FAIL": "#d62728"},
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Most Frequently Failing Checks")
    fail_counts = (
        df[df["status"] == "FAIL"]
        .groupby("check_name").size().reset_index(name="fail_count")
        .sort_values("fail_count", ascending=False).head(10)
    )
    if not fail_counts.empty:
        fig2 = px.bar(fail_counts, x="fail_count", y="check_name", orientation="h")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No failing checks recorded.")

# ----------------------------------------------------------------------------
# PAGE 2: BUSINESS METRICS
# ----------------------------------------------------------------------------
elif page == "Business Metrics":
    st.title("Business Metrics")
    st.caption("Transaction volume, value, and failure rate by branch and channel")

    df = load_business_metrics()

    if df.empty:
        st.warning("business_metrics_daily doesn't exist yet - waiting on the Gold layer dbt models.")
        st.stop()

    df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    min_date, max_date = df["transaction_date"].min(), df["transaction_date"].max()

    # Safely handle single-date dataset vs multi-date range
    if min_date == max_date:
        st.info(f"Showing data for: {min_date.strftime('%Y-%m-%d')}")
        filtered = df
    else:
        date_range = st.slider(
            "Date range",
            min_value=min_date.to_pydatetime(),
            max_value=max_date.to_pydatetime(),
            value=(min_date.to_pydatetime(), max_date.to_pydatetime()),
        )
        filtered = df[
            (df["transaction_date"] >= date_range[0])
            & (df["transaction_date"] <= date_range[1])
        ]

    total_txns = filtered["transaction_count"].sum()
    total_value = filtered["total_amount"].sum()
    failed = filtered[filtered["status"] == "FAILED"]["transaction_count"].sum()
    fail_rate = (failed / total_txns * 100) if total_txns else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Transactions", f"{total_txns:,.0f}")
    col2.metric("Total Value (NPR)", f"{total_value:,.0f}")
    col3.metric("Failure Rate", f"{fail_rate:.2f}%")

    st.subheader("Daily Volume & Value")
    daily = (
        filtered.groupby("transaction_date")
        .agg(transaction_count=("transaction_count", "sum"), total_amount=("total_amount", "sum"))
        .reset_index()
    )
    fig = px.line(daily, x="transaction_date", y="transaction_count", title="Daily Transaction Count")
    st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Volume by Branch")
        by_branch = (
            filtered.groupby("branch_name")["transaction_count"].sum().reset_index()
            .sort_values("transaction_count", ascending=False)
        )
        fig_b = px.bar(by_branch, x="branch_name", y="transaction_count")
        st.plotly_chart(fig_b, use_container_width=True)

    with col_b:
        st.subheader("Volume by Channel")
        by_channel = (
            filtered.groupby("channel")["transaction_count"].sum().reset_index()
            .sort_values("transaction_count", ascending=False)
        )
        fig_c = px.pie(by_channel, names="channel", values="transaction_count")
        st.plotly_chart(fig_c, use_container_width=True)

    st.subheader("Failure Rate by Branch")
    fail_by_branch = (
        filtered.groupby(["branch_name", "status"])["transaction_count"].sum()
        .reset_index()
    )
    fig_d = px.bar(
        fail_by_branch,
        x="branch_name",
        y="transaction_count",
        color="status",
        barmode="stack",
    )
    st.plotly_chart(fig_d, use_container_width=True)

# ----------------------------------------------------------------------------
# PAGE 3: FRAUD / RISK MONITORING
# ----------------------------------------------------------------------------
else:
    st.title("Fraud/Risk Monitoring")
    st.caption("Suspicious transactions flagged by the streaming fraud rule (amount > 10,000)")

    df = load_fraud_alerts()

    if df.empty:
        st.warning(
            "No fraud alerts yet - run src/fraud_alerts_consumer.py to pull events "
            "from the banking.fraud.alerts Kafka topic into Postgres."
        )
        st.stop()

    # Data transformation & Null handling
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    df["merchant_category_code"] = df["merchant_category_code"].fillna("N/A (Non-Card)")
    df["channel"] = df["channel"].fillna("Unknown")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Alerts", len(df))
    col2.metric("Total Flagged Amount (NPR)", f"{df['amount'].sum():,.0f}")
    col3.metric("Unique Accounts Flagged", df["account_id"].nunique())

    st.subheader("Recent Alerts")
    st.dataframe(
        df[["event_ts", "transaction_id", "account_id", "channel",
            "merchant_category_code", "amount", "status"]]
        .sort_values("event_ts", ascending=False)
        .reset_index(drop=True),
        use_container_width=True,
    )

    # Render visualizations side-by-side
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.subheader("Alerts by Channel")
        by_channel = df.groupby("channel").size().reset_index(name="alert_count")
        fig_e = px.bar(by_channel, x="channel", y="alert_count", color="channel")
        st.plotly_chart(fig_e, use_container_width=True)

    with col_chart2:
        st.subheader("Alerts by Merchant Category")
        by_mcc = df.groupby("merchant_category_code").size().reset_index(name="alert_count")
        fig_f = px.pie(by_mcc, names="merchant_category_code", values="alert_count", hole=0.3)
        st.plotly_chart(fig_f, use_container_width=True)