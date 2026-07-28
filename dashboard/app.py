import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Banking Lakehouse Dashboard", layout="wide")

# ----------------------------------------------------------------------------
# CONFIG — flip this to False once real data is flowing through Postgres
# ----------------------------------------------------------------------------
USE_MOCK_DATA = True

PG_CONFIG = dict(
    host="etl-postgres",
    port=5432,
    dbname="etl_db",
    user="etl_user",
    password="etl_password",
)


def get_conn():
    return psycopg2.connect(**PG_CONFIG)


# ----------------------------------------------------------------------------
# MOCK DATA GENERATORS — remove/ignore once USE_MOCK_DATA = False
# ----------------------------------------------------------------------------
def mock_dq_check_results() -> pd.DataFrame:
    rng = np.random.default_rng(14)
    checks = [
        "unique_stg_accounts_account_id",
        "not_null_stg_accounts_account_id",
        "unique_stg_customers_customer_id",
        "accepted_values_stg_customers_segment",
        "unique_stg_transactions_transaction_id",
        "not_null_stg_transactions_account_id",
        "accepted_values_stg_transactions_status",
        "unique_fact_transaction_transaction_id",
        "relationships_fact_transaction_account_id",
        "reversed_txn_has_original",
        "no_duplicate_transactions",
        "balance_identity_holds",
        "settlement_variance_within_tolerance",
        "unique_stg_settlements_settlement_batch_id",
        "unique_stg_reference__branches_branch_id",
    ]

    rows = []
    now = datetime.now()
    for day_offset in range(7):
        run_at = now - timedelta(days=day_offset, hours=int(rng.integers(0, 5)))
        for check in checks:
            fails = rng.random() < 0.12
            failures = int(rng.integers(1, 50)) if fails else 0
            rows.append(
                {
                    "run_at": run_at,
                    "check_name": check,
                    "status": "fail" if fails else "pass",
                    "execution_time": round(float(rng.uniform(0.1, 3.5)), 2),
                    "failures": failures,
                }
            )
    return pd.DataFrame(rows)


def mock_transactions() -> pd.DataFrame:
    rng = np.random.default_rng(14)
    branches = [f"Branch {i}" for i in range(1, 9)]
    channels = ["BRANCH", "ATM", "MOBILE", "INTERNET_BANKING", "CARD", "WALLET"]
    statuses = ["SUCCESS", "FAILED", "REVERSED", "PENDING"]
    rows = []
    for day_offset in range(14):
        d = datetime.now().date() - timedelta(days=day_offset)
        for branch in branches:
            for channel in channels:
                count = int(rng.integers(20, 400))
                amount = round(float(rng.uniform(5000, 200000)), 2)
                status = rng.choice(statuses, p=[0.90, 0.05, 0.03, 0.02])
                rows.append(
                    {
                        "transaction_date": d,
                        "branch_name": branch,
                        "channel": channel,
                        "status": status,
                        "transaction_count": count,
                        "total_amount": amount,
                    }
                )
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# REAL DATA LOADERS — used once USE_MOCK_DATA = False
# ----------------------------------------------------------------------------
def load_dq_check_results() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql(
        "SELECT * FROM dq_check_results ORDER BY run_at DESC LIMIT 500", conn
    )
    conn.close()
    return df


def load_business_metrics() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM business_metrics_daily", conn)
    conn.close()
    return df


# ----------------------------------------------------------------------------
# DATA ACCESS SWITCH
# ----------------------------------------------------------------------------
def get_dq_results() -> pd.DataFrame:
    return mock_dq_check_results() if USE_MOCK_DATA else load_dq_check_results()


def get_business_metrics() -> pd.DataFrame:
    return mock_transactions() if USE_MOCK_DATA else load_business_metrics()


# ----------------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------------
st.sidebar.title("Banking Lakehouse")
page = st.sidebar.radio("Page", ["Pipeline Health", "Business Metrics"])

if USE_MOCK_DATA:
    st.sidebar.warning("Showing MOCK data — flip USE_MOCK_DATA to False once real pipelines land.")

if page == "Pipeline Health":
    st.title("Pipeline Health")
    st.caption("Data quality check results across dbt test runs")

    df = get_dq_results()
    df["run_at"] = pd.to_datetime(df["run_at"])

    latest_run = df["run_at"].max()
    latest = df[df["run_at"] == latest_run]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Checks Run (latest)", len(latest))
    col2.metric("Passed", int((latest["status"] == "pass").sum()))
    col3.metric("Failed", int((latest["status"] != "pass").sum()))
    pass_rate = (latest["status"] == "pass").mean() * 100
    col4.metric("Pass Rate", f"{pass_rate:.0f}%")

    st.subheader("Latest Run — Check Results")
    st.dataframe(
        latest[["check_name", "status", "failures", "execution_time"]]
        .sort_values("status")
        .reset_index(drop=True),
        use_container_width=True,
    )

    st.subheader("Failure Trend Over Time")
    trend = (
        df.groupby([df["run_at"].dt.date.rename("run_date"), "status"])
        .size()
        .reset_index(name="count")
    )
    fig = px.bar(
        trend,
        x="run_date",
        y="count",
        color="status",
        barmode="stack",
        color_discrete_map={"pass": "#2ca02c", "fail": "#d62728"},
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Most Frequently Failing Checks")
    fail_counts = (
        df[df["status"] != "pass"]
        .groupby("check_name")
        .size()
        .reset_index(name="fail_count")
        .sort_values("fail_count", ascending=False)
        .head(10)
    )
    if not fail_counts.empty:
        fig2 = px.bar(fail_counts, x="fail_count", y="check_name", orientation="h")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No failing checks in this data.")

else:
    st.title("Business Metrics")
    st.caption("Transaction volume, value, and failure rate by branch and channel")

    df = get_business_metrics()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    min_date, max_date = df["transaction_date"].min(), df["transaction_date"].max()
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
