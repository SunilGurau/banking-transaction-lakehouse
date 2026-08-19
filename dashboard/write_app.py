import pathlib

APP_CODE = r'''import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

# ---------------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Banking Lakehouse Dashboard",
    page_icon="\U0001f3e6",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CUSTOM CSS  - dark glass-morphism
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp {
    background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
    min-height: 100vh;
}
[data-testid="stSidebar"] {
    background: rgba(255,255,255,0.04);
    backdrop-filter: blur(16px);
    border-right: 1px solid rgba(255,255,255,0.08);
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stMetric"] {
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 14px;
    padding: 1rem 1.2rem;
    backdrop-filter: blur(10px);
    transition: transform .2s;
}
[data-testid="stMetric"]:hover { transform: translateY(-3px); }
[data-testid="stMetricLabel"] {
    color: #94a3b8 !important;
    font-size: .78rem !important;
    text-transform: uppercase;
    letter-spacing: .06em;
}
[data-testid="stMetricValue"] {
    color: #f1f5f9 !important;
    font-size: 1.7rem !important;
    font-weight: 700;
}
[data-testid="stMetricDelta"] { color: #34d399 !important; }
h1 { color: #f8fafc !important; font-weight: 700 !important; }
h2, h3 { color: #cbd5e1 !important; font-weight: 600 !important; }
hr { border-color: rgba(255,255,255,0.08) !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# DB CONNECTION  - real etl_postgres (banking-analytics / admin / testpassword)
# ---------------------------------------------------------------------------
PG_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "etl-postgres"),
    port=int(os.getenv("POSTGRES_PORT", 5432)),
    dbname=os.getenv("POSTGRES_DB", "banking-analytics"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "testpassword"),
)


def get_conn():
    return psycopg2.connect(**PG_CONFIG)


def _q(sql):
    """Execute SQL and return DataFrame; show error and return empty DF on failure."""
    try:
        conn = get_conn()
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as exc:
        st.error(f"Query error: {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# SHARED PLOTLY THEME
# ---------------------------------------------------------------------------
PLY = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#cbd5e1", family="Inter"),
    margin=dict(t=40, b=30, l=10, r=10),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#94a3b8")),
)
PAL = ["#818cf8", "#34d399", "#fb923c", "#38bdf8", "#f472b6", "#a78bfa"]
STATUS_MAP = {
    "SUCCESS": "#34d399",
    "FAILED": "#f87171",
    "PENDING": "#fbbf24",
    "REVERSED": "#a78bfa",
}

# ---------------------------------------------------------------------------
# CACHED DATA LOADERS  (60 s TTL)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_transactions() -> pd.DataFrame:
    return _q("""
        SELECT
            ft.transaction_id,
            ft.transaction_date,
            ft.account_id,
            ft.branch_id,
            db.branch_name,
            db.province,
            db.region,
            ft.channel,
            ft.transaction_type_code,
            dtt.transaction_type_name,
            ft.status,
            ft.amount,
            ft.fee_amount,
            ft.net_amount
        FROM fact_transaction ft
        LEFT JOIN dim_branch db ON ft.branch_id = db.branch_id
        LEFT JOIN dim_transaction_type dtt
               ON ft.transaction_type_code = dtt.transaction_type_code
    """)


@st.cache_data(ttl=60)
def load_settlement() -> pd.DataFrame:
    return _q("""
        SELECT
            sr.settlement_date,
            sr.channel,
            dtt.transaction_type_name,
            sr.settled_transaction_count,
            sr.settled_gross_amount,
            sr.actual_transaction_count,
            sr.actual_gross_amount,
            sr.count_variance,
            sr.amount_variance,
            sr.variance_pct,
            sr.reconciliation_status
        FROM fact_settlement_reconciliation sr
        LEFT JOIN dim_transaction_type dtt
               ON sr.transaction_type_code = dtt.transaction_type_code
    """)


@st.cache_data(ttl=60)
def load_balance() -> pd.DataFrame:
    return _q("""
        SELECT
            fab.balance_date,
            fab.account_id,
            fab.branch_id,
            db.branch_name,
            db.province,
            fab.opening_balance,
            fab.credit_total,
            fab.debit_total,
            fab.closing_balance,
            fab.net_change,
            fab.is_balance_valid
        FROM fact_daily_account_balance fab
        LEFT JOIN dim_branch db ON fab.branch_id = db.branch_id
    """)


@st.cache_data(ttl=60)
def load_dq() -> pd.DataFrame:
    return _q(
        "SELECT * FROM audit.dq_check_results ORDER BY created_at DESC LIMIT 2000"
    )


# ---------------------------------------------------------------------------
# SIDEBAR  NAVIGATION
# ---------------------------------------------------------------------------
st.sidebar.markdown("""
<div style='text-align:center;padding:1rem 0 0.5rem'>
  <span style='font-size:2.5rem'>\U0001f3e6</span>
  <h2 style='margin:0;color:#f1f5f9;font-size:1.1rem;font-weight:700'>Banking Lakehouse</h2>
  <p style='margin:0;color:#64748b;font-size:.75rem'>Analytics Platform</p>
</div>
<hr style='margin:.8rem 0'/>
""", unsafe_allow_html=True)

PAGES = [
    "\U0001f4ca Transaction Operations",
    "\U0001f3e2 Branch & Channel",
    "\U0001f4b0 Settlement Reconciliation",
    "\U000026a1 Pipeline Health",
]
page = st.sidebar.radio("Navigate", PAGES, label_visibility="collapsed")

if st.sidebar.button("\U0001f504 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("""
<hr/>
<div style='color:#475569;font-size:.7rem;text-align:center'>
  Connected to <b style='color:#818cf8'>banking-analytics</b><br/>
  Jan 2026 data
</div>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# HELPER: date range filter
# ---------------------------------------------------------------------------
def date_filter(df: pd.DataFrame, col: str = "transaction_date") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_datetime(df[col])
    mn, mx = df[col].min().date(), df[col].max().date()
    if mn == mx:
        return df
    ca, cb = st.columns(2)
    s = ca.date_input("From", value=mn, min_value=mn, max_value=mx)
    e = cb.date_input("To",   value=mx, min_value=mn, max_value=mx)
    return df[(df[col].dt.date >= s) & (df[col].dt.date <= e)]


# ===========================================================================
# PAGE 1  TRANSACTION OPERATIONS
# ===========================================================================
if page == PAGES[0]:
    st.title("\U0001f4ca Transaction Operations")
    st.caption("Volume, value, failure rate, and channel mix \u2014 Jan 2026")

    raw = load_transactions()
    if raw.empty:
        st.warning("No data in fact_transaction.")
        st.stop()

    df = date_filter(raw)

    total  = len(df)
    value  = df["amount"].sum()
    failed = (df["status"] == "FAILED").sum()
    frate  = failed / total * 100 if total else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Transactions", f"{total:,.0f}")
    c2.metric("Total Value (NPR)",  f"{value:,.0f}")
    c3.metric("Failed",             f"{failed:,.0f}")
    c4.metric("Failure Rate",       f"{frate:.2f}%")
    st.markdown("---")

    # Daily volume trend
    st.subheader("Daily Transaction Volume")
    daily = (
        df.groupby(df["transaction_date"].dt.date)
        .agg(cnt=("transaction_id", "count"), val=("amount", "sum"))
        .reset_index()
        .rename(columns={"transaction_date": "date"})
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily["date"], y=daily["cnt"],
        mode="lines+markers",
        line=dict(color="#818cf8", width=2.5),
        fill="tozeroy", fillcolor="rgba(129,140,248,0.1)",
        name="Count",
    ))
    fig.update_layout(**PLY, xaxis_title="Date", yaxis_title="Transactions")
    st.plotly_chart(fig, use_container_width=True)

    # Status + Channel side-by-side
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Transaction Status Breakdown")
        stt = df["status"].value_counts().reset_index()
        stt.columns = ["status", "count"]
        fig2 = px.pie(
            stt, names="status", values="count", hole=0.45,
            color="status", color_discrete_map=STATUS_MAP,
        )
        fig2.update_layout(**PLY)
        fig2.update_traces(textfont_color="#f1f5f9")
        st.plotly_chart(fig2, use_container_width=True)

    with col_r:
        st.subheader("Channel Mix (by Value)")
        ch = df.groupby("channel")["amount"].sum().reset_index()
        fig3 = px.pie(
            ch, names="channel", values="amount", hole=0.45,
            color_discrete_sequence=PAL,
        )
        fig3.update_layout(**PLY)
        fig3.update_traces(textfont_color="#f1f5f9")
        st.plotly_chart(fig3, use_container_width=True)

    # Stacked bar by type & status
    st.subheader("Transactions by Type & Status")
    ta = (
        df.groupby(["transaction_type_name", "status"])
        .size()
        .reset_index(name="count")
    )
    fig4 = px.bar(
        ta, x="transaction_type_name", y="count",
        color="status", barmode="stack",
        color_discrete_map=STATUS_MAP,
    )
    fig4.update_layout(**PLY, xaxis_title="Type", yaxis_title="Count")
    st.plotly_chart(fig4, use_container_width=True)

    # Failure rate by channel
    st.subheader("Failure Rate by Channel")
    cf = (
        df.groupby("channel")
        .apply(lambda x: pd.Series({"fail_rate": (x["status"] == "FAILED").mean() * 100}))
        .reset_index()
    )
    fig5 = px.bar(
        cf, x="channel", y="fail_rate",
        color="fail_rate",
        color_continuous_scale=["#34d399", "#fbbf24", "#f87171"],
        labels={"fail_rate": "Failure Rate (%)"},
    )
    fig5.update_layout(**PLY)
    st.plotly_chart(fig5, use_container_width=True)


# ===========================================================================
# PAGE 2  BRANCH & CHANNEL PERFORMANCE
# ===========================================================================
elif page == PAGES[1]:
    st.title("\U0001f3e2 Branch & Channel Performance")
    st.caption("Volume, value, and failure rate by branch, region, and channel")

    raw = load_transactions()
    if raw.empty:
        st.warning("No data in fact_transaction.")
        st.stop()

    df = date_filter(raw)

    br = (
        df.groupby(["branch_id", "branch_name", "province", "region"])
        .agg(
            txns=("transaction_id", "count"),
            value=("amount", "sum"),
            fails=("status", lambda x: (x == "FAILED").sum()),
        )
        .reset_index()
    )
    br["fail_rate"] = br["fails"] / br["txns"] * 100

    top   = br.nlargest(1, "txns").iloc[0]
    worst = br.nlargest(1, "fail_rate").iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Branches",    len(br))
    c2.metric("Top Branch (Vol)",  top["branch_name"],   f"{top['txns']:,.0f} txns")
    c3.metric("Highest Fail Rate", worst["branch_name"], f"{worst['fail_rate']:.1f}%")
    st.markdown("---")

    # Top-20 branch bar
    st.subheader("Transaction Volume by Branch (Top 20)")
    t20 = br.nlargest(20, "txns").sort_values("txns")
    fig = px.bar(
        t20, x="txns", y="branch_name", orientation="h",
        color="txns",
        color_continuous_scale=["#312e81", "#818cf8", "#c7d2fe"],
        labels={"txns": "Transactions", "branch_name": "Branch"},
    )
    fig.update_layout(**PLY, height=600)
    st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Volume by Region")
        rg = df.groupby("region").size().reset_index(name="count")
        figr = px.pie(rg, names="region", values="count", hole=0.4,
                      color_discrete_sequence=PAL)
        figr.update_layout(**PLY)
        figr.update_traces(textfont_color="#f1f5f9")
        st.plotly_chart(figr, use_container_width=True)

    with col_b:
        st.subheader("Volume by Province")
        pv = df.groupby("province").size().reset_index(name="count")
        figp = px.bar(
            pv.sort_values("count"),
            x="count", y="province", orientation="h",
            color_discrete_sequence=["#38bdf8"],
        )
        figp.update_layout(**PLY)
        st.plotly_chart(figp, use_container_width=True)

    # Heatmap
    st.subheader("Channel \u00d7 Branch Heatmap (Top 15)")
    t15 = br.nlargest(15, "txns")["branch_name"].tolist()
    hd = (
        df[df["branch_name"].isin(t15)]
        .groupby(["branch_name", "channel"])
        .size()
        .reset_index(name="count")
        .pivot(index="branch_name", columns="channel", values="count")
        .fillna(0)
    )
    figh = px.imshow(
        hd, color_continuous_scale="Purples", aspect="auto",
        labels=dict(x="Channel", y="Branch", color="Txns"),
    )
    figh.update_layout(**PLY, height=500)
    st.plotly_chart(figh, use_container_width=True)

    # Worst failure rate
    st.subheader("Top 10 Branches by Failure Rate")
    w10 = br.nlargest(10, "fail_rate").sort_values("fail_rate")
    figf = px.bar(
        w10, x="fail_rate", y="branch_name", orientation="h",
        color="fail_rate",
        color_continuous_scale=["#fbbf24", "#f87171", "#dc2626"],
        labels={"fail_rate": "Failure Rate (%)", "branch_name": "Branch"},
    )
    figf.update_layout(**PLY)
    st.plotly_chart(figf, use_container_width=True)


# ===========================================================================
# PAGE 3  SETTLEMENT RECONCILIATION
# ===========================================================================
elif page == PAGES[2]:
    st.title("\U0001f4b0 Settlement Reconciliation")
    st.caption("Batch vs actual totals, variance analysis, and unmatched records")

    raw = load_settlement()
    if raw.empty:
        st.warning("No data in fact_settlement_reconciliation.")
        st.stop()

    df = date_filter(raw, col="settlement_date")

    matched   = (df["reconciliation_status"] == "MATCHED").sum()
    unmatched = (df["reconciliation_status"] == "UNMATCHED").sum()
    total_rec = len(df)
    match_pct = matched / total_rec * 100 if total_rec else 0
    tot_var   = df["amount_variance"].abs().sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Records",  f"{total_rec:,.0f}")
    c2.metric("Matched",        f"{matched:,.0f}",   f"{match_pct:.1f}%")
    c3.metric("Unmatched",      f"{unmatched:,.0f}")
    c4.metric("Total Variance", f"{tot_var:,.0f} NPR")
    st.markdown("---")

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Reconciliation Status")
        sp = df["reconciliation_status"].value_counts().reset_index()
        sp.columns = ["status", "count"]
        fsp = px.pie(
            sp, names="status", values="count", hole=0.45,
            color="status",
            color_discrete_map={"MATCHED": "#34d399", "UNMATCHED": "#f87171"},
        )
        fsp.update_layout(**PLY)
        fsp.update_traces(textfont_color="#f1f5f9")
        st.plotly_chart(fsp, use_container_width=True)

    with col_b:
        st.subheader("Variance by Channel")
        cv = (
            df.groupby("channel")["amount_variance"]
            .apply(lambda x: x.abs().sum())
            .reset_index()
            .sort_values("amount_variance", ascending=False)
        )
        fcv = px.bar(
            cv, x="channel", y="amount_variance",
            color_discrete_sequence=["#fb923c"],
            labels={"amount_variance": "Abs Variance (NPR)"},
        )
        fcv.update_layout(**PLY)
        st.plotly_chart(fcv, use_container_width=True)

    # Daily variance trend
    st.subheader("Daily Variance Trend")
    dv = (
        df.groupby(df["settlement_date"].dt.date)
        .agg(
            var=("amount_variance", lambda x: x.abs().sum()),
            unm=("reconciliation_status", lambda x: (x == "UNMATCHED").sum()),
        )
        .reset_index()
        .rename(columns={"settlement_date": "date"})
    )
    fdv = go.Figure()
    fdv.add_trace(go.Bar(
        x=dv["date"], y=dv["var"],
        name="Abs Variance", marker_color="#fb923c", opacity=0.8,
    ))
    fdv.add_trace(go.Scatter(
        x=dv["date"], y=dv["unm"],
        name="Unmatched Count", yaxis="y2",
        mode="lines+markers", line=dict(color="#f87171", width=2),
    ))
    fdv.update_layout(
        **PLY,
        yaxis=dict(title="Variance (NPR)", color="#fb923c"),
        yaxis2=dict(title="Unmatched", overlaying="y", side="right", color="#f87171"),
    )
    st.plotly_chart(fdv, use_container_width=True)

    # Settled vs Actual grouped bar
    st.subheader("Settled vs Actual by Transaction Type")
    tc = df.groupby("transaction_type_name").agg(
        settled=("settled_gross_amount", "sum"),
        actual=("actual_gross_amount",   "sum"),
    ).reset_index()
    ftc = go.Figure([
        go.Bar(name="Settled", x=tc["transaction_type_name"], y=tc["settled"],
               marker_color="#818cf8"),
        go.Bar(name="Actual",  x=tc["transaction_type_name"], y=tc["actual"],
               marker_color="#34d399"),
    ])
    ftc.update_layout(**PLY, barmode="group",
                      xaxis_title="Type", yaxis_title="Amount (NPR)")
    st.plotly_chart(ftc, use_container_width=True)

    # Unmatched table
    st.subheader("Unmatched Records Detail")
    um = df[df["reconciliation_status"] == "UNMATCHED"][[
        "settlement_date", "channel", "transaction_type_name",
        "settled_gross_amount", "actual_gross_amount",
        "amount_variance", "variance_pct",
    ]].sort_values("amount_variance", ascending=False)
    st.dataframe(um.reset_index(drop=True), use_container_width=True)


# ===========================================================================
# PAGE 4  PIPELINE HEALTH
# ===========================================================================
elif page == PAGES[3]:
    st.title("\U000026a1 Pipeline Health")
    st.caption("Table sizes, balance validity, and DQ check results")

    # Table row counts
    st.subheader("Gold Layer Table Sizes")
    dcnt = _q("""
        SELECT 'fact_transaction'               AS tbl, COUNT(*) AS cnt FROM fact_transaction
        UNION ALL SELECT 'fact_daily_account_balance',   COUNT(*) FROM fact_daily_account_balance
        UNION ALL SELECT 'fact_settlement_reconciliation',COUNT(*) FROM fact_settlement_reconciliation
        UNION ALL SELECT 'fact_fraud_alert',              COUNT(*) FROM fact_fraud_alert
        UNION ALL SELECT 'dim_branch',                   COUNT(*) FROM dim_branch
        UNION ALL SELECT 'dim_customer',                 COUNT(*) FROM dim_customer
        UNION ALL SELECT 'dim_account',                  COUNT(*) FROM dim_account
        UNION ALL SELECT 'dim_channel',                  COUNT(*) FROM dim_channel
        UNION ALL SELECT 'dim_transaction_type',         COUNT(*) FROM dim_transaction_type
    """)
    if not dcnt.empty:
        fig = px.bar(
            dcnt.sort_values("cnt"),
            x="cnt", y="tbl", orientation="h",
            color="cnt",
            color_continuous_scale=["#312e81", "#818cf8", "#c7d2fe"],
            labels={"cnt": "Row Count", "tbl": "Table"},
        )
        fig.update_layout(**PLY, height=380)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Balance validity
    st.subheader("Account Balance Validity")
    bal = load_balance()
    if not bal.empty:
        valid   = int(bal["is_balance_valid"].sum())
        invalid = int((~bal["is_balance_valid"]).sum())
        cv1, cv2, cv3 = st.columns(3)
        cv1.metric("Total Records",   f"{len(bal):,.0f}")
        cv2.metric("Valid Balances",  f"{valid:,.0f}")
        cv3.metric("Invalid/Anomaly", f"{invalid:,.0f}")

    st.markdown("---")

    # DQ check results
    dq = load_dq()
    if dq.empty:
        st.info(
            "No DQ results in **audit.dq_check_results** yet.\n\n"
            "Run ingestion DAGs or `src/test_dq_local.py` to populate this table."
        )
    else:
        dq["created_at"] = pd.to_datetime(dq["created_at"])
        latest_rid = dq.sort_values("created_at", ascending=False)["run_id"].iloc[0]
        lt = dq[dq["run_id"] == latest_rid]

        chk = len(lt)
        psd = int((lt["status"] == "PASS").sum())
        fld = int((lt["status"] == "FAIL").sum())
        prt = psd / chk * 100 if chk else 0

        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Checks Run", chk)
        d2.metric("Passed",     psd)
        d3.metric("Failed",     fld)
        d4.metric("Pass Rate",  f"{prt:.0f}%")

        st.subheader("Latest Run Results")
        st.dataframe(
            lt[["layer", "table_name", "check_name", "status",
                "failed_count", "row_count", "details"]]
            .sort_values("status")
            .reset_index(drop=True),
            use_container_width=True,
        )

        st.subheader("Failure Trend Over Time")
        tr = (
            dq.groupby([dq["created_at"].dt.date.rename("dt"), "status"])
            .size()
            .reset_index(name="n")
        )
        ftr = px.bar(tr, x="dt", y="n", color="status", barmode="stack",
                     color_discrete_map={"PASS": "#34d399", "FAIL": "#f87171"})
        ftr.update_layout(**PLY)
        st.plotly_chart(ftr, use_container_width=True)

        fa = (
            dq[dq["status"] == "FAIL"]
            .groupby("check_name").size()
            .reset_index(name="n")
            .sort_values("n", ascending=False)
            .head(10)
        )
        if not fa.empty:
            st.subheader("Most Frequently Failing Checks")
            ffa = px.bar(fa, x="n", y="check_name", orientation="h",
                         color_discrete_sequence=["#f87171"])
            ffa.update_layout(**PLY)
            st.plotly_chart(ffa, use_container_width=True)
        else:
            st.success("\u2705 No failing checks recorded.")
'''

pathlib.Path('c:/Users/Lenovo/Desktop/banking-transaction-lakehouse/dashboard/app.py').write_text(
    APP_CODE, encoding='utf-8'
)
print("Written OK, bytes:", len(APP_CODE))
