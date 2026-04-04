# Import python packages
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Basic Page Config
st.set_page_config(layout="wide", page_title="Healthcare Medallion Analytics")
st.title("🏥 SNF Medallion Dashboard: 100% Complete Visualization")

# Get the current credentials
session = get_active_session()

# ══════════════════════════════════════════════
# 1. Data Loading (Fast & Cached)
# ══════════════════════════════════════════════
@st.cache_data
def load_gold_data():
    # Load all 3 Fact tables Joined to Dimension attributes
    queries = {
        "staffing": """
            SELECT 
                d.provider_name, d.state_code, d.city, d.ownership_type, d.overall_rating,
                f.occupancy_rate, f.reported_total_nurse_hours_prd, f.staffing_to_national_avg_ratio
            FROM HEALTHCARE.GOLD.DIM_PROVIDERS d
            INNER JOIN HEALTHCARE.GOLD.FCT_STAFFING_OCCUPANCY f ON d.ccn = f.ccn
        """,
        "quality": """
            SELECT 
                d.provider_name, d.state_code, d.city, d.ownership_type, d.overall_rating,
                f.performance_readmission_rate, f.reported_total_nurse_hours_prd, f.readmission_to_national_avg_ratio
            FROM HEALTHCARE.GOLD.DIM_PROVIDERS d
            INNER JOIN HEALTHCARE.GOLD.FCT_QUALITY_READMISSION f ON d.ccn = f.ccn
        """,
        "turnover": """
            SELECT 
                d.provider_name, d.state_code, d.city, d.ownership_type, d.overall_rating,
                f.total_nurse_turnover_pct, f.rn_turnover_pct, f.reported_total_nurse_hours_prd
            FROM HEALTHCARE.GOLD.DIM_PROVIDERS d
            INNER JOIN HEALTHCARE.GOLD.FCT_TURNOVER_ANALYSIS f ON d.ccn = f.ccn
        """
    }
    
    return {k: session.sql(v).to_pandas().fillna(0) for k, v in queries.items()}

# Initial Load
data_dict = load_gold_data()

# ══════════════════════════════════════════════
# 2. Global Sidebar Filters
# ══════════════════════════════════════════════
st.sidebar.header("🗺️ Global Filters")

all_states = sorted(data_dict['staffing']['STATE_CODE'].unique())
selected_states = st.sidebar.multiselect("Select States", all_states, default=all_states[:5])

all_ownership = sorted(data_dict['staffing']['OWNERSHIP_TYPE'].unique())
selected_ownership = st.sidebar.multiselect("Ownership Type", all_ownership, default=all_ownership)

# Apply filters across all dataframes
def filter_df(df):
    return df[(df['STATE_CODE'].isin(selected_states)) & (df['OWNERSHIP_TYPE'].isin(selected_ownership))].copy()

df_s = filter_df(data_dict['staffing'])
df_q = filter_df(data_dict['quality'])
df_t = filter_df(data_dict['turnover'])

# ══════════════════════════════════════════════
# 3. Multi-Tab Navigation
# ══════════════════════════════════════════════
tab_staff, tab_quality, tab_turnover = st.tabs([
    "📊 Staffing & Occupancy", "🏥 Quality & Readmission", "🔄 Turnover & Burnout"
])

# ══════════════════════════════════════════════
# TAB 1: STAFFING & OCCUPANCY (Metric 1.1, 2.3, 2.5)
# ══════════════════════════════════════════════
with tab_staff:
    st.header("Facility Staffing vs Capacity Analysis")
    
    col1, col2, col3 = st.columns(3)
    with col1: st.metric("Avg Occupancy", f"{df_s['OCCUPANCY_RATE'].mean():.1%}")
    with col2: st.metric("Avg Nurse Hours", f"{df_s['REPORTED_TOTAL_NURSE_HOURS_PRD'].mean():.2f}")
    with col3: st.metric("Staffing Index (to US)", f"{df_s['STAFFING_TO_NATIONAL_AVG_RATIO'].mean():.2f}x")

    # Visualization
    fig_s = px.scatter(
        df_s, x="OCCUPANCY_RATE", y="REPORTED_TOTAL_NURSE_HOURS_PRD",
        size="OVERALL_RATING", color="OWNERSHIP_TYPE", hover_name="PROVIDER_NAME",
        render_mode="svg", template="plotly_dark", height=500,
        labels={"OCCUPANCY_RATE": "Occupancy Rate (%)", "REPORTED_TOTAL_NURSE_HOURS_PRD": "Nurse Hours/Resident/Day"}
    )
    st.plotly_chart(fig_s, use_container_width=True)

    st.write("#### 🏁 Understaffed Outliers")
    st.dataframe(df_s.nsmallest(10, 'STAFFING_TO_NATIONAL_AVG_RATIO')[['PROVIDER_NAME', 'STATE_CODE', 'STAFFING_TO_NATIONAL_AVG_RATIO']], hide_index=True, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 2: QUALITY & READMISSION (Metric 3.3, 3.5)
# ══════════════════════════════════════════════
with tab_quality:
    st.header("Care Quality: Readmission Impact")
    
    ql1, ql2 = st.columns(2)
    with ql1: st.metric("Avg Readmission Rate", f"{df_q['PERFORMANCE_READMISSION_RATE'].mean():.2%}")
    with ql2: st.metric("Performance vs. US Average", f"{df_q['READMISSION_TO_NATIONAL_AVG_RATIO'].mean():.2f}x")

    st.write("#### Correlation: Nurse Staffing vs. Readmission Performance")
    # Higher hours should correlate with lower readmission (Metric 3.5)
    fig_q = px.scatter(
        df_q, x="REPORTED_TOTAL_NURSE_HOURS_PRD", y="PERFORMANCE_READMISSION_RATE",
        color="STATE_CODE", hover_name="PROVIDER_NAME", trendline="ols", # Shows the trend!
        render_mode="svg", template="plotly_dark", height=500,
        labels={"REPORTED_TOTAL_NURSE_HOURS_PRD": "Total Nurse Hours", "PERFORMANCE_READMISSION_RATE": "30-Day Readmission Rate"}
    )
    st.plotly_chart(fig_q, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 3: TURNOVER & BURNOUT (Metric v1-Q2)
# ══════════════════════════════════════════════
with tab_turnover:
    st.header("Nurse Retention & Workforce Vitality")
    
    tl1, tl2 = st.columns(2)
    with tl1: st.metric("Total Team Turnover", f"{df_t['TOTAL_NURSE_TURNOVER_PCT'].mean():.1%}")
    with tl2: st.metric("RN Specific Turnover", f"{df_t['RN_TURNOVER_PCT'].mean():.1%}")

    st.write("#### Burnout Analysis: Retention vs. Daily Workload")
    # Higher workload (low staffing hours) often correlates with higher turnover
    fig_t = px.scatter(
        df_t, x="REPORTED_TOTAL_NURSE_HOURS_PRD", y="TOTAL_NURSE_TURNOVER_PCT",
        color="STATE_CODE", symbol="OWNERSHIP_TYPE", hover_name="PROVIDER_NAME",
        render_mode="svg", template="plotly_dark", height=500,
        labels={"REPORTED_TOTAL_NURSE_HOURS_PRD": "Total Nurse Hours", "TOTAL_NURSE_TURNOVER_PCT": "Retention (% Left Facility)"}
    )
    st.plotly_chart(fig_t, use_container_width=True)

    # State leaderboard for highest turnover
    st.write("#### 📉 Ranking: States with Highest Nurse Burnout (Avg Turnover %)")
    burnout_stats = df_t.groupby('STATE_CODE')['TOTAL_NURSE_TURNOVER_PCT'].mean().reset_index().sort_values('TOTAL_NURSE_TURNOVER_PCT', ascending=False)
    st.bar_chart(burnout_stats, x="STATE_CODE", y="TOTAL_NURSE_TURNOVER_PCT", use_container_width=True)

