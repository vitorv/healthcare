# Import python packages
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Basic Page Config
st.set_page_config(layout="wide", page_title="Healthcare Medallion Analytics")
st.title("Healthcare Visualization Dashboard")

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

    # ══════════════════════════════════════════════
    # UNDERSTAFFED OUTLIERS (REPORTERS)
    # ══════════════════════════════════════════════
    st.write("#### 🏁 Understaffed Outliers (Lowest Reported Staffing)")
    reporters_only = df_s[df_s['STAFFING_TO_NATIONAL_AVG_RATIO'] > 0]
    outliers_table = reporters_only.nsmallest(10, 'STAFFING_TO_NATIONAL_AVG_RATIO')[
        ['PROVIDER_NAME', 'STATE_CODE', 'STAFFING_TO_NATIONAL_AVG_RATIO']
    ]
    st.dataframe(outliers_table, hide_index=True, use_container_width=True,
        column_config={"STAFFING_TO_NATIONAL_AVG_RATIO": st.column_config.NumberColumn("Staffing Index", format="%.2fx")})
    st.divider()
    # ══════════════════════════════════════════════
    # NON-REPORTING FACILITIES (0s)
    # ══════════════════════════════════════════════
    st.write("#### ⚠️ High Risk: Non-Reporting Facilities (Missing Data)")
    non_reporters = df_s[df_s['STAFFING_TO_NATIONAL_AVG_RATIO'] == 0][
        ['PROVIDER_NAME', 'CITY', 'STATE_CODE', 'OWNERSHIP_TYPE']
    ].head(15) # Limiting to 15 to keep it clean
    if not non_reporters.empty:
        st.info("The facilities below have failed to report staffing data. This is often flagged as a major compliance risk by CMS.")
        st.dataframe(non_reporters, hide_index=True, use_container_width=True)
    else:
        st.success("Great news! All selected facilities have reported their staffing data.")
    
# ══════════════════════════════════════════════
# TAB 2: QUALITY & READMISSION (Metric 3.3, 3.5)
# ══════════════════════════════════════════════
with tab_quality:
    st.header("Care Quality: Readmission Performance Analysis")
    
    ql1, ql2, ql3 = st.columns(3)
    with ql1: st.metric("Avg Readmission Rate", f"{df_q['PERFORMANCE_READMISSION_RATE'].mean():.2%}")
    with ql2: st.metric("Performance vs. US Avg", f"{df_q['READMISSION_TO_NATIONAL_AVG_RATIO'].mean():.2f}x")
    with ql3: st.metric("Top Performers", f"{len(df_q[df_q['OVERALL_RATING'] >= 4])} (4-5 Stars)")

    # ══════════════════════════════════════════════
    # Correlation Chart (Staffing vs Quality)
    # ══════════════════════════════════════════════
    st.write("#### 🏥 Visual Correlation: Does Staffing Improve Readmissions?")
    fig_q = px.scatter(
        df_q, x="REPORTED_TOTAL_NURSE_HOURS_PRD", y="PERFORMANCE_READMISSION_RATE",
        color="STATE_CODE", hover_name="PROVIDER_NAME", trendline="ols",
        render_mode="svg", template="plotly_dark", height=450,
        labels={"REPORTED_TOTAL_NURSE_HOURS_PRD": "Nurse Hours/Day", "PERFORMANCE_READMISSION_RATE": "30-Day Readmission Rate"}
    )
    st.plotly_chart(fig_q, use_container_width=True)

    st.divider()

    # ══════════════════════════════════════════════
    # State & Facility Breakdowns
    # ══════════════════════════════════════════════
    col_state, col_fac = st.columns(2)

    with col_state:
        st.write("#### 📊 Readmission Rates by State")
        state_q = df_q.groupby('STATE_CODE')['PERFORMANCE_READMISSION_RATE'].mean().reset_index().sort_values('PERFORMANCE_READMISSION_RATE')
        
        # Color by Readmission Rate (Lower is Better/Greener)
        fig_bar_q = px.bar(state_q, x="STATE_CODE", y="PERFORMANCE_READMISSION_RATE",
                           color="PERFORMANCE_READMISSION_RATE", color_continuous_scale="Viridis", # Reversed scale
                           labels={"PERFORMANCE_READMISSION_RATE": "Avg Readmission Rate (%)"},
                           text_auto='.1%', template="plotly_dark")
        st.plotly_chart(fig_bar_q, use_container_width=True)

    with col_fac:
        st.write("#### 🏆 Top 10 Facilities (Lowest Readmissions)")
        # Note: Lower is better for readmissions
        top_10_readmit = df_q.nsmallest(10, 'PERFORMANCE_READMISSION_RATE')[
            ['PROVIDER_NAME', 'STATE_CODE', 'PERFORMANCE_READMISSION_RATE']
        ]
        st.dataframe(top_10_readmit, hide_index=True, use_container_width=True,
                     column_config={"PERFORMANCE_READMISSION_RATE": st.column_config.NumberColumn("Readmit Rate", format="%.2%")})

    # High-Risk Outlier List
    st.write("#### 🚨 Warning: Highest Readmission Risk Outliers")
    outlier_df = df_q.nlargest(10, 'PERFORMANCE_READMISSION_RATE')[['PROVIDER_NAME', 'CITY', 'STATE_CODE', 'PERFORMANCE_READMISSION_RATE']]
    st.dataframe(outlier_df, hide_index=True, use_container_width=True,
                 column_config={"PERFORMANCE_READMISSION_RATE": st.column_config.NumberColumn("Readmit Rate", format="%.2%")})


# ══════════════════════════════════════════════
# TAB 3: TURNOVER & BURNOUT (Metric Q2)
# ══════════════════════════════════════════════
with tab_turnover:
    st.header("Nurse Turnover & Workforce Burnout")
    
    # Filter out the 0s (missing data) for accurate averages
    df_t_clean = df_t[(df_t['TOTAL_NURSE_TURNOVER_PCT'] > 0) & (df_t['REPORTED_TOTAL_NURSE_HOURS_PRD'] > 0)]
    
    tl1, tl2 = st.columns(2)
    # Replaced :.1% with :.1f% because the data is already on a 0-100 scale
    with tl1: st.metric("Avg Total Team Turnover", f"{df_t_clean['TOTAL_NURSE_TURNOVER_PCT'].mean():.1f}%")
    with tl2: st.metric("Avg RN Specific Turnover", f"{df_t_clean['RN_TURNOVER_PCT'].mean():.1f}%")

    st.write("#### Burnout Analysis: Turnover vs. Daily Workload")
    # Cleaned legend, added trendline, grouped by STATE
    fig_t = px.scatter(
        df_t_clean, x="REPORTED_TOTAL_NURSE_HOURS_PRD", y="TOTAL_NURSE_TURNOVER_PCT",
        color="STATE_CODE", hover_name="PROVIDER_NAME", 
        trendline="ols", # Shows if low hours = high turnover
        render_mode="svg", template="plotly_dark", height=500,
        labels={
            "REPORTED_TOTAL_NURSE_HOURS_PRD": "Nurse Hours (per resident/day)", 
            "TOTAL_NURSE_TURNOVER_PCT": "Turnover Rate (%)",
            "STATE_CODE": "State"
        }
    )
    st.plotly_chart(fig_t, use_container_width=True)


    # Replaced simple bar chart with a colored Plotly bar chart
    st.write("#### 📉 Ranking: States with Highest Nurse Burnout")
    burnout_stats = df_t_clean.groupby('STATE_CODE')['TOTAL_NURSE_TURNOVER_PCT'].mean().reset_index().sort_values('TOTAL_NURSE_TURNOVER_PCT', ascending=False)
    
    fig_bar_t = px.bar(
        burnout_stats, x="STATE_CODE", y="TOTAL_NURSE_TURNOVER_PCT", 
        text_auto='.1f', color="TOTAL_NURSE_TURNOVER_PCT", color_continuous_scale="Reds",
        labels={"STATE_CODE": "State", "TOTAL_NURSE_TURNOVER_PCT": "Avg Turnover (%)"},
        template="plotly_dark", height=400
    )
    fig_bar_t.update_layout(showlegend=False)
    st.plotly_chart(fig_bar_t, use_container_width=True)
