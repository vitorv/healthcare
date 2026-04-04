# Import python packages
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Basic Page Config
st.set_page_config(layout="wide", page_title="Healthcare SNF Analytics")
st.title("🏥 Skilled Nursing Facility (SNF) Analytics")
st.subheader("Gold Layer Insights: Staffing, Occupancy & Quality")

# Get the current credentials
session = get_active_session()

# ══════════════════════════════════════════════
# 1. Data Loading (Cached for Performance)
# ══════════════════════════════════════════════
@st.cache_data
def load_data():
    # Join Dimension and Fact tables
    query = """
        SELECT 
            d.provider_name,
            d.state_code,
            d.city,
            d.ownership_type,
            d.overall_rating,
            d.facility_size_category,
            f.occupancy_rate,
            f.reported_total_nurse_hours_prd,
            f.staffing_to_national_avg_ratio
        FROM HEALTHCARE.GOLD.DIM_PROVIDERS d
        INNER JOIN HEALTHCARE.GOLD.FCT_STAFFING_OCCUPANCY f ON d.ccn = f.ccn
    """
    return session.sql(query).to_pandas()

df = load_data()

# ══════════════════════════════════════════════
# 2. Sidebar Filters
# ══════════════════════════════════════════════
st.sidebar.header("Filter Dashboard")

selected_states = st.sidebar.multiselect(
    "Select States",
    options=sorted(df['STATE_CODE'].unique()),
    default=['CA', 'TX', 'NY', 'FL'] # Default to top 4 states
)

selected_ownership = st.sidebar.multiselect(
    "Ownership Type",
    options=sorted(df['OWNERSHIP_TYPE'].unique()),
    default=df['OWNERSHIP_TYPE'].unique()
)

# Apply Filters
filtered_df = df[
    (df['STATE_CODE'].isin(selected_states)) & 
    (df['OWNERSHIP_TYPE'].isin(selected_ownership))
].copy() # Added .copy() to avoid SettingWithCopyWarning

# FIX: Fill missing ratings with 0 so Plotly can handle the 'size' property
filtered_df['OVERALL_RATING'] = filtered_df['OVERALL_RATING'].fillna(0)

# ══════════════════════════════════════════════
# 3. KPI Highlights
# ══════════════════════════════════════════════
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Avg Quality Rating", f"{filtered_df['OVERALL_RATING'].mean():.1f} ⭐")
with col2:
    st.metric("Avg Occupancy Rate", f"{filtered_df['OCCUPANCY_RATE'].mean():.1%}")
with col3:
    st.metric("Total Facilities", f"{len(filtered_df):,}")
with col4:
    st.metric("National Staffing Ratio", f"{filtered_df['STAFFING_TO_NATIONAL_AVG_RATIO'].mean():.2f}x")

st.divider()

# ══════════════════════════════════════════════
# 4. Staffing Chart (Full Width)
# ══════════════════════════════════════════════
st.write("#### Staffing Levels vs. Occupancy Rates")
fig = px.scatter(
    filtered_df,
    x="OCCUPANCY_RATE",
    y="REPORTED_TOTAL_NURSE_HOURS_PRD",
    size="OVERALL_RATING",
    color="STATE_CODE",
    hover_name="PROVIDER_NAME",
    render_mode="svg",   # Kept SVG for your browser support
    labels={
        "OCCUPANCY_RATE": "Occupancy Rate (%)",
        "REPORTED_TOTAL_NURSE_HOURS_PRD": "Nurse Hours (per resident/day)"
    },
    template="plotly_dark",
    height=600 # Slightly taller
)
st.plotly_chart(fig, use_container_width=True)
st.divider()
# ══════════════════════════════════════════════
# 5. Understaffed Outliers (Now wide and easy to read)
# ══════════════════════════════════════════════
st.write("#### Understaffed Outliers (Bottom 15 Facilities vs. National Average)")
outliers = filtered_df.nsmallest(15, 'STAFFING_TO_NATIONAL_AVG_RATIO')[
    ['PROVIDER_NAME', 'CITY', 'STATE_CODE', 'STAFFING_TO_NATIONAL_AVG_RATIO']
]
# Using column_config to rename columns and format decimal places
st.dataframe(
    outliers, 
    hide_index=True, 
    use_container_width=True,
    column_config={
        "PROVIDER_NAME": "Facility Name",
        "CITY": "City",
        "STATE_CODE": "State",
        "STAFFING_TO_NATIONAL_AVG_RATIO": st.column_config.NumberColumn(
            "Staffing Ratio (to US Avg)",
            format="%.2fx"
        )
    }
)
st.divider()
# ══════════════════════════════════════════════
# 6. Geographic Performance (Leaderboard + Bar)
# ══════════════════════════════════════════════
st.write("#### 🏁 State Occupancy Leaderboard")

# Sort data for ranking
state_perf = filtered_df.groupby('STATE_CODE')['OCCUPANCY_RATE'].mean().reset_index()
state_perf = state_perf.sort_values('OCCUPANCY_RATE', ascending=False)

# Top 5 vs Bottom 5 in clean columns
top_col, bot_col = st.columns(2)

with top_col:
    st.write("📈 **Highest Occupancy States**")
    st.dataframe(
        state_perf.head(5), 
        hide_index=True, 
        use_container_width=True,
        column_config={"OCCUPANCY_RATE": st.column_config.NumberColumn(format="%.1%")}
    )

with bot_col:
    st.write("📉 **Lowest Occupancy States**")
    st.dataframe(
        state_perf.tail(5).sort_values('OCCUPANCY_RATE'), 
        hide_index=True, 
        use_container_width=True,
        column_config={"OCCUPANCY_RATE": st.column_config.NumberColumn(format="%.1%")}
    )

st.write("#### 📊 Occupancy Distribution by State")
# High-contrast bar chart (Replacing the Map)
fig_bar = px.bar(
    state_perf,
    x="STATE_CODE",
    y="OCCUPANCY_RATE",
    color="OCCUPANCY_RATE",
    color_continuous_scale="Viridis",
    text_auto='.1%',
    labels={'STATE_CODE': 'State Abbreviation', 'OCCUPANCY_RATE': 'Avg Occupancy (%)'},
    template="plotly_dark",
    height=400
)

fig_bar.update_layout(showlegend=False) # Legend is redundant on a bar chart
st.plotly_chart(fig_bar, use_container_width=True)
