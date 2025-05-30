import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import os
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import altair as alt

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent))

# Database connection
def get_db_connection():
    return create_engine('postgresql://postgres:password@localhost:5432/airflow')

# Page configuration
st.set_page_config(
    page_title="RTV Household Progress Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select a page",
    ["Overview", "Household Progress", "Geographic Analysis", "Indicator Trends", "Data Quality"]
)

# Common functions
def load_household_data():
    engine = get_db_connection()
    query = """
    SELECT * FROM household_progress
    WHERE survey_date >= CURRENT_DATE - INTERVAL '2 years'
    """
    return pd.read_sql(query, engine)

def get_date_range():
    return st.sidebar.date_input(
        "Select Date Range",
        value=(datetime.now() - timedelta(days=365), datetime.now()),
        max_value=datetime.now()
    )

# Overview Page
if page == "Overview":
    st.title("RTV Household Progress Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Households",
            value="1,234",
            delta="12%"
        )
    
    with col2:
        st.metric(
            label="Active Villages",
            value="45",
            delta="5%"
        )
    
    with col3:
        st.metric(
            label="Average Progress",
            value="68%",
            delta="8%"
        )
    
    with col4:
        st.metric(
            label="Data Quality Score",
            value="95%",
            delta="2%"
        )
    
    # Progress Overview Chart
    st.subheader("Overall Progress by Category")
    df = load_household_data()
    
    fig = px.box(
        df,
        x="indicator_category",
        y="measurement_value",
        color="survey_round",
        title="Progress Distribution by Category and Survey Round"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Recent Activity
    st.subheader("Recent Activity")
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("Latest Surveys")
        # Add survey activity table
        
    with col2:
        st.write("Data Quality Alerts")
        # Add quality alerts

# Household Progress Page
elif page == "Household Progress":
    st.title("Household Progress Analysis")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_village = st.selectbox(
            "Select Village",
            options=["All"] + sorted(df["village_name"].unique().tolist())
        )
    
    with col2:
        selected_indicator = st.selectbox(
            "Select Indicator",
            options=["All"] + sorted(df["indicator_name"].unique().tolist())
        )
    
    with col3:
        date_range = get_date_range()
    
    # Household Progress Chart
    st.subheader("Household Progress Over Time")
    
    filtered_df = df.copy()
    if selected_village != "All":
        filtered_df = filtered_df[filtered_df["village_name"] == selected_village]
    if selected_indicator != "All":
        filtered_df = filtered_df[filtered_df["indicator_name"] == selected_indicator]
    
    fig = px.line(
        filtered_df,
        x="round_start_date",
        y="measurement_value",
        color="household_code",
        title="Household Progress Timeline"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Household Details
    st.subheader("Household Details")
    st.dataframe(
        filtered_df.pivot_table(
            index=["household_code", "village_name"],
            columns="survey_round",
            values="measurement_value"
        ).reset_index()
    )

# Geographic Analysis Page
elif page == "Geographic Analysis":
    st.title("Geographic Analysis")
    
    # Map View
    st.subheader("Progress by Location")
    
    # Create a map using Plotly
    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        color="measurement_value",
        size="household_size",
        hover_name="village_name",
        zoom=6,
        mapbox_style="carto-positron"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Regional Analysis
    st.subheader("Regional Progress Comparison")
    
    fig = px.bar(
        df.groupby(["region", "indicator_category"])["measurement_value"].mean().reset_index(),
        x="region",
        y="measurement_value",
        color="indicator_category",
        title="Average Progress by Region and Category"
    )
    st.plotly_chart(fig, use_container_width=True)

# Indicator Trends Page
elif page == "Indicator Trends":
    st.title("Indicator Trends Analysis")
    
    # Indicator Selection
    selected_indicators = st.multiselect(
        "Select Indicators to Compare",
        options=sorted(df["indicator_name"].unique().tolist()),
        default=sorted(df["indicator_name"].unique().tolist())[:3]
    )
    
    # Trend Analysis
    st.subheader("Indicator Trends Over Time")
    
    fig = px.line(
        df[df["indicator_name"].isin(selected_indicators)],
        x="round_start_date",
        y="measurement_value",
        color="indicator_name",
        title="Indicator Trends"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Correlation Analysis
    st.subheader("Indicator Correlations")
    
    pivot_df = df.pivot_table(
        index="household_id",
        columns="indicator_name",
        values="measurement_value"
    ).corr()
    
    fig = px.imshow(
        pivot_df,
        title="Indicator Correlation Matrix"
    )
    st.plotly_chart(fig, use_container_width=True)

# Data Quality Page
elif page == "Data Quality":
    st.title("Data Quality Dashboard")
    
    # Quality Metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Completeness Score",
            value="98%",
            delta="1%"
        )
    
    with col2:
        st.metric(
            label="Consistency Score",
            value="96%",
            delta="2%"
        )
    
    with col3:
        st.metric(
            label="Timeliness Score",
            value="99%",
            delta="0%"
        )
    
    # Quality Issues
    st.subheader("Recent Quality Issues")
    
    # Add quality issues table
    quality_issues = pd.DataFrame({
        "Issue Type": ["Missing Data", "Inconsistent Values", "Outliers"],
        "Count": [5, 3, 8],
        "Severity": ["Low", "Medium", "High"],
        "Status": ["Resolved", "In Progress", "Open"]
    })
    
    st.dataframe(quality_issues)
    
    # Data Quality Trends
    st.subheader("Quality Metrics Over Time")
    
    # Add quality trends chart
    fig = px.line(
        pd.DataFrame({
            "Date": pd.date_range(start="2024-01-01", periods=30),
            "Completeness": [95 + i * 0.1 for i in range(30)],
            "Consistency": [94 + i * 0.05 for i in range(30)],
            "Timeliness": [98 + i * 0.02 for i in range(30)]
        }),
        x="Date",
        y=["Completeness", "Consistency", "Timeliness"],
        title="Data Quality Trends"
    )
    st.plotly_chart(fig, use_container_width=True)

# Footer
st.sidebar.markdown("---")
st.sidebar.info(
    "Dashboard last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
)