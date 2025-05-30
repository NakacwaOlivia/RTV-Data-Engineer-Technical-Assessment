import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os

# Database connection
def get_db_conn():
    host = "postgres" if os.getenv("DOCKER_ENV") else "localhost"
    try:
        engine = create_engine(f"postgresql+psycopg2://postgres:password@{host}:5432/rtv")
        return engine
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Fetch data
@st.cache_data
def load_data(query):
    engine = get_db_conn()
    if engine is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Query failed: {e}")
        df = pd.DataFrame()
    return df

# Streamlit app
st.set_page_config(page_title="RTV Household Survey Dashboard", layout="wide")
st.title("RTV Household Survey Dashboard")
st.markdown("Analyze poverty, female empowerment, and agricultural productivity across survey years.")

# Sidebar filters
st.sidebar.header("Filters")
survey_years = load_data("SELECT survey_year FROM dim_time")
if not survey_years.empty:
    survey_years = survey_years['survey_year'].tolist()
    selected_years = st.sidebar.multiselect("Select Year", survey_years, default=survey_years)
else:
    st.sidebar.error("No survey years available. Please ensure the pipeline has run.")
    selected_years = []

districts = load_data("SELECT DISTINCT district FROM dim_household WHERE district IS NOT NULL")
if not districts.empty:
    districts = districts['district'].tolist()
    selected_districts = st.sidebar.multiselect("District", districts, default=districts)
else:
    selected_districts = []

# Initialize DataFrames
poverty_df = pd.DataFrame()
empowerment_df = pd.DataFrame()
crop_df = pd.DataFrame()

# Poverty Trends
st.header("Poverty Trends")
if selected_years and selected_districts:
    poverty_query = f"""
        SELECT f.survey_year, h.quartile, AVG(f.asp_actual_income) as avg_income
        FROM fact_survey f
        JOIN dim_household h ON f.hhid_2 = h.hhid_2
        WHERE f.survey_year IN ({','.join(map(str, selected_years))})
        AND h.district IN ({','.join([f"'{d}'" for d in selected_districts])})
        GROUP BY f.survey_year, h.quartile
        ORDER BY f.survey_year, h.quartile
    """
    poverty_df = load_data(poverty_query)
    if not poverty_df.empty:
        fig_poverty = px.bar(
            poverty_df,
            x="survey_year",
            y="avg_income",
            color="quartile",
            barmode="group",
            title="Average Household Income by Quartile",
            labels={"avg_income": "Average Income", "survey_year": "Year", "quartile": "Income Quartile"}
        )
        fig_poverty.update_traces(
            marker=dict(line=dict(color='black', width=1)),
            texttemplate='%{y:.2f}',
            textposition='auto'
        )
        fig_poverty.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            xaxis=dict(showgrid=False, tickmode='linear'),
            yaxis=dict(showgrid=True, gridcolor='lightgray')
        )
        st.plotly_chart(fig_poverty, use_container_width=True)
        st.dataframe(poverty_df, use_container_width=True)
    else:
        st.warning("No poverty data available for selected filters.")
else:
    st.warning("Please select years and districts.")

# Female Empowerment
st.header("Female Empowerment")
if selected_years and selected_districts:
    empowerment_query = f"""
        SELECT f.survey_year, h.district, 
               COUNT(*) as total_hh,
               SUM(CASE WHEN f.hhh_sex = 'Female' THEN 1 ELSE 0 END) as female_head_hh
        FROM fact_survey f
        JOIN dim_household h ON f.hhid_2 = h.hhid_2
        WHERE f.survey_year IN ({','.join(map(str, selected_years))})
        AND h.district IN ({','.join([f"'{d}'" for d in selected_districts])})
        GROUP BY f.survey_year, h.district
        ORDER BY f.survey_year, h.district
    """
    empowerment_df = load_data(empowerment_query)
    if not empowerment_df.empty:
        empowerment_df['female_head_pct'] = (empowerment_df['female_head_hh'] / empowerment_df['total_hh'] * 100)
        fig_empowerment = px.line(
            empowerment_df,
            x="survey_year",
            y="female_head_pct",
            color="district",
            title="Percentage of Female-Headed Households",
            labels={"female_head_pct": "% Female-Headed Households", "survey_year": "Year"}
        )
        fig_empowerment.update_traces(mode="lines+markers")
        fig_empowerment.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            xaxis=dict(showgrid=False, tickmode='linear'),
            yaxis=dict(showgrid=True, gridcolor='lightgray')
        )
        st.plotly_chart(fig_empowerment, use_container_width=True)
        st.dataframe(empowerment_df[['survey_year', 'district', 'female_head_pct']], use_container_width=True)
    else:
        st.warning("No empowerment data available for selected filters.")
else:
    st.warning("Please select years and districts.")

# Crop Yields
st.header("Agricultural Productivity")
if selected_years and selected_districts:
    crop_query = f"""
        SELECT c.survey_year, c.crop_type, h.district, AVG(c.total_yield) as avg_yield
        FROM fact_crop_yield c
        JOIN dim_household h ON c.hhid_2 = h.hhid_2
        WHERE c.survey_year IN ({','.join(map(str, selected_years))})
        AND h.district IN ({','.join([f"'{d}'" for d in selected_districts])})
        GROUP BY c.survey_year, c.crop_type, h.district
        ORDER BY c.survey_year, c.crop_type, h.district
    """
    crop_df = load_data(crop_query)
    if not crop_df.empty:
        selected_crop = st.selectbox("Select Crop", crop_df['crop_type'].unique())
        crop_filtered = crop_df[crop_df['crop_type'] == selected_crop]
        fig_crop = px.bar(
            crop_filtered,
            x="survey_year",
            y="avg_yield",
            color="district",
            barmode="group",
            title=f"Average Yield for {selected_crop}",
            labels={"avg_yield": "Average Yield (kg)", "survey_year": "Year"}
        )
        fig_crop.update_traces(
            marker=dict(line=dict(color='black', width=1)),
            texttemplate='%{y:.2f}',
            textposition='auto'
        )
        fig_crop.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            xaxis=dict(showgrid=False, tickmode='linear'),
            yaxis=dict(showgrid=True, gridcolor='lightgray')
        )
        st.plotly_chart(fig_crop, use_container_width=True)
        st.dataframe(crop_filtered, use_container_width=True)
    else:
        st.warning("No crop yield data available for selected filters.")
else:
    st.warning("Please select years and districts.")

# PPI Indicators
st.header("PPI Indicators")
if selected_years and selected_districts:
    ppi_query = f"""
        SELECT 
            f.survey_year,
            h.district,
            COUNT(*) as total_hh,
            SUM(CASE WHEN f.material_walls = 'Brick/Block' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_brick_walls,
            SUM(CASE WHEN f.fuel_source_cooking = 'Electricity' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_electric_cooking,
            SUM(CASE WHEN f.every_member_shoes THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_all_shoes
        FROM fact_survey f
        JOIN dim_household h ON f.hhid_2 = h.hhid_2
        WHERE f.survey_year IN ({','.join(map(str, selected_years))})
        AND h.district IN ({','.join([f"'{d}'" for d in selected_districts])})
        GROUP BY f.survey_year, h.district
        ORDER BY f.survey_year, h.district
    """
    ppi_df = load_data(ppi_query)
    if not ppi_df.empty:
        # Create tabs for different PPI indicators
        ppi_tab1, ppi_tab2, ppi_tab3 = st.tabs(["Housing Quality", "Cooking Fuel", "Basic Needs"])
        
        with ppi_tab1:
            fig_walls = px.line(
                ppi_df,
                x="survey_year",
                y="pct_brick_walls",
                color="district",
                title="Percentage of Households with Brick/Block Walls",
                labels={"pct_brick_walls": "% Brick/Block Walls", "survey_year": "Year"}
            )
            fig_walls.update_traces(mode="lines+markers")
            st.plotly_chart(fig_walls, use_container_width=True)
            
        with ppi_tab2:
            fig_fuel = px.line(
                ppi_df,
                x="survey_year",
                y="pct_electric_cooking",
                color="district",
                title="Percentage of Households Using Electricity for Cooking",
                labels={"pct_electric_cooking": "% Electric Cooking", "survey_year": "Year"}
            )
            fig_fuel.update_traces(mode="lines+markers")
            st.plotly_chart(fig_fuel, use_container_width=True)
            
        with ppi_tab3:
            fig_shoes = px.line(
                ppi_df,
                x="survey_year",
                y="pct_all_shoes",
                color="district",
                title="Percentage of Households Where All Members Have Shoes",
                labels={"pct_all_shoes": "% All Members Have Shoes", "survey_year": "Year"}
            )
            fig_shoes.update_traces(mode="lines+markers")
            st.plotly_chart(fig_shoes, use_container_width=True)
        
        st.dataframe(ppi_df, use_container_width=True)
    else:
        st.warning("No PPI indicator data available for selected filters.")
else:
    st.warning("Please select years and districts.")

# Education Levels
st.header("Education Levels")
if selected_years and selected_districts:
    education_query = f"""
        SELECT 
            f.survey_year,
            h.district,
            COUNT(*) as total_hh,
            SUM(CASE WHEN r.hhh_educ_level = 'Primary' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_hhh_primary,
            SUM(CASE WHEN r.hhh_educ_level = 'Secondary' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_hhh_secondary,
            SUM(CASE WHEN r.hhh_educ_level = 'Tertiary' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_hhh_tertiary,
            SUM(CASE WHEN r.spouse_educ_level = 'Primary' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_spouse_primary,
            SUM(CASE WHEN r.spouse_educ_level = 'Secondary' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_spouse_secondary,
            SUM(CASE WHEN r.spouse_educ_level = 'Tertiary' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_spouse_tertiary
        FROM fact_survey f
        JOIN dim_household h ON f.hhid_2 = h.hhid_2
        JOIN dim_respondent r ON f.hhid_2 = r.hhid_2 AND f.survey_year = r.survey_year
        WHERE f.survey_year IN ({','.join(map(str, selected_years))})
        AND h.district IN ({','.join([f"'{d}'" for d in selected_districts])})
        GROUP BY f.survey_year, h.district
        ORDER BY f.survey_year, h.district
    """
    education_df = load_data(education_query)
    if not education_df.empty:
        # Create tabs for different education metrics
        edu_tab1, edu_tab2 = st.tabs(["Household Head Education", "Spouse Education"])
        
        with edu_tab1:
            fig_hhh_edu = px.line(
                education_df,
                x="survey_year",
                y=["pct_hhh_primary", "pct_hhh_secondary", "pct_hhh_tertiary"],
                color="district",
                title="Household Head Education Levels",
                labels={
                    "value": "% of Households",
                    "variable": "Education Level",
                    "survey_year": "Year"
                }
            )
            fig_hhh_edu.update_traces(mode="lines+markers")
            st.plotly_chart(fig_hhh_edu, use_container_width=True)
            
        with edu_tab2:
            fig_spouse_edu = px.line(
                education_df,
                x="survey_year",
                y=["pct_spouse_primary", "pct_spouse_secondary", "pct_spouse_tertiary"],
                color="district",
                title="Spouse Education Levels",
                labels={
                    "value": "% of Households",
                    "variable": "Education Level",
                    "survey_year": "Year"
                }
            )
            fig_spouse_edu.update_traces(mode="lines+markers")
            st.plotly_chart(fig_spouse_edu, use_container_width=True)
        
        st.dataframe(education_df, use_container_width=True)
    else:
        st.warning("No education data available for selected filters.")
else:
    st.warning("Please select years and districts.")

# Enhanced Female Empowerment
st.header("Female Empowerment - Decision Making")
if selected_years and selected_districts:
    # Note: This assumes decision_1 through decision_5 columns exist in fact_survey
    # You may need to adjust the query based on actual column names
    empowerment_query = f"""
        SELECT 
            f.survey_year,
            h.district,
            COUNT(*) as total_hh,
            SUM(CASE WHEN f.decision_1 = 'Female' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_female_decision1,
            SUM(CASE WHEN f.decision_2 = 'Female' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_female_decision2,
            SUM(CASE WHEN f.decision_3 = 'Female' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_female_decision3,
            SUM(CASE WHEN f.decision_4 = 'Female' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_female_decision4,
            SUM(CASE WHEN f.decision_5 = 'Female' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_female_decision5
        FROM fact_survey f
        JOIN dim_household h ON f.hhid_2 = h.hhid_2
        WHERE f.survey_year IN ({','.join(map(str, selected_years))})
        AND h.district IN ({','.join([f"'{d}'" for d in selected_districts])})
        GROUP BY f.survey_year, h.district
        ORDER BY f.survey_year, h.district
    """
    try:
        decision_df = load_data(empowerment_query)
        if not decision_df.empty:
            fig_decision = px.line(
                decision_df,
                x="survey_year",
                y=["pct_female_decision1", "pct_female_decision2", "pct_female_decision3", 
                   "pct_female_decision4", "pct_female_decision5"],
                color="district",
                title="Female Decision-Making Roles Over Time",
                labels={
                    "value": "% Female Decision Making",
                    "variable": "Decision Type",
                    "survey_year": "Year"
                }
            )
            fig_decision.update_traces(mode="lines+markers")
            st.plotly_chart(fig_decision, use_container_width=True)
            st.dataframe(decision_df, use_container_width=True)
        else:
            st.warning("No decision-making data available for selected filters.")
    except Exception as e:
        st.warning("Decision-making data not available in the current schema. Please check if the decision columns exist.")
else:
    st.warning("Please select years and districts.")

# Download data
st.header("Export Data")
export_option = st.selectbox("Select Data to Export", ["Poverty Trends", "Female Empowerment", "Crop Yields"])
export_dfs = {
    "Poverty Trends": poverty_df,
    "Female Empowerment": empowerment_df,
    "Crop Yields": crop_df
}
if st.button("Download CSV"):
    export_df = export_dfs[export_option]
    if not export_df.empty:
        csv = export_df.to_csv(index=False)
        st.download_button(
            label="Download",
            data=csv,
            file_name=f"{export_option.lower().replace(' ', '_')}.csv",
            mime="text/csv"
        )
    else:
        st.error("No data to export.")