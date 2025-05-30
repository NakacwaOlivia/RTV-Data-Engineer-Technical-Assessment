import pandas as pd
from sqlalchemy import create_engine
import os

# Database connection
def get_db_conn():
    host = "postgres" if os.getenv("DOCKER_ENV") else "localhost"
    return create_engine(f"postgresql+psycopg2://postgres:password@{host}:5432/rtv")

# Fetch data
def load_data(query):
    engine = get_db_conn()
    return pd.read_sql(query, engine)

# Generate report
def generate_report():
    # Poverty trends by district
    poverty_query = """
        SELECT h.district, f.survey_year, AVG(f.asp_actual_income) as avg_income
        FROM fact_survey f
        JOIN dim_household h ON f.hhid_2 = h.hhid_2
        GROUP BY h.district, f.survey_year
        ORDER BY h.district, f.survey_year
    """
    poverty_df = load_data(poverty_query)

    # Female empowerment summary
    empowerment_query = """
        SELECT survey_year, COUNT(*) as total_hh, 
               SUM(CASE WHEN hhh_sex = 'Female' THEN 1 ELSE 0 END) as female_head_hh
        FROM fact_survey
        GROUP BY survey_year
        ORDER BY survey_year
    """
    empowerment_df = load_data(empowerment_query)
    empowerment_df['female_head_pct'] = (empowerment_df['female_head_hh'] / empowerment_df['total_hh'] * 100)

    # Crop yield averages
    crop_query = """
        SELECT crop_type, AVG(total_yield) as avg_yield
        FROM fact_crop_yield
        GROUP BY crop_type
        ORDER BY avg_yield DESC
    """
    crop_df = load_data(crop_query)

    # Compile report
    report = f"""
RTV Household Survey Report
==========================

Poverty Trends by District
-------------------------
{poverty_df.to_string(index=False)}

Female Empowerment Summary
-------------------------
{empowerment_df.to_string(index=False)}

Crop Yield Averages
-------------------
{crop_df.to_string(index=False)}
"""
    with open("report.txt", "w") as f:
        f.write(report)
    print("Report generated as 'report.txt'")

if __name__ == "__main__":
    generate_report()