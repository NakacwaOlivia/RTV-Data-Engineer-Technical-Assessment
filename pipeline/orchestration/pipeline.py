import pandas as pd
from minio import Minio
import psycopg2
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# MinIO client
minio_client = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

# PostgreSQL connection
def get_db_conn():
    return psycopg2.connect(dbname="rtv", user="postgres", password="pass", host="postgres", port="5432")

# Core columns
CORE_COLUMNS = [
    "hhid_2", "hhid_2_again", "survey_year", "SubmissionDate", "duration", "district",
    "pre_district", "pre_subcounty", "pre_parish", "pre_cluster", "pre_village",
    "Quartile", "pre_vid", "survey_type", "status", "respondent_sex", "hhh_sex",
    "hhh_age", "hhh_educ_level", "spouse_sex", "spouse_age", "spouse_educ_level",
    "no_wives", "tot_hhmembers", "hh_size", "females_hh_count", "children_num_u5",
    "Material_walls", "Material_roof", "Fuel_source_cooking",
    "Every_Member_at_least_ONE_Pair_of_Shoes", "asp_actual_income", "cereals_week",
    "tubers_week", "medical_care_annual"
]

# Dynamic crop columns
def get_crop_columns(file_path):
    try:
        df = pd.read_csv(file_path, nrows=1)
        return [col for col in df.columns if col.startswith("sn_1_") and any(s in col.lower() for s in ["planted", "total_yield", "yield_sold", "yield_consumed", "market_price"])]
    except:
        return []

# Ingest CSVs
def ingest_data():
    data_dir = "/opt/airflow/data"
    csv_files = ["01_baseline.csv", "02_year_one.csv", "03_year_two.csv"]
    
    for csv_file in csv_files:
        file_path = os.path.join(data_dir, csv_file)
        if not os.path.exists(file_path):
            print(f"File {csv_file} not found")
            continue
        
        # Get available columns
        crop_cols = get_crop_columns(file_path)
        cols_to_read = [col for col in CORE_COLUMNS + crop_cols if col in pd.read_csv(file_path, nrows=1).columns]
        df = pd.read_csv(file_path, usecols=cols_to_read)
        
        # Validate hhid_2
        if "hhid_2" in df.columns and "hhid_2_again" in df.columns:
            if not (df["hhid_2"] == df["hhid_2_again"]).all():
                raise ValueError(f"hhid_2 mismatch in {csv_file}")
            df.drop(columns=["hhid_2_again"], inplace=True)
        
        # Add survey_year
        if "survey_year" not in df.columns:
            year_map = {"01_baseline.csv": 2020, "02_year_one.csv": 2021, "03_year_two.csv": 2023}
            df["survey_year"] = year_map[csv_file]
        
        # Clean data
        df.replace([-98, -99], pd.NA, inplace=True)
        df.drop(columns=["index", "Unnamed: 0"], errors="ignore", inplace=True)
        
        # Save to MinIO
        parquet_file = csv_file.replace(".csv", ".parquet")
        temp_path = f"/tmp/{parquet_file}"
        df.to_parquet(temp_path)
        minio_client.fput_object("data-lake", f"raw/{parquet_file}", temp_path)
        os.remove(temp_path)

# Load to staging
def load_to_staging():
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # Create staging table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging_survey (
            hhid_2 VARCHAR(50),
            survey_year INT,
            submission_date TIMESTAMP,
            duration INT,
            district VARCHAR(50),
            pre_district VARCHAR(50),
            pre_subcounty VARCHAR(50),
            pre_parish VARCHAR(50),
            pre_cluster VARCHAR(50),
            pre_village VARCHAR(50),
            quartile VARCHAR(20),
            pre_vid INT,
            survey_type INT,
            status VARCHAR(20),
            respondent_sex VARCHAR(10),
            hhh_sex VARCHAR(10),
            hhh_age INT,
            hhh_educ_level VARCHAR(50),
            spouse_sex VARCHAR(10),
            spouse_age INT,
            spouse_educ_level VARCHAR(50),
            no_wives INT,
            tot_hhmembers INT,
            hh_size INT,
            females_hh_count INT,
            children_num_u5 INT,
            material_walls VARCHAR(50),
            material_roof VARCHAR(50),
            fuel_source_cooking VARCHAR(50),
            every_member_shoes BOOLEAN,
            asp_actual_income DECIMAL,
            cereals_week DECIMAL,
            tubers_week DECIMAL,
            medical_care_annual DECIMAL,
            _source_file VARCHAR(100)
        );
    """)
    
    # Load Parquet files
    parquet_files = ["01_baseline.parquet", "02_year_one.parquet", "03_year_two.parquet"]
    for file in parquet_files:
        try:
            minio_client.stat_object("data-lake", f"raw/{file}")
            with minio_client.get_object("data-lake", f"raw/{file}") as obj:
                df = pd.read_parquet(obj)
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO staging_survey (
                            hhid_2, survey_year, submission_date, duration, district, pre_district,
                            pre_subcounty, pre_parish, pre_cluster, pre_village, quartile, pre_vid,
                            survey_type, status, respondent_sex, hhh_sex, hhh_age, hhh_educ_level,
                            spouse_sex, spouse_age, spouse_educ_level, no_wives, tot_hhmembers,
                            hh_size, females_hh_count, children_num_u5, material_walls, material_roof,
                            fuel_source_cooking, every_member_shoes, asp_actual_income, cereals_week,
                            tubers_week, medical_care_annual, _source_file
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row.get("hhid_2"), row.get("survey_year"), row.get("SubmissionDate"),
                        row.get("duration"), row.get("district"), row.get("pre_district"),
                        row.get("pre_subcounty"), row.get("pre_parish"), row.get("pre_cluster"),
                        row.get("pre_village"), row.get("Quartile"), row.get("pre_vid"),
                        row.get("survey_type"), row.get("status"), row.get("respondent_sex"),
                        row.get("hhh_sex"), row.get("hhh_age"), row.get("hhh_educ_level"),
                        row.get("spouse_sex"), row.get("spouse_age"), row.get("spouse_educ_level"),
                        row.get("no_wives"), row.get("tot_hhmembers"), row.get("hh_size"),
                        row.get("females_hh_count"), row.get("children_num_u5"),
                        row.get("Material_walls"), row.get("Material_roof"),
                        row.get("Fuel_source_cooking"), row.get("Every_Member_at_least_ONE_Pair_of_Shoes"),
                        row.get("asp_actual_income"), row.get("cereals_week"),
                        row.get("tubers_week"), row.get("medical_care_annual"), file
                    ))
        except Exception as e:
            print(f"Error loading {file}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()

# Transform to star schema
def transform_data():
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_household (
            hhid_2 VARCHAR(50) PRIMARY KEY,
            district VARCHAR(50),
            subcounty VARCHAR(50),
            parish VARCHAR(50),
            cluster VARCHAR(50),
            village VARCHAR(50),
            vid INT,
            quartile VARCHAR(20),
            hh_category VARCHAR(50),
            treat_status VARCHAR(50)
        );
        CREATE TABLE IF NOT EXISTS dim_time (
            survey_year INT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS dim_respondent (
            respondent_id SERIAL PRIMARY KEY,
            hhid_2 VARCHAR(50),
            survey_year INT,
            respondent_sex VARCHAR(10),
            hhh_sex VARCHAR(10),
            hhh_age INT,
            hhh_educ_level VARCHAR(50),
            spouse_sex VARCHAR(10),
            spouse_age INT,
            spouse_educ_level VARCHAR(50),
            no_wives INT,
            FOREIGN KEY (hhid_2) REFERENCES dim_household(hhid_2),
            FOREIGN KEY (survey_year) REFERENCES dim_time(survey_year)
        );
        CREATE TABLE IF NOT EXISTS fact_survey (
            hhid_2 VARCHAR(50),
            survey_year INT,
            submission_date TIMESTAMP,
            duration INT,
            survey_type INT,
            status VARCHAR(20),
            tot_hhmembers INT,
            hh_size INT,
            females_hh_count INT,
            children_num_u5 INT,
            asp_actual_income DECIMAL,
            material_walls VARCHAR(50),
            material_roof VARCHAR(50),
            fuel_source_cooking VARCHAR(50),
            every_member_shoes BOOLEAN,
            PRIMARY KEY (hhid_2, survey_year),
            FOREIGN KEY (hhid_2) REFERENCES dim_household(hhid_2),
            FOREIGN KEY (survey_year) REFERENCES dim_time(survey_year)
        ) PARTITION BY LIST (survey_year);
        CREATE TABLE fact_survey_2020 PARTITION OF fact_survey FOR VALUES IN (2020);
        CREATE TABLE fact_survey_2021 PARTITION OF fact_survey FOR VALUES IN (2021);
        CREATE TABLE fact_survey_2022 PARTITION OF fact_survey FOR VALUES IN (2022);
        CREATE TABLE fact_survey_2023 PARTITION OF fact_survey FOR VALUES IN (2023);
        CREATE TABLE IF NOT EXISTS fact_expenditure (
            hhid_2 VARCHAR(50),
            survey_year INT,
            cereals_week DECIMAL,
            tubers_week DECIMAL,
            medical_care_annual DECIMAL,
            total_expenditure DECIMAL,
            PRIMARY KEY (hhid_2, survey_year),
            FOREIGN KEY (hhid_2) REFERENCES dim_household(hhid_2),
            FOREIGN KEY (survey_year) REFERENCES dim_time(survey_year)
        );
        CREATE TABLE IF NOT EXISTS fact_crop_yield (
            hhid_2 VARCHAR(50),
            survey_year INT,
            crop_type VARCHAR(50),
            planted_qty DECIMAL,
            total_yield DECIMAL,
            yield_sold DECIMAL,
            yield_consumed DECIMAL,
            market_price DECIMAL,
            PRIMARY KEY (hhid_2, survey_year, crop_type),
            FOREIGN KEY (hhid_2) REFERENCES dim_household(hhid_2),
            FOREIGN KEY (survey_year) REFERENCES dim_time(survey_year)
        );
    """)
    
    # Populate dim_time
    cursor.execute("""
        INSERT INTO dim_time (survey_year)
        SELECT DISTINCT survey_year FROM staging_survey WHERE survey_year IS NOT NULL
        ON CONFLICT (survey_year) DO NOTHING;
    """)
    
    # Populate dim_household
    cursor.execute("""
        INSERT INTO dim_household (hhid_2, district, subcounty, parish, cluster, village, vid, quartile, hh_category, treat_status)
        SELECT DISTINCT hhid_2, pre_district, pre_subcounty, pre_parish, pre_cluster, pre_village, pre_vid, quartile, hh_category, treat_status
        FROM staging_survey
        WHERE hhid_2 IS NOT NULL
        ON CONFLICT (hhid_2) DO NOTHING;
    """)
    
    # Populate dim_respondent
    cursor.execute("""
        INSERT INTO dim_respondent (hhid_2, survey_year, respondent_sex, hhh_sex, hhh_age, hhh_educ_level,
                                   spouse_sex, spouse_age, spouse_educ_level, no_wives)
        SELECT hhid_2, survey_year, respondent_sex, hhh_sex, hhh_age, hhh_educ_level,
               spouse_sex, spouse_age, spouse_educ_level, no_wives
        FROM staging_survey
        WHERE hhid_2 IS NOT NULL AND survey_year IS NOT NULL
        ON CONFLICT DO NOTHING;
    """)
    
    # Populate fact_survey
    cursor.execute("""
        INSERT INTO fact_survey (
            hhid_2, survey_year, submission_date, duration, survey_type, status, tot_hhmembers,
            hh_size, females_hh_count, children_num_u5, asp_actual_income, material_walls,
            material_roof, fuel_source_cooking, every_member_shoes
        )
        SELECT hhid_2, survey_year, submission_date, duration, survey_type, status, tot_hhmembers,
               hh_size, females_hh_count, children_num_u5, asp_actual_income, material_walls,
               material_roof, fuel_source_cooking, every_member_shoes
        FROM staging_survey
        WHERE hhid_2 IS NOT NULL AND survey_year IS NOT NULL
        ON CONFLICT (hhid_2, survey_year) DO NOTHING;
    """)
    
    # Populate fact_expenditure
    cursor.execute("""
        INSERT INTO fact_expenditure (hhid_2, survey_year, cereals_week, tubers_week, medical_care_annual, total_expenditure)
        SELECT hhid_2, survey_year, cereals_week, tubers_week, medical_care_annual,
               COALESCE(cereals_week, 0) + COALESCE(tubers_week, 0) + COALESCE(medical_care_annual, 0)
        FROM staging_survey
        WHERE hhid_2 IS NOT NULL AND survey_year IS NOT NULL
        ON CONFLICT (hhid_2, survey_year) DO NOTHING;
    """)
    
    # Populate fact_crop_yield dynamically
    crop_types = [
        "beans", "gnuts", "soya_beans", "peas", "maize", "millet", "sorghum", "barley",
        "rice", "irish_potatoes", "sweetpotatoes", "cassava", "yams", "garlic", "ginger", "tabbaco"
    ]
    for crop in crop_types:
        cursor.execute(f"""
            INSERT INTO fact_crop_yield (hhid_2, survey_year, crop_type, planted_qty, total_yield, yield_sold, yield_consumed, market_price)
            SELECT hhid_2, survey_year, '{crop}' AS crop_type, sn_1_{crop}_planted, sn_1_{crop}_Total_Yield,
                   sn_1_{crop}_Total_Yield_sold, sn_1_{crop}_Total_Yield_consume,
                   COALESCE(sn_1_{crop}_Market_Price_fresh, sn_1_{crop}_Market_Price)
            FROM staging_survey
            WHERE hhid_2 IS NOT NULL AND survey_year IS NOT NULL AND sn_1_{crop}_planted IS NOT NULL
            ON CONFLICT (hhid_2, survey_year, crop_type) DO NOTHING;
        """)
    
    conn.commit()
    cursor.close()
    conn.close()

# Data quality checks
def validate_data():
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # Check duplicates
    cursor.execute("SELECT COUNT(*) FROM fact_survey WHERE (hhid_2, survey_year) IN (SELECT hhid_2, survey_year FROM fact_survey GROUP BY hhid_2, survey_year HAVING COUNT(*) > 1)")
    duplicates = cursor.fetchone()[0]
    if duplicates > 0:
        raise ValueError(f"Found {duplicates} duplicate records in fact_survey")
    
    # Check null rates
    cursor.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN asp_actual_income IS NULL THEN 1 ELSE 0 END) AS nulls FROM fact_survey")
    total, nulls = cursor.fetchone()
    null_rate = nulls / total if total > 0 else 0
    if null_rate > 0.5:
        print(f"Warning: High null rate ({null_rate:.2%}) for asp_actual_income")
    
    # Log counts
    cursor.execute("""
        SELECT 'fact_survey' AS table_name, COUNT(*) FROM fact_survey
        UNION ALL
        SELECT 'fact_expenditure', COUNT(*) FROM fact_expenditure
        UNION ALL
        SELECT 'fact_crop_yield', COUNT(*) FROM fact_crop_yield
    """)
    rows = cursor.fetchall()
    for row in rows:
        print(f"Loaded {row[1]} rows into {row[0]}")
    
    cursor.close()
    conn.close()

# Airflow DAG
with DAG(
    "rtv_pipeline",
    start_date=datetime(2025, 5, 30),
    schedule_interval=None,
    catchup=False
) as dag:
    ingest_task = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    load_task = PythonOperator(task_id="load_to_staging", python_callable=load_to_staging)
    transform_task = PythonOperator(task_id="transform_data", python_callable=transform_data)
    validate_task = PythonOperator(task_id="validate_data", python_callable=validate_data)
    
    ingest_task >> load_task >> transform_task >> validate_task