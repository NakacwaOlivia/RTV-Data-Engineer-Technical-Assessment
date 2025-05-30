# RTV Data Engineer Technical Assessment

## 📌 Overview
This repository contains the solution to the Raising The Village (RTV) Data Engineering Technical Assessment. The goal is to build a complete data pipeline and analytical dashboard to track poverty-related household progress using RTV's longitudinal survey data.

## 📁 Repository Structure
```
rtv-data-engineer-assessment/
├── data/
│   ├── combined_data/                   # Provided datasets & collection tools
├── notebooks/
│   ├── exploratory_analysis.ipynb       # EDA and profiling
├── pipeline/
│   ├── ingestion/                       # Ingestion scripts
│   ├── transformation/                  # Transformation logic
│   └── orchestration/                   # Airflow DAGs
├── warehouse/
│   ├── schema.sql                       # DDL for data warehouse
│   ├── models/                          # SQL models
├── dashboard/
│   ├── app.py                          # Streamlit dashboard
├── docs/
│   ├── architecture_diagram.png         # System architecture
│   ├── design_document.md              # Technical design
│   └── data_quality_report.md          # Data quality metrics
├── tests/
│   ├── test_pipeline.py                # Unit tests
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## ⚙️ Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/rtv-data-engineer-assessment.git
cd rtv-data-engineer-assessment
```

### 2. Create and Activate Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Launch Services via Docker
```bash
docker-compose up --build
```
This will initialize:
- PostgreSQL (Data Warehouse) on port 5432
- MinIO (Object Storage) on ports 9000 (API) and 9090 (Console)
- Airflow on port 8080
- Streamlit Dashboard on port 8501

### 4. Access the Services
- Airflow UI: http://localhost:8080 (admin/admin)
- MinIO Console: http://localhost:9090 (minioadmin/minioadmin)
- Streamlit Dashboard: http://localhost:8501

### 5. Run the Pipeline
```bash
# Using Airflow UI
airflow dags trigger rtv_pipeline

# Or manually run scripts
python pipeline/ingestion/ingest_data.py
python pipeline/transformation/transform_data.py
```

## 🛠️ Technology Stack
- **Data Processing**: Python, Pandas, SQL
- **Data Warehouse**: PostgreSQL
- **Object Storage**: MinIO
- **Orchestration**: Apache Airflow
- **Transformations**: SQL Models
- **Visualization**: Streamlit
- **Containerization**: Docker & Docker Compose

## 📊 Dashboard Features
The Streamlit dashboard provides:
- Trend analysis across Baseline, Year 1, and Year 2 surveys
- Geographic drill-down capabilities
- Household indicator tracking
- Progress metrics visualization
- Interactive data exploration

## ✅ Data Quality & Validation
- Automated data quality checks
- Schema validation across survey rounds
- Variable normalization
- Data completeness monitoring
- Detailed quality reports in `docs/data_quality_report.md`

## 🔍 Testing
Run the test suite:
```bash
pytest tests/
```

Test coverage includes:
- Data ingestion validation
- Transformation logic
- Data quality checks
- API endpoints
- Dashboard components

## 📝 Documentation
- Technical Design: `docs/design_document.md`
- Architecture: `docs/architecture_diagram.png`
- Data Quality: `docs/data_quality_report.md`

## 🔐 Security
- All credentials are managed through environment variables
- Database access is restricted to internal network
- MinIO access requires authentication
- Airflow authentication enabled

## 🤝 Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 