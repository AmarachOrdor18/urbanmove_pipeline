# 🚌 UrbanMove Pipeline

A comprehensive **ETL (Extract-Transform-Load) pipeline** for urban transportation analytics. This project automates the collection, processing, and analysis of ridership data, vehicle GPS locations, and weather conditions using Apache Airflow.

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Database Schema](#database-schema)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## 🎯 Project Overview

UrbanMove Pipeline is designed to:

- **Extract** transportation data from multiple CSV sources
- **Transform** and validate data for consistency and quality
- **Load** processed data into a PostgreSQL database
- **Orchestrate** the entire workflow using Apache Airflow
- **Generate** synthetic test data for development and testing

### Key Features

✅ Automated daily data processing  
✅ Multi-source data integration (ridership, vehicle locations, weather)  
✅ Data quality validation and cleaning  
✅ PostgreSQL persistence with optimized schema  
✅ Apache Airflow orchestration with scheduling  
✅ Comprehensive logging and error handling  

## 🏗️ Architecture

```
Data Sources (CSVs)
        ↓
   [EXTRACT] ─→ Read ridership, vehicle, weather data
        ↓
   [TRANSFORM] ─→ Clean, validate, and normalize data
        ↓
   [LOAD] ─→ Insert into PostgreSQL database
        ↓
   [AIRFLOW] ─→ Schedule and monitor daily runs
```

### Technology Stack

| Component | Purpose |
|-----------|---------|
| **Apache Airflow** | Workflow orchestration & scheduling |
| **PostgreSQL** | Data warehouse |
| **Pandas** | Data processing & transformation |
| **Python 3.8+** | Core programming language |
| **SQLAlchemy** | Database ORM |

## 📦 Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- pip (Python package manager)
- 2GB RAM minimum
- Unix-like shell (Bash/Zsh) or PowerShell on Windows

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd urbanmove_pipeline
```

### 2. Create a Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up PostgreSQL Database

```bash
# Create a new PostgreSQL database
createdb urbanmove

# Run the schema setup script
psql -U postgres -d urbanmove -f sql/create_tables.sql
```

### 5. Configure Environment Variables

Create a `.env` file in the project root:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=urbanmove

# Airflow Configuration
AIRFLOW_HOME=./airflow
AIRFLOW__CORE__DAGS_FOLDER=./dags
```

### 6. Initialize Airflow

```bash
# Set Airflow home
export AIRFLOW_HOME=$PWD/airflow  # or set-item env:AIRFLOW_HOME on Windows

# Initialize Airflow database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

## 📁 Project Structure

```
urbanmove_pipeline/
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies
├── .env                        # Environment variables (not in repo)
│
├── dags/
│   └── urbanmove_dag.py       # Airflow DAG definition
│
├── scripts/
│   ├── db_setup.py            # Database initialization script
│   ├── etl_pipeline.py        # Main ETL logic (Extract, Transform, Load)
│   └── generate_data.py       # Synthetic data generator for testing
│
├── sql/
│   └── create_tables.sql      # PostgreSQL schema definition
│
├── data/
│   ├── ridership.csv          # Sample ridership data
│   ├── vehicle_locations.csv  # Sample vehicle GPS data
│   └── weather.csv            # Sample weather conditions
│
└── screenshots/                # Documentation screenshots
```

## 💻 Usage

### Option 1: Run ETL Pipeline Directly

```bash
# Navigate to scripts directory
cd scripts

# Run the pipeline
python etl_pipeline.py
```

Output:
```
📂 Extracting ridership data...
   → Loaded X rows, Y columns
✅ Data processing complete!
```

### Option 2: Generate Test Data

```bash
cd scripts
python generate_data.py
```

Creates fresh CSV files with randomly generated transportation data.

### Option 3: Run with Apache Airflow (Recommended)

#### Start Airflow Scheduler

```bash
airflow scheduler
```

#### Start Airflow Web UI

```bash
airflow webui
```

Visit **http://localhost:8080** in your browser.

- Username: `admin`
- Password: `admin`

#### Trigger the DAG

In the Airflow UI:
1. Locate `urbanmove_daily_pipeline` in the DAGs list
2. Click the play button (▶) to trigger the workflow
3. Monitor execution in the Graph View

### Option 4: Run Database Setup Script

```bash
cd scripts
python db_setup.py
```

Initializes the database with empty tables and configuration.

## 📊 Database Schema

### Table 1: `ridership`

Stores public transportation ridership data.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `trip_id` | VARCHAR(20) | Unique trip identifier |
| `route_id` | VARCHAR(20) | Bus/transit route ID |
| `stop_name` | VARCHAR(50) | Transit stop name |
| `passenger_count` | INTEGER | Number of passengers |
| `trip_date` | DATE | Date of trip |
| `trip_hour` | INTEGER | Hour of day (0-23) |
| `vehicle_id` | VARCHAR(20) | Vehicle identifier |
| `direction` | VARCHAR(20) | Route direction (N/S/E/W) |
| `loaded_at` | TIMESTAMP | When record was loaded |

### Table 2: `vehicle_locations`

Stores GPS location and status of vehicles.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `location_id` | VARCHAR(20) | Unique location record ID |
| `vehicle_id` | VARCHAR(20) | Vehicle identifier |
| `latitude` | DECIMAL(9,6) | GPS latitude |
| `longitude` | DECIMAL(9,6) | GPS longitude |
| `speed_kmh` | DECIMAL(5,1) | Current speed (km/h) |
| `timestamp` | TIMESTAMP | GPS reading timestamp |
| `status` | VARCHAR(20) | Vehicle status (active/inactive) |
| `loaded_at` | TIMESTAMP | When record was loaded |

### Table 3: `weather`

Stores weather conditions data.

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `weather_id` | VARCHAR(20) | Unique weather record ID |
| `weather_date` | DATE | Date of weather observation |
| `temperature_c` | DECIMAL(4,1) | Temperature in Celsius |
| `humidity_pct` | INTEGER | Humidity percentage |
| `rainfall_mm` | DECIMAL(5,1) | Rainfall in millimeters |
| `weather_condition` | VARCHAR(30) | Condition description (sunny/rainy/cloudy) |
| `wind_speed_kmh` | DECIMAL(4,1) | Wind speed (km/h) |
| `loaded_at` | TIMESTAMP | When record was loaded |

## ⚙️ Configuration

### Database Configuration

Edit `scripts/etl_pipeline.py`:

```python
DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'user':     'postgres',
    'password': 'postgres',
    'database': 'urbanmove'
}
```

### Airflow Configuration

Edit `dags/urbanmove_dag.py`:

```python
default_args = {
    'owner': 'urbanmove_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

### Schedule Interval

Modify the DAG schedule to change how often the pipeline runs:

```python
with DAG(
    dag_id='urbanmove_daily_pipeline',
    schedule_interval='@daily',  # Options: @hourly, @daily, @weekly, '0 2 * * *'
    ...
)
```

## 🐛 Troubleshooting

### Issue: PostgreSQL Connection Error

**Error**: `could not connect to server: Connection refused`

**Solution**:
```bash
# Check if PostgreSQL is running
# Windows: Services > PostgreSQL
# macOS: brew services list
# Linux: sudo systemctl status postgresql

# Verify connection parameters in DB_CONFIG
psql -h localhost -U postgres -d urbanmove
```

### Issue: Airflow DAG Not Showing

**Solution**:
```bash
# Verify AIRFLOW_HOME and DAGS_FOLDER
export AIRFLOW_HOME=$PWD/airflow

# Refresh DAGs in UI
airflow dags list

# Check for syntax errors
python -m py_compile dags/urbanmove_dag.py
```

### Issue: Out of Memory During Processing

**Solution**:
```bash
# Process data in chunks (modify etl_pipeline.py)
chunksize = 10000
for chunk in pd.read_csv('data/ridership.csv', chunksize=chunksize):
    # Process chunk
    pass
```

### Issue: Data Validation Failures

**Check logs**:
```bash
# View pipeline logs
tail -f airflow/logs/urbanmove_daily_pipeline/
```

## 📝 Development Workflow

1. **Create test data**: `python scripts/generate_data.py`
2. **Test ETL locally**: `python scripts/etl_pipeline.py`
3. **Deploy to Airflow**: Place DAG in `dags/` folder
4. **Monitor execution**: Use Airflow Web UI
5. **Query results**: Use pgAdmin or `psql`

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/)
- [SQLAlchemy Documentation](https://www.sqlalchemy.org/)

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

## 👥 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📧 Support

For questions or issues, please open an issue in the repository or contact the development team.

---

**Last Updated**: June 2026  
**Version**: 1.0.0
