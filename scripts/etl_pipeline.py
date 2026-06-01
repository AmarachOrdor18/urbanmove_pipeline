"""
STEP 3: ETL Pipeline
Extract → Transform (Clean) → Load into PostgreSQL

This is the main pipeline script that:
1. Reads CSV files (Extract)
2. Cleans and validates data (Transform)  
3. Loads into PostgreSQL (Load)
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ── Configuration ──────────────────────────────────────────
DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'user':     'postgres',
    'password': 'postgres',
    'database': 'urbanmove'
}

# ══════════════════════════════════════════════════════════════
# EXTRACT FUNCTIONS
# ══════════════════════════════════════════════════════════════

def extract_ridership():
    """Extract ridership data from CSV"""
    print("📂 Extracting ridership data...")
    df = pd.read_csv('data/ridership.csv')
    print(f"   → Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

def extract_vehicle_locations():
    """Extract vehicle GPS data from CSV"""
    print("📂 Extracting vehicle location data...")
    df = pd.read_csv('data/vehicle_locations.csv')
    print(f"   → Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

def extract_weather():
    """Extract weather data from CSV"""
    print("📂 Extracting weather data...")
    df = pd.read_csv('data/weather.csv')
    print(f"   → Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


# ══════════════════════════════════════════════════════════════
# TRANSFORM / CLEAN FUNCTIONS  
# ══════════════════════════════════════════════════════════════

def transform_ridership(df):
    """Clean and validate ridership data"""
    print("\n🔄 Transforming ridership data...")
    original_count = len(df)
    
    # 1. Remove duplicate trip_ids
    df = df.drop_duplicates(subset=['trip_id'])
    removed_dupes = original_count - len(df)
    print(f"   → Removed {removed_dupes} duplicate records")
    
    # 2. Handle missing passenger_count — fill with median
    missing_count = df['passenger_count'].isna().sum()
    median_passengers = df['passenger_count'].median()
    df['passenger_count'] = df['passenger_count'].fillna(median_passengers).astype(int)
    print(f"   → Filled {missing_count} missing passenger counts with median ({median_passengers})")
    
    # 3. Validate trip_hour is between 0-23
    invalid_hours = df[~df['trip_hour'].between(0, 23)]
    if len(invalid_hours) > 0:
        df = df[df['trip_hour'].between(0, 23)]
        print(f"   → Removed {len(invalid_hours)} records with invalid trip_hour")
    
    # 4. Convert trip_date to proper date format
    df['trip_date'] = pd.to_datetime(df['trip_date']).dt.date
    
    # 5. Strip whitespace from text columns
    df['route_id'] = df['route_id'].str.strip()
    df['direction'] = df['direction'].str.strip()
    
    print(f"   → Final record count: {len(df)}")
    return df


def transform_vehicle_locations(df):
    """Clean and validate vehicle location data"""
    print("\n🔄 Transforming vehicle location data...")
    original_count = len(df)
    
    # 1. Remove records with invalid coordinates
    # Valid latitude: -90 to 90, Valid longitude: -180 to 180
    invalid_coords = df[
        ~(df['latitude'].between(-90, 90)) |
        ~(df['longitude'].between(-180, 180))
    ]
    df = df[
        df['latitude'].between(-90, 90) &
        df['longitude'].between(-180, 180)
    ]
    print(f"   → Removed {len(invalid_coords)} records with invalid GPS coordinates")
    
    # 2. Fill missing status with 'Unknown'
    df['status'] = df['status'].fillna('Unknown')
    
    # 3. Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # 4. Remove duplicate location_ids
    df = df.drop_duplicates(subset=['location_id'])
    
    print(f"   → Final record count: {len(df)}")
    return df


def transform_weather(df):
    """Clean and validate weather data"""
    print("\n🔄 Transforming weather data...")
    
    # 1. Convert weather_date to date
    df['weather_date'] = pd.to_datetime(df['weather_date']).dt.date
    
    # 2. Validate temperature range (reasonable for Lagos: 15-45°C)
    df = df[df['temperature_c'].between(15, 45)]
    
    # 3. Validate humidity (0-100%)
    df = df[df['humidity_pct'].between(0, 100)]
    
    # 4. Fill negative rainfall with 0
    df['rainfall_mm'] = df['rainfall_mm'].clip(lower=0)
    
    # 5. Remove duplicates
    df = df.drop_duplicates(subset=['weather_id'])
    
    print(f"   → Final record count: {len(df)}")
    return df


# ══════════════════════════════════════════════════════════════
# DATA QUALITY VALIDATION
# ══════════════════════════════════════════════════════════════

def validate_data(df, table_name):
    """Run basic data quality checks before loading"""
    print(f"\n✅ Validating {table_name}...")
    
    checks_passed = True
    
    # Check 1: No empty dataframe
    if len(df) == 0:
        print(f"   ❌ FAIL: {table_name} has no records!")
        checks_passed = False
    else:
        print(f"   ✓ Record count check passed: {len(df)} records")
    
    # Check 2: No completely null columns
    null_pct = df.isnull().mean() * 100
    high_null_cols = null_pct[null_pct > 50]
    if len(high_null_cols) > 0:
        print(f"   ⚠️  WARNING: Columns with >50% nulls: {list(high_null_cols.index)}")
    else:
        print(f"   ✓ Null check passed")
    
    # Check 3: No negative passenger counts (for ridership)
    if 'passenger_count' in df.columns:
        negative = (df['passenger_count'] < 0).sum()
        if negative > 0:
            print(f"   ❌ FAIL: {negative} negative passenger counts found!")
            checks_passed = False
        else:
            print(f"   ✓ Passenger count values are valid")
    
    return checks_passed


# ══════════════════════════════════════════════════════════════
# LOAD FUNCTIONS
# ══════════════════════════════════════════════════════════════

def get_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)


def load_ridership(df):
    """Load ridership data into PostgreSQL"""
    print("\n📤 Loading ridership data into PostgreSQL...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Clear existing data (for daily refresh)
    cursor.execute("TRUNCATE TABLE ridership RESTART IDENTITY CASCADE")
    
    # Prepare data as list of tuples
    records = [
        (
            row['trip_id'], row['route_id'], row['stop_name'],
            row['passenger_count'], row['trip_date'], row['trip_hour'],
            row['vehicle_id'], row['direction']
        )
        for _, row in df.iterrows()
    ]
    
    # Bulk insert using execute_values (much faster than row by row)
    insert_query = """
        INSERT INTO ridership 
            (trip_id, route_id, stop_name, passenger_count, 
             trip_date, trip_hour, vehicle_id, direction)
        VALUES %s
        ON CONFLICT (trip_id) DO NOTHING
    """
    
    execute_values(cursor, insert_query, records)
    conn.commit()
    
    # Verify load
    cursor.execute("SELECT COUNT(*) FROM ridership")
    count = cursor.fetchone()[0]
    print(f"   → Successfully loaded {count} records into ridership table")
    
    cursor.close()
    conn.close()


def load_vehicle_locations(df):
    """Load vehicle location data into PostgreSQL"""
    print("\n📤 Loading vehicle location data into PostgreSQL...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE vehicle_locations RESTART IDENTITY CASCADE")
    
    records = [
        (
            row['location_id'], row['vehicle_id'], row['latitude'],
            row['longitude'], row['speed_kmh'], row['timestamp'], row['status']
        )
        for _, row in df.iterrows()
    ]
    
    insert_query = """
        INSERT INTO vehicle_locations 
            (location_id, vehicle_id, latitude, longitude, 
             speed_kmh, timestamp, status)
        VALUES %s
        ON CONFLICT (location_id) DO NOTHING
    """
    
    execute_values(cursor, insert_query, records)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM vehicle_locations")
    count = cursor.fetchone()[0]
    print(f"   → Successfully loaded {count} records into vehicle_locations table")
    
    cursor.close()
    conn.close()


def load_weather(df):
    """Load weather data into PostgreSQL"""
    print("\n📤 Loading weather data into PostgreSQL...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("TRUNCATE TABLE weather RESTART IDENTITY CASCADE")
    
    records = [
        (
            row['weather_id'], row['weather_date'], row['temperature_c'],
            row['humidity_pct'], row['rainfall_mm'], 
            row['weather_condition'], row['wind_speed_kmh']
        )
        for _, row in df.iterrows()
    ]
    
    insert_query = """
        INSERT INTO weather 
            (weather_id, weather_date, temperature_c, humidity_pct, 
             rainfall_mm, weather_condition, wind_speed_kmh)
        VALUES %s
        ON CONFLICT (weather_id) DO NOTHING
    """
    
    execute_values(cursor, insert_query, records)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM weather")
    count = cursor.fetchone()[0]
    print(f"   → Successfully loaded {count} records into weather table")
    
    cursor.close()
    conn.close()


# ══════════════════════════════════════════════════════════════
# MAIN PIPELINE RUNNER
# ══════════════════════════════════════════════════════════════

def run_pipeline():
    """Run the complete ETL pipeline"""
    start_time = datetime.now()
    print("=" * 60)
    print("🚌 URBANMOVE ANALYTICS - ETL PIPELINE STARTING")
    print(f"   Run time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # ── RIDERSHIP ────────────────────────────────
        df_ridership = extract_ridership()
        df_ridership = transform_ridership(df_ridership)
        validate_data(df_ridership, 'ridership')
        load_ridership(df_ridership)
        
        # ── VEHICLE LOCATIONS ────────────────────────
        df_locations = extract_vehicle_locations()
        df_locations = transform_vehicle_locations(df_locations)
        validate_data(df_locations, 'vehicle_locations')
        load_vehicle_locations(df_locations)
        
        # ── WEATHER ──────────────────────────────────
        df_weather = extract_weather()
        df_weather = transform_weather(df_weather)
        validate_data(df_weather, 'weather')
        load_weather(df_weather)
        
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        
        print("\n" + "=" * 60)
        print(f"✅ PIPELINE COMPLETED SUCCESSFULLY in {duration}s")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()