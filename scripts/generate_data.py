"""
STEP 1: Generate sample transportation data
This script creates realistic CSV files for UrbanMove Analytics
"""

import pandas as pd
import random
from datetime import datetime, timedelta

# Set seed for reproducibility
random.seed(42)

def generate_ridership_data(num_records=200):
    """Generate passenger ridership data"""
    
    routes = ['Route_A1', 'Route_B2', 'Route_C3', 'Route_D4', 'Route_E5']
    stops = ['Central_Station', 'Airport', 'Mall', 'University', 
             'Hospital', 'Stadium', 'Market', 'Park']
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        # Introduce some intentional data quality issues for teaching
        passenger_count = random.randint(10, 150)
        
        # Add some missing values (10% of records)
        if random.random() < 0.10:
            passenger_count = None
            
        record = {
            'trip_id': f'TRIP_{i+1:04d}',
            'route_id': random.choice(routes),
            'stop_name': random.choice(stops),
            'passenger_count': passenger_count,
            'trip_date': (start_date + timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d'),
            'trip_hour': random.randint(5, 23),
            'vehicle_id': f'VH_{random.randint(1, 20):03d}',
            'direction': random.choice(['Northbound', 'Southbound', 'Eastbound', 'Westbound'])
        }
        data.append(record)
    
    # Add some duplicate records for teaching data cleaning
    data.extend(data[:5])  # duplicate first 5 records
    
    df = pd.DataFrame(data)
    df.to_csv('data/ridership.csv', index=False)
    print(f"✅ Generated ridership.csv with {len(df)} records (includes duplicates)")
    return df


def generate_vehicle_locations(num_records=150):
    """Generate GPS vehicle location data"""
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        # Lagos, Nigeria coordinates range (for realism)
        lat = round(random.uniform(6.4, 6.7), 6)
        lon = round(random.uniform(3.2, 3.6), 6)
        
        # Some invalid coordinates for data quality teaching
        if random.random() < 0.05:
            lat = 999.99   # Invalid coordinate
            
        record = {
            'location_id': f'LOC_{i+1:04d}',
            'vehicle_id': f'VH_{random.randint(1, 20):03d}',
            'latitude': lat,
            'longitude': lon,
            'speed_kmh': round(random.uniform(0, 80), 1),
            'timestamp': (start_date + timedelta(
                days=random.randint(0, 180),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).strftime('%Y-%m-%d %H:%M:%S'),
            'status': random.choice(['Moving', 'Stopped', 'Idle', None])
        }
        data.append(record)
    
    df = pd.DataFrame(data)
    df.to_csv('data/vehicle_locations.csv', index=False)
    print(f"✅ Generated vehicle_locations.csv with {len(df)} records")
    return df


def generate_weather_data(num_records=180):
    """Generate daily weather data"""
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        record = {
            'weather_id': f'WX_{i+1:04d}',
            'weather_date': (start_date + timedelta(days=i)).strftime('%Y-%m-%d'),
            'temperature_c': round(random.uniform(22, 38), 1),
            'humidity_pct': random.randint(50, 95),
            'rainfall_mm': round(random.uniform(0, 50), 1) if random.random() < 0.3 else 0.0,
            'weather_condition': random.choice([
                'Sunny', 'Cloudy', 'Rainy', 'Heavy Rain', 'Partly Cloudy'
            ]),
            'wind_speed_kmh': round(random.uniform(5, 40), 1)
        }
        data.append(record)
    
    df = pd.DataFrame(data)
    df.to_csv('data/weather.csv', index=False)
    print(f"✅ Generated weather.csv with {len(df)} records")
    return df


if __name__ == "__main__":
    print("🚌 UrbanMove Analytics - Generating sample data...\n")
    generate_ridership_data()
    generate_vehicle_locations()
    generate_weather_data()
    print("\n✅ All data files created in /data folder!")