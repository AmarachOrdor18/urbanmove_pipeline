"""
STEP 2: Create the PostgreSQL database and tables
Run this once to set up your database
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ── Configuration ──────────────────────────────────────────
DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'user':     'postgres',
    'password': 'postgres',   # Change if you used a different password
    'database': 'urbanmove'
}


def create_database():
    """Create the urbanmove database if it doesn't exist"""
    # Connect to default 'postgres' database first
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database='postgres'
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'urbanmove'")
    exists = cursor.fetchone()
    
    if not exists:
        cursor.execute("CREATE DATABASE urbanmove")
        print("✅ Database 'urbanmove' created successfully!")
    else:
        print("ℹ️  Database 'urbanmove' already exists, skipping creation.")
    
    cursor.close()
    conn.close()


def create_tables():
    """Create all tables in the urbanmove database"""
    
    # Read SQL from file
    with open('sql/create_tables.sql', 'r') as f:
        sql_script = f.read()
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        cursor.execute(sql_script)
        conn.commit()
        print("✅ All tables created successfully!")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error creating tables: {e}")
    finally:
        cursor.close()
        conn.close()


def test_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connection successful!")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    print("🔧 Setting up UrbanMove database...\n")
    create_database()
    test_connection()
    create_tables()
    print("\n✅ Database setup complete!")