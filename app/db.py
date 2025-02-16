import os
import sys
from psycopg2 import pool, DatabaseError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("DATABASE_URL not found")
    sys.exit(1)

# Initialize connection pool (created during FastAPI startup)
connection_pool = None


def init_db():
    """Initialize the PostgreSQL connection pool."""
    global connection_pool
    try:
        connection_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)
        print("Database connection pool created successfully")
    except DatabaseError as e:
        print(f"Database error: {e}")
        sys.exit(1)


def close_db():
    """Close the database connection pool."""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        print("Connection pool closed")


def get_connection():
    """Retrieve a database connection from the pool."""
    return connection_pool.getconn()


def release_connection(conn):
    """Return a database connection to the pool."""
    if conn:
        connection_pool.putconn(conn)
