import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_engine() -> Engine:
    """
    Create and return a SQLAlchemy engine for database connections.
    This is the recommended way for pandas.read_sql() to avoid warnings.
    """
    try:
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '3306')
        db_name = os.getenv('DB_NAME', 'roomiego')
        db_user = os.getenv('DB_USERNAME', 'root')
        db_password = os.getenv('DB_PASSWORD', 'root')
        
        # Create connection string for MySQL using pymysql driver
        connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        # Create engine with connection pooling
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,  # Verify connections before using them
            pool_recycle=3600,   # Recycle connections after 1 hour
            echo=False           # Set to True for SQL debugging
        )
        
        return engine
    except Exception as e:
        print(f"Lỗi kết nối MySQL: {e}")
        return None

# Backward compatibility: keep the old function name but return engine
def get_db_connection():
    """
    Deprecated: Use get_db_engine() instead.
    Returns SQLAlchemy engine for compatibility.
    """
    return get_db_engine()