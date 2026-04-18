"""
Database schema and initialization for the UBID platform using SQLAlchemy.
Swappable to PostgreSQL via DATABASE_URL environment variable.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from database.models import Base

# Default to SQLite for the demo
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ubid.db")
DEFAULT_URL = f"sqlite:///{DB_PATH}"

# Allow overriding with PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL", DEFAULT_URL)

def get_engine():
    if DATABASE_URL.startswith("sqlite"):
        engine = create_engine(DATABASE_URL, connect_args={'timeout': 30.0})
    else:
        engine = create_engine(DATABASE_URL)
    return engine

Engine = get_engine()
SessionFactory = sessionmaker(bind=Engine)
Session = scoped_session(SessionFactory)

def get_session():
    """Get a database session thread-safely."""
    return Session()

def init_db():
    """Create all tables if they don't exist."""
    Base.metadata.create_all(Engine)
    print(f"[DB] Database initialized at {DATABASE_URL}")

if __name__ == "__main__":
    init_db()
