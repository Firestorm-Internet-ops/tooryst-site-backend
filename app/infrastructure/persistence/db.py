"""Database setup helpers (SQLAlchemy engine/session)."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from backend/.env
backend_dir = Path(__file__).parent.parent.parent.parent
env_file = backend_dir / ".env"
load_dotenv(env_file)

# Build DATABASE_URL from individual variables or use provided URL
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    # Construct from individual variables
    db_user = os.getenv("DATABASE_USER", "root")
    db_password = os.getenv("DATABASE_PASSWORD", "")
    db_host = os.getenv("DATABASE_HOST", "localhost")
    db_port = os.getenv("DATABASE_PORT", "3306")
    db_name = os.getenv("DATABASE_NAME", "storyboard")
    
    DATABASE_URL = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()


def get_db():
    """FastAPI-style dependency to provide a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


