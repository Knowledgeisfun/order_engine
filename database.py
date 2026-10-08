import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Database URL (Default connects to local Postgres 'order_db')
# Format: postgresql://<username>:<password>@<host>:<port>/<dbname>
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/order_db"
)

# Engine configured with Connection Pooling for high concurrency
engine = create_engine(
    DATABASE_URL,
    pool_size=20,          # 20 pre-allocated active DB connections
    max_overflow=10,       # Up to 10 extra temporary connections during traffic spikes
    pool_pre_ping=True     # Verifies connection health before executing queries
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# FastAPI dependency to yield a DB session per HTTP request
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()