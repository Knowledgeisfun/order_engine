from database import engine, Base
import models  # Ensures models are registered with Base metadata

def create_tables():
    print("Connecting to PostgreSQL and creating tables...")
    Base.metadata.create_all(bind=engine)
    print("✅ Success! Tables created: items, inventory_buckets, orders")

if __name__ == "__main__":
    create_tables()