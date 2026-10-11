# reset_db.py
from database import engine
from models import Base

print("🗑️ Dropping old tables...")
Base.metadata.drop_all(bind=engine)

print("🏗️ Creating new tables with String UUIDs...")
Base.metadata.create_all(bind=engine)

print("✅ Database reset complete!")