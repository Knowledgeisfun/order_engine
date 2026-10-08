from database import SessionLocal
from models import Item, InventoryBucket

def seed_data():
    db = SessionLocal()
    try:
        if db.query(Item).first():
            print("Database already seeded!")
            return

        print("Seeding initial flash-sale items and stock...")
        item1 = Item(name="Flash Sale Laptop", price=1200.00)
        item2 = Item(name="Wireless Earbuds", price=150.00)

        db.add_all([item1, item2])
        db.commit()

        db.refresh(item1)
        db.refresh(item2)

        inv1 = InventoryBucket(item_id=item1.id, stock=50)
        inv2 = InventoryBucket(item_id=item2.id, stock=100)

        db.add_all([inv1, inv2])
        db.commit()

        print("✅ Success! Seeded items and stock into PostgreSQL.")
    except Exception as e:
        db.rollback()
        print(f"Error seeding data: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    seed_data()