import enum
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Enum, CheckConstraint
from sqlalchemy.orm import relationship
from database import Base

class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # 1-to-1 relationship with inventory
    inventory = relationship("InventoryBucket", back_populates="item", uselist=False)

class InventoryBucket(Base):
    __tablename__ = "inventory_buckets"

    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False, unique=True)
    stock = Column(Integer, nullable=False, default=0)
    version = Column(Integer, nullable=False, default=1)  # Version counter for Optimistic Locking

    item = relationship("Item", back_populates="inventory")

    # Hard database constraint: prevents stock from dropping below 0 under race conditions
    __table_args__ = (
        CheckConstraint("stock >= 0", name="check_stock_positive"),
    )

class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False, index=True)
    quantity = Column(Integer, nullable=False, default=1)
    status = Column(Enum(OrderStatus), default=OrderStatus.PENDING, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    item = relationship("Item")