from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel
from database import get_db
from models import Item, InventoryBucket, Order, OrderStatus

app = FastAPI(title="Distributed High-Throughput Order Engine")

# --- Pydantic DTO Schemas ---
class InventorySchema(BaseModel):
    stock: int
    class Config:
        from_attributes = True

class ItemResponse(BaseModel):
    id: int
    name: str
    price: float
    inventory: Optional[InventorySchema] = None
    class Config:
        from_attributes = True

class OrderCreate(BaseModel):
    user_id: int
    item_id: int
    quantity: int = 1

class OrderResponse(BaseModel):
    id: int
    user_id: int
    item_id: int
    quantity: int
    status: OrderStatus
    class Config:
        from_attributes = True

# --- API Endpoints ---
@app.get("/")
def root():
    return {"status": "online", "message": "Order Engine API is running!"}

@app.get("/items", response_model=List[ItemResponse])
def get_items(db: Session = Depends(get_db)):
    return db.query(Item).all()

@app.post("/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
def create_order(order_data: OrderCreate, db: Session = Depends(get_db)):
    # 1. Fetch item inventory
    updated_rows = db.query(InventoryBucket).filter(InventoryBucket.item_id == order_data.item_id, InventoryBucket.stock >= order_data.quantity).update({InventoryBucket.stock: InventoryBucket.stock - order_data.quantity}, synchronize_session = False)

    if updated_rows == 0:
        item_exsist = db.query(Item).filter(Item.id == order_data.item_id).first()

        if not item_exsist:
            raise HTTPException(status_code=404, detail="Item not found")
        raise HTTPException(status_code=400, detail="Out of stock! or insufficent inventory")


    # 3. Deduct Stock & Create Order
    new_order = Order(
        user_id=order_data.user_id,
        item_id=order_data.item_id,
        quantity=order_data.quantity,
        status=OrderStatus.COMPLETED
    )

    db.add(new_order)
    db.commit()
    db.refresh(new_order)

    return new_order