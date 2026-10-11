from typing import List, Optional
from pydantic import BaseModel
from models import OrderStatus


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