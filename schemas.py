"""
Pydantic models matching the FoodExpress event contract exactly.
Anything that doesn't match this shape gets rejected with a 422
before it ever reaches RabbitMQ.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Shared sub-objects
# ---------------------------------------------------------------------------

class OrderItem(BaseModel):
    menuItemId: str
    name: str
    qty: int = Field(gt=0)
    price: float = Field(ge=0)


# ---------------------------------------------------------------------------
# data payloads (the "data" field inside each envelope)
# ---------------------------------------------------------------------------

class OrderCreatedData(BaseModel):
    orderId: UUID
    customerId: UUID
    restaurantId: UUID | None = None
    items: list[OrderItem]
    totalPrice: float = Field(ge=0)
    currency: str = "UZS"
    status: Literal["CREATED"]
    createdAt: datetime


class OrderStatusChangedData(BaseModel):
    orderId: UUID
    customerId: UUID
    courierId: UUID | None = None
    oldStatus: Literal[
        "CREATED", "CONFIRMED", "PREPARING", "READY", "DELIVERING", "DELIVERED", "CANCELLED"
    ]
    newStatus: Literal[
        "CREATED", "CONFIRMED", "PREPARING", "READY", "DELIVERING", "DELIVERED", "CANCELLED"
    ]
    changedAt: datetime

    @field_validator("newStatus")
    @classmethod
    def validate_transition(cls, new_status: str, info):
        """Enforce the status machine described in the contract.
        Disallowed transitions (e.g. DELIVERED -> PREPARING) are rejected here,
        before anything reaches RabbitMQ or your app.
        """
        old_status = info.data.get("oldStatus")
        if old_status is None:
            return new_status

        allowed = {
            "CREATED": {"CONFIRMED", "CANCELLED"},
            "CONFIRMED": {"PREPARING", "CANCELLED"},
            "PREPARING": {"READY"},
            "READY": {"DELIVERING"},
            "DELIVERING": {"DELIVERED"},
            "DELIVERED": set(),
            "CANCELLED": set(),
        }

        if new_status not in allowed.get(old_status, set()):
            raise ValueError(
                f"Invalid status transition: {old_status} -> {new_status} is not allowed"
            )
        return new_status


# ---------------------------------------------------------------------------
# Envelopes (top-level event shape)
# ---------------------------------------------------------------------------

class OrderCreatedEvent(BaseModel):
    eventId: UUID
    eventType: Literal["order.created"]
    occurredAt: datetime
    version: int
    data: OrderCreatedData


class OrderStatusChangedEvent(BaseModel):
    eventId: UUID
    eventType: Literal["order.status_changed"]
    occurredAt: datetime
    version: int
    data: OrderStatusChangedData