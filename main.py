import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv()  # reads .env into os.environ before anything else needs it

from fastapi import Depends, FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.requests import Request
from fastapi.encoders import jsonable_encoder

from auth import verify_api_key
from publisher import close_connection, publish_event
from schemas import OrderCreatedEvent, OrderStatusChangedEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EventGateway")

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_connection()


app = FastAPI(title="FoodExpress Event Gateway", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Rejected malformed event from {request.client.host}: {exc}")
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(exc.errors())})


@app.post("/v1/events/order-created", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def receive_order_created(request: Request, event: OrderCreatedEvent):
    """Receives an order.created event, validates it, publishes to RabbitMQ."""
    payload = event.model_dump(mode="json")
    
    # Debug print statement
    print(f"\n--- [DEBUG] Incoming order-created payload ---\n{payload}\n")
    
    await publish_event(routing_key="order.created", payload=payload)
    return {"status": "accepted", "eventId": str(event.eventId)}


@app.post("/v1/events/order-status-changed", dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def receive_order_status_changed(request: Request, event: OrderStatusChangedEvent):
    """Receives an order.status_changed event, validates it, publishes to RabbitMQ."""
    payload = event.model_dump(mode="json")
    
    # Debug print statement
    print(f"\n--- [DEBUG] Incoming order-status-changed payload ---\n{payload}\n")
    
    await publish_event(routing_key="order.status_changed", payload=payload)
    return {"status": "accepted", "eventId": str(event.eventId)}


@app.get("/health")
async def health():
    """Plain health check - no API key required, useful for uptime monitoring."""
    return {"status": "ok"}