"""
Publishes validated events to RabbitMQ.

This connects to RabbitMQ on 127.0.0.1 only - the same broker your
consumer.py already reads from. RabbitMQ's port is never opened to
the internet; only this gateway process talks to it, over localhost.
"""

import json
import logging
import os

import aio_pika
from aio_pika import ExchangeType

logger = logging.getLogger("EventGatewayPublisher")

RABBITMQ_URL = os.environ["RABBITMQ_URL"]  # e.g. amqp://gateway_user:PASS@127.0.0.1:5672/foodexpress
EXCHANGE_NAME = "foodexpress"

_connection: aio_pika.RobustConnection | None = None
_channel: aio_pika.Channel | None = None
_exchange: aio_pika.Exchange | None = None


async def get_exchange() -> aio_pika.Exchange:
    """Lazily creates (and reuses) a single robust connection/channel/exchange.

    Reusing the connection avoids reconnecting to RabbitMQ on every request,
    which would be slow and wasteful under load.
    """
    global _connection, _channel, _exchange

    if _exchange is not None and not _connection.is_closed:
        return _exchange

    logger.info("Connecting to RabbitMQ for the event gateway publisher...")
    _connection = await aio_pika.connect_robust(RABBITMQ_URL, timeout=5)
    _channel = await _connection.channel()
    _exchange = await _channel.declare_exchange(
        EXCHANGE_NAME,
        type=ExchangeType.TOPIC,
        durable=True,
    )
    logger.info("Event gateway publisher connected and exchange declared.")
    return _exchange


async def publish_event(routing_key: str, payload: dict) -> None:
    """Publish a validated event dict to the foodexpress exchange.

    routing_key must match the event name exactly, e.g. "order.created",
    per the contract. The payload is the already-validated event,
    serialized back to JSON (datetimes/UUIDs as strings).
    """
    exchange = await get_exchange()

    body = json.dumps(payload, default=str).encode("utf-8")

    message = aio_pika.Message(
        body=body,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    await exchange.publish(message, routing_key=routing_key)
    logger.info(f"Published event '{routing_key}' (eventId={payload.get('eventId')})")


async def close_connection() -> None:
    """Call this on FastAPI shutdown to close the RabbitMQ connection cleanly."""
    if _connection is not None and not _connection.is_closed:
        await _connection.close()
        logger.info("Event gateway publisher connection closed.")