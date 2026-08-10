"""RabbitMQ disposition for permanently malformed dispatch deliveries."""

from __future__ import annotations

from aio_pika.abc import AbstractIncomingMessage


async def reject_permanently_malformed(
    delivery: AbstractIncomingMessage,
) -> None:
    """Reject once without requeue so configured dead-lettering can quarantine."""
    await delivery.reject(requeue=False)
