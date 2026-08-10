"""RabbitMQ permanent-malformed disposition tests."""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock

from aio_pika.abc import AbstractIncomingMessage

from taskforge.broker.malformed import reject_permanently_malformed


def test_permanently_malformed_delivery_is_rejected_without_requeue() -> None:
    delivery = AsyncMock()

    asyncio.run(
        reject_permanently_malformed(cast(AbstractIncomingMessage, cast(Any, delivery)))
    )

    delivery.reject.assert_awaited_once_with(requeue=False)
