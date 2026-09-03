import json
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _now() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


class OrdersRepository:
    """Persistence boundary for the Orders table, scoped to what the DLQ handler needs."""

    def __init__(self, table):
        """Wrap the given DynamoDB Table resource."""
        self._table = table

    def mark_dead_letter(self, order_id: str) -> None:
        """Mark an order as permanently stuck after exhausting all delivery attempts."""
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET #status = :dead_letter, updated_at = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":dead_letter": "dead_letter", ":now": _now()},
        )


class StructuredLogger:
    """Emits the order_id/function/outcome JSON shape CloudWatch queries expect."""

    def __init__(self, target: logging.Logger, function_name: str):
        """Wrap a stdlib Logger, tagging every line with the given function name."""
        self._target = target
        self._function_name = function_name

    def info(self, order_id, outcome, **extra) -> None:
        """Log an info-level outcome for the given order."""
        self._target.info(self._payload(order_id, outcome, extra))

    def _payload(self, order_id, outcome, extra) -> str:
        """Build the structured JSON log line."""
        return json.dumps({"order_id": order_id, "function": self._function_name, "outcome": outcome, **extra})


class DlqHandler:
    """Marks orders whose messages were exhausted and dead-lettered, for visibility and alerting."""

    def __init__(self, orders: OrdersRepository, log: StructuredLogger):
        """Wire up the collaborators this handler will use."""
        self._orders = orders
        self._log = log

    def handle(self, event: dict) -> None:
        """Process every dead-lettered order message in this batch."""
        for record in event.get("Records", []):
            self._handle_record(record)

    def _handle_record(self, record: dict) -> None:
        """Mark a single dead-lettered order and log it for alerting."""
        order_id = json.loads(record["body"])["order_id"]
        self._orders.mark_dead_letter(order_id)
        self._log.info(order_id, "dead_lettered")


def _build_handler() -> DlqHandler:
    """Wire up the real DynamoDB table and logger into a DlqHandler."""
    table = boto3.resource("dynamodb").Table(os.environ["ORDERS_TABLE_NAME"])
    return DlqHandler(OrdersRepository(table), StructuredLogger(logger, "dlq_handler"))


_handler = _build_handler()


def lambda_handler(event, context):
    """Lambda entry point: delegate to the module's DlqHandler instance."""
    return _handler.handle(event)
