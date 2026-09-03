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

    def get(self, order_id: str) -> dict:
        """Fetch an order's current record."""
        return self._table.get_item(Key={"order_id": order_id})["Item"]

    def mark_dead_letter(self, order_id: str) -> None:
        """Mark an order as permanently stuck after exhausting all delivery attempts, clearing any inventory reservation claim."""
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET #status = :dead_letter, inventory_reserved = :false, updated_at = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":dead_letter": "dead_letter", ":false": False, ":now": _now()},
        )


class InventoryRepository:
    """Persistence boundary for the Inventory table, scoped to what the DLQ handler needs."""

    def __init__(self, table):
        """Wrap the given DynamoDB Table resource."""
        self._table = table

    def release(self, item_id: str, quantity: int) -> None:
        """Give back previously reserved stock for an order that will never be fulfilled."""
        self._table.update_item(
            Key={"item_id": item_id},
            UpdateExpression="SET stock_level = stock_level + :quantity",
            ExpressionAttributeValues={":quantity": quantity},
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
    """Marks orders whose messages were exhausted and dead-lettered, releasing any reserved stock."""

    def __init__(self, orders: OrdersRepository, inventory: InventoryRepository, log: StructuredLogger):
        """Wire up the collaborators this handler will use."""
        self._orders = orders
        self._inventory = inventory
        self._log = log

    def handle(self, event: dict) -> None:
        """Process every dead-lettered order message in this batch."""
        for record in event.get("Records", []):
            self._handle_record(record)

    def _handle_record(self, record: dict) -> None:
        """Mark a single dead-lettered order, release any reserved stock, and log it for alerting."""
        order_id = json.loads(record["body"])["order_id"]
        order = self._orders.get(order_id)

        if order.get("inventory_reserved"):
            self._inventory.release(order["item_id"], order["quantity"])

        self._orders.mark_dead_letter(order_id)
        self._log.info(order_id, "dead_lettered")


def _build_handler() -> DlqHandler:
    """Wire up the real DynamoDB tables and logger into a DlqHandler."""
    dynamodb = boto3.resource("dynamodb")
    orders = OrdersRepository(dynamodb.Table(os.environ["ORDERS_TABLE_NAME"]))
    inventory = InventoryRepository(dynamodb.Table(os.environ["INVENTORY_TABLE_NAME"]))
    return DlqHandler(orders, inventory, StructuredLogger(logger, "dlq_handler"))


_handler = _build_handler()


def lambda_handler(event, context):
    """Lambda entry point: delegate to the module's DlqHandler instance."""
    return _handler.handle(event)
