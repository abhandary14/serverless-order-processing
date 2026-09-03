import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"


class InsufficientStockError(Exception):
    """Raised when an item doesn't have enough stock to fulfill an order."""


class PaymentError(Exception):
    """Raised when the mock payment gateway simulates a failure."""


# Fast, in-process retries only, for transient failures (mock payment failure, DynamoDB
# throttling) within a single invocation. 3 attempts with backoff up to 4s between them
# maxes out at a few seconds total - well under the 60s SQS visibility timeout on
# OrdersQueue, so tenacity always finishes long before SQS could redeliver. Giving up
# permanently is SQS's job (maxReceiveCount + the DLQ), not tenacity's - see PROJECT.md's
# retry strategy section.
_transient_retry = retry(
    retry=retry_if_exception_type((PaymentError, ClientError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    reraise=True,
)


def _now() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


class OrdersRepository:
    """Persistence boundary for the Orders table, including its idempotency markers."""

    def __init__(self, table):
        """Wrap the given DynamoDB Table resource."""
        self._table = table

    def get(self, order_id: str) -> dict:
        """Fetch an order's current record."""
        return self._table.get_item(Key={"order_id": order_id})["Item"]

    def begin_processing(self, order_id: str) -> bool:
        """Conditionally flip status received -> processing. Returns False if another invocation already owns it, or it's already terminal."""
        try:
            self._table.update_item(
                Key={"order_id": order_id},
                UpdateExpression="SET #status = :processing, updated_at = :now",
                ConditionExpression="#status = :received",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":processing": "processing", ":received": "received", ":now": _now()},
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == CONDITIONAL_CHECK_FAILED:
                return False
            raise

    def revert_to_received(self, order_id: str) -> None:
        """Undo begin_processing after a failure, so a later redelivery can retry this order."""
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET #status = :received, updated_at = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":received": "received", ":now": _now()},
        )

    def mark_inventory_reserved(self, order_id: str) -> None:
        """Record that stock has already been decremented for this order, so a retry won't double-decrement it."""
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET inventory_reserved = :true, updated_at = :now",
            ExpressionAttributeValues={":true": True, ":now": _now()},
        )

    def record_payment(self, order_id: str, payment_id: str) -> None:
        """Record a successful payment immediately, so a retry won't charge twice."""
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET payment_id = :payment_id, updated_at = :now",
            ExpressionAttributeValues={":payment_id": payment_id, ":now": _now()},
        )

    def mark_processed(self, order_id: str) -> None:
        """Mark an order as successfully processed."""
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET #status = :processed, updated_at = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":processed": "processed", ":now": _now()},
        )

    def mark_failed(self, order_id: str) -> None:
        """Mark an order as permanently failed (e.g. insufficient stock)."""
        self._table.update_item(
            Key={"order_id": order_id},
            UpdateExpression="SET #status = :failed, updated_at = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":failed": "failed", ":now": _now()},
        )


class InventoryRepository:
    """Persistence boundary for the Inventory table."""

    def __init__(self, table):
        """Wrap the given DynamoDB Table resource."""
        self._table = table

    def reserve(self, item_id: str, quantity: int) -> None:
        """Atomically decrement stock, or raise InsufficientStockError if there isn't enough."""
        try:
            self._table.update_item(
                Key={"item_id": item_id},
                UpdateExpression="SET stock_level = stock_level - :quantity",
                ConditionExpression="attribute_exists(item_id) AND stock_level >= :quantity",
                ExpressionAttributeValues={":quantity": quantity},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == CONDITIONAL_CHECK_FAILED:
                raise InsufficientStockError(f"insufficient stock for item {item_id}") from e
            raise


class PaymentGateway:
    """A mock payment gateway with a configurable simulated failure rate, for chaos testing."""

    def __init__(self, failure_rate: float):
        """Set the probability (0.0-1.0) that a charge simulates a failure."""
        self._failure_rate = failure_rate

    def charge(self, order_id: str) -> str:
        """Simulate charging for an order, returning a payment id, or raising PaymentError."""
        if random.random() < self._failure_rate:
            raise PaymentError(f"simulated payment failure for order {order_id}")
        return str(uuid.uuid4())


class StructuredLogger:
    """Emits the order_id/function/outcome JSON shape CloudWatch queries expect."""

    def __init__(self, target: logging.Logger, function_name: str):
        """Wrap a stdlib Logger, tagging every line with the given function name."""
        self._target = target
        self._function_name = function_name

    def info(self, order_id, outcome, **extra) -> None:
        """Log an info-level outcome for the given order."""
        self._target.info(self._payload(order_id, outcome, extra))

    def exception(self, order_id, outcome, **extra) -> None:
        """Log an outcome for the given order, including the current exception's traceback."""
        self._target.exception(self._payload(order_id, outcome, extra))

    def _payload(self, order_id, outcome, extra) -> str:
        """Build the structured JSON log line shared by info and exception."""
        return json.dumps({"order_id": order_id, "function": self._function_name, "outcome": outcome, **extra})


class OrderProcessingHandler:
    """Checks inventory, charges payment, and updates order status for each queued order."""

    def __init__(self, orders: OrdersRepository, inventory: InventoryRepository, payment: PaymentGateway, log: StructuredLogger):
        """Wire up the collaborators this handler will use."""
        self._orders = orders
        self._inventory = inventory
        self._payment = payment
        self._log = log

    def handle(self, event: dict) -> None:
        """Process every order message in this SQS batch."""
        for record in event.get("Records", []):
            self._handle_record(record)

    def _handle_record(self, record: dict) -> None:
        """Process a single order message, enforcing the double-processing and idempotency protections."""
        order_id = json.loads(record["body"])["order_id"]

        if not self._orders.begin_processing(order_id):
            self._log.info(order_id, "skipped_in_progress_or_already_done")
            return

        try:
            self._process(order_id)
        except InsufficientStockError:
            self._orders.mark_failed(order_id)
            self._log.info(order_id, "failed_insufficient_stock")
        except Exception:
            self._orders.revert_to_received(order_id)
            self._log.exception(order_id, "processing_error_reverted_for_retry")
            raise

    @_transient_retry
    def _process(self, order_id: str) -> None:
        """Reserve inventory and charge payment, skipping any step already completed by a prior attempt."""
        order = self._orders.get(order_id)

        if order.get("payment_id"):
            self._orders.mark_processed(order_id)
            self._log.info(order_id, "already_paid_finalizing")
            return

        if not order.get("inventory_reserved"):
            self._inventory.reserve(order["item_id"], order["quantity"])
            self._orders.mark_inventory_reserved(order_id)

        payment_id = self._payment.charge(order_id)
        self._orders.record_payment(order_id, payment_id)
        self._orders.mark_processed(order_id)
        self._log.info(order_id, "processed")


def _build_handler() -> OrderProcessingHandler:
    """Wire up the real DynamoDB tables, payment gateway, and logger into an OrderProcessingHandler."""
    dynamodb = boto3.resource("dynamodb")
    orders = OrdersRepository(dynamodb.Table(os.environ["ORDERS_TABLE_NAME"]))
    inventory = InventoryRepository(dynamodb.Table(os.environ["INVENTORY_TABLE_NAME"]))
    payment = PaymentGateway(float(os.environ.get("PAYMENT_FAILURE_RATE", "0")))
    return OrderProcessingHandler(orders, inventory, payment, StructuredLogger(logger, "order_processing"))


_handler = _build_handler()


def lambda_handler(event, context):
    """Lambda entry point: delegate to the module's OrderProcessingHandler instance."""
    return _handler.handle(event)
