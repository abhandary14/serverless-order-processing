import json
import logging
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class ValidationError(Exception):
    """Raised when an incoming order payload fails validation."""


@dataclass(frozen=True)
class OrderRequest:
    item_id: str
    quantity: int
    customer_email: str

    @classmethod
    def from_payload(cls, body: dict) -> "OrderRequest":
        item_id = body.get("item_id")
        quantity = body.get("quantity")
        customer_email = body.get("customer_email")

        if not isinstance(item_id, str) or not item_id.strip():
            raise ValidationError("item_id is required and must be a non-empty string")
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
            raise ValidationError("quantity is required and must be a positive integer")
        if not isinstance(customer_email, str) or not EMAIL_RE.match(customer_email):
            raise ValidationError("customer_email is required and must be a valid email address")

        return cls(item_id=item_id, quantity=quantity, customer_email=customer_email)


@dataclass(frozen=True)
class Order:
    order_id: str
    item_id: str
    quantity: int
    customer_email: str
    status: str
    created_at: str
    updated_at: str

    @classmethod
    def new(cls, request: OrderRequest) -> "Order":
        now = datetime.now(timezone.utc).isoformat()
        return cls(
            order_id=str(uuid.uuid4()),
            item_id=request.item_id,
            quantity=request.quantity,
            customer_email=request.customer_email,
            status="received",
            created_at=now,
            updated_at=now,
        )

    def to_item(self) -> dict:
        return {
            "order_id": self.order_id,
            "item_id": self.item_id,
            "quantity": self.quantity,
            "customer_email": self.customer_email,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class OrderRepository:
    """Persistence boundary for the Orders table."""

    def __init__(self, table):
        self._table = table

    def save(self, order: Order) -> None:
        self._table.put_item(Item=order.to_item())


class StructuredLogger:
    """Emits the order_id/function/outcome JSON shape CloudWatch queries expect."""

    def __init__(self, target: logging.Logger, function_name: str):
        self._target = target
        self._function_name = function_name

    def info(self, order_id, outcome, **extra) -> None:
        self._target.info(self._payload(order_id, outcome, extra))

    def exception(self, order_id, outcome, **extra) -> None:
        self._target.exception(self._payload(order_id, outcome, extra))

    def _payload(self, order_id, outcome, extra) -> str:
        return json.dumps({"order_id": order_id, "function": self._function_name, "outcome": outcome, **extra})


class OrderIntakeHandler:
    """Validates an incoming order request and persists it to DynamoDB."""

    def __init__(self, repository: OrderRepository, log: StructuredLogger):
        self._repository = repository
        self._log = log

    def handle(self, event: dict) -> dict:
        order_id = None
        try:
            body = json.loads(event.get("body") or "{}")
            request = OrderRequest.from_payload(body)

            order = Order.new(request)
            self._repository.save(order)

            self._log.info(order.order_id, "received")
            return self._response(201, {"order_id": order.order_id, "status": order.status})

        except json.JSONDecodeError:
            self._log.info(order_id, "invalid_json")
            return self._response(400, {"error": "request body must be valid JSON"})

        except ValidationError as e:
            self._log.info(order_id, "validation_failed", error=str(e))
            return self._response(400, {"error": str(e)})

        except Exception:
            self._log.exception(order_id, "error")
            return self._response(500, {"error": "internal server error"})

    @staticmethod
    def _response(status_code: int, body: dict) -> dict:
        return {
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body),
        }


def _build_handler() -> OrderIntakeHandler:
    table = boto3.resource("dynamodb").Table(os.environ["ORDERS_TABLE_NAME"])
    return OrderIntakeHandler(OrderRepository(table), StructuredLogger(logger, "order_intake"))


_handler = _build_handler()


def lambda_handler(event, context):
    return _handler.handle(event)
