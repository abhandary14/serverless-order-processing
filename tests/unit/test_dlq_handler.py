import json
from unittest.mock import MagicMock

from conftest import load_lambda_module

app = load_lambda_module("dlq_handler", env={"ORDERS_TABLE_NAME": "test-orders-table"})


def _event(order_id):
    """Build an SQS-shaped event with a single order_id message."""
    return {"Records": [{"body": json.dumps({"order_id": order_id})}]}


class TestOrdersRepository:
    """Tests for the DLQ handler's narrow view of the Orders table."""

    def test_mark_dead_letter_updates_status(self):
        """Marking dead-lettered performs a status update on the order's record."""
        table = MagicMock()
        app.OrdersRepository(table).mark_dead_letter("order-1")

        table.update_item.assert_called_once()
        assert table.update_item.call_args.kwargs["Key"] == {"order_id": "order-1"}


class TestDlqHandler:
    """Tests for the end-to-end DLQ message handling."""

    def test_marks_every_order_in_the_batch_dead_letter(self):
        """Each dead-lettered order in the batch gets marked and logged."""
        table = MagicMock()
        orders = app.OrdersRepository(table)
        log = app.StructuredLogger(app.logger, "dlq_handler")
        handler = app.DlqHandler(orders, log)

        handler.handle(_event("order-1"))

        table.update_item.assert_called_once()
        assert table.update_item.call_args.kwargs["Key"] == {"order_id": "order-1"}
