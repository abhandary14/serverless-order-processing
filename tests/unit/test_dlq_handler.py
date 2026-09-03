import json
from unittest.mock import MagicMock

import pytest

from conftest import load_lambda_module

app = load_lambda_module(
    "dlq_handler",
    env={"ORDERS_TABLE_NAME": "test-orders-table", "INVENTORY_TABLE_NAME": "test-inventory-table"},
)


def _event(order_id):
    """Build an SQS-shaped event with a single order_id message."""
    return {"Records": [{"body": json.dumps({"order_id": order_id})}]}


class TestOrdersRepository:
    """Tests for the DLQ handler's narrow view of the Orders table."""

    def test_mark_dead_letter_updates_status_and_clears_reservation_flag(self):
        """Marking dead-lettered updates status and clears any inventory reservation claim."""
        table = MagicMock()
        app.OrdersRepository(table).mark_dead_letter("order-1")

        table.update_item.assert_called_once()
        call = table.update_item.call_args.kwargs
        assert call["Key"] == {"order_id": "order-1"}
        assert call["ExpressionAttributeValues"][":dead_letter"] == "dead_letter"
        assert call["ExpressionAttributeValues"][":false"] is False


class TestInventoryRepository:
    """Tests for releasing previously reserved stock."""

    def test_release_increments_stock(self):
        """Releasing stock performs an increment update on the item's record."""
        table = MagicMock()
        app.InventoryRepository(table).release("sku-1", 2)

        table.update_item.assert_called_once()
        assert table.update_item.call_args.kwargs["Key"] == {"item_id": "sku-1"}


class TestDlqHandler:
    """Tests for the end-to-end DLQ message handling."""

    @pytest.fixture
    def orders(self):
        """A fake OrdersRepository whose order had inventory reserved."""
        orders = MagicMock()
        orders.get.return_value = {"order_id": "order-1", "item_id": "sku-1", "quantity": 2, "inventory_reserved": True}
        return orders

    @pytest.fixture
    def inventory(self):
        """A fake InventoryRepository."""
        return MagicMock()

    @pytest.fixture
    def handler(self, orders, inventory):
        """A DlqHandler wired up to the fake collaborators."""
        log = app.StructuredLogger(app.logger, "dlq_handler")
        return app.DlqHandler(orders, inventory, log)

    def test_marks_dead_letter_and_releases_reserved_inventory(self, handler, orders, inventory):
        """An order that had inventory reserved gets that stock released, then marked dead_letter."""
        handler.handle(_event("order-1"))

        inventory.release.assert_called_once_with("sku-1", 2)
        orders.mark_dead_letter.assert_called_once_with("order-1")

    def test_does_not_release_inventory_that_was_never_reserved(self, handler, orders, inventory):
        """An order that never reached inventory reservation (e.g. failed before it) releases nothing."""
        orders.get.return_value = {"order_id": "order-1", "item_id": "sku-1", "quantity": 2}

        handler.handle(_event("order-1"))

        inventory.release.assert_not_called()
        orders.mark_dead_letter.assert_called_once_with("order-1")
