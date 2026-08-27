import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from conftest import load_lambda_module

app = load_lambda_module(
    "order_processing",
    env={"ORDERS_TABLE_NAME": "test-orders-table", "INVENTORY_TABLE_NAME": "test-inventory-table"},
)


def _conditional_check_failed():
    """Build the ClientError botocore raises for a failed ConditionExpression."""
    return ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "boom"}}, "UpdateItem")


def _event(order_id):
    """Build an SQS-shaped event with a single order_id message."""
    return {"Records": [{"body": json.dumps({"order_id": order_id})}]}


class TestOrdersRepositoryBeginProcessing:
    """Tests for the conditional received -> processing transition."""

    def test_returns_true_when_status_was_received(self):
        """A successful conditional update reports success."""
        table = MagicMock()
        repository = app.OrdersRepository(table)

        assert repository.begin_processing("order-1") is True
        table.update_item.assert_called_once()

    def test_returns_false_when_condition_fails(self):
        """A failed condition (already in progress or already terminal) reports failure, not an error."""
        table = MagicMock()
        table.update_item.side_effect = _conditional_check_failed()
        repository = app.OrdersRepository(table)

        assert repository.begin_processing("order-1") is False

    def test_reraises_other_errors(self):
        """A non-conditional error (e.g. throttling) is not swallowed."""
        table = MagicMock()
        table.update_item.side_effect = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "boom"}}, "UpdateItem"
        )
        repository = app.OrdersRepository(table)

        with pytest.raises(ClientError):
            repository.begin_processing("order-1")


class TestInventoryRepository:
    """Tests for the atomic stock decrement."""

    def test_reserve_decrements_stock(self):
        """A successful reservation just performs the conditional update."""
        table = MagicMock()
        app.InventoryRepository(table).reserve("sku-1", 2)
        table.update_item.assert_called_once()

    def test_reserve_raises_insufficient_stock_on_condition_failure(self):
        """Not enough stock raises InsufficientStockError, not a raw ClientError."""
        table = MagicMock()
        table.update_item.side_effect = _conditional_check_failed()

        with pytest.raises(app.InsufficientStockError):
            app.InventoryRepository(table).reserve("sku-1", 2)


class TestPaymentGateway:
    """Tests for the mock payment gateway's failure-injection hook."""

    def test_zero_failure_rate_always_succeeds(self):
        """With failure_rate 0, charge always returns a payment id."""
        gateway = app.PaymentGateway(failure_rate=0.0)
        assert gateway.charge("order-1")

    def test_full_failure_rate_always_raises(self):
        """With failure_rate 1, charge always raises PaymentError."""
        gateway = app.PaymentGateway(failure_rate=1.0)
        with pytest.raises(app.PaymentError):
            gateway.charge("order-1")


class TestOrderProcessingHandler:
    """Tests for the end-to-end per-order processing flow, against fake collaborators."""

    @pytest.fixture
    def orders(self):
        """A fake OrdersRepository. begin_processing succeeds by default; get returns a fresh order."""
        orders = MagicMock()
        orders.begin_processing.return_value = True
        orders.get.return_value = {"order_id": "order-1", "item_id": "sku-1", "quantity": 2}
        return orders

    @pytest.fixture
    def inventory(self):
        """A fake InventoryRepository."""
        return MagicMock()

    @pytest.fixture
    def payment(self):
        """A fake PaymentGateway that always succeeds by default."""
        payment = MagicMock()
        payment.charge.return_value = "payment-123"
        return payment

    @pytest.fixture
    def handler(self, orders, inventory, payment):
        """An OrderProcessingHandler wired up to the fake collaborators."""
        log = app.StructuredLogger(app.logger, "order_processing")
        return app.OrderProcessingHandler(orders, inventory, payment, log)

    def test_skips_when_begin_processing_fails(self, handler, orders, inventory, payment):
        """An order already in progress or already terminal is left untouched."""
        orders.begin_processing.return_value = False

        handler.handle(_event("order-1"))

        orders.get.assert_not_called()
        inventory.reserve.assert_not_called()
        payment.charge.assert_not_called()

    def test_happy_path_reserves_inventory_and_charges_payment(self, handler, orders, inventory, payment):
        """A fresh order reserves inventory, charges payment, and is marked processed."""
        handler.handle(_event("order-1"))

        inventory.reserve.assert_called_once_with("sku-1", 2)
        orders.mark_inventory_reserved.assert_called_once_with("order-1")
        payment.charge.assert_called_once_with("order-1")
        orders.record_payment.assert_called_once_with("order-1", "payment-123")
        orders.mark_processed.assert_called_once_with("order-1")
        orders.revert_to_received.assert_not_called()

    def test_already_paid_skips_inventory_and_payment(self, handler, orders, inventory, payment):
        """A redelivery that already has a payment_id skips straight to finalizing."""
        orders.get.return_value = {"order_id": "order-1", "item_id": "sku-1", "quantity": 2, "payment_id": "payment-123"}

        handler.handle(_event("order-1"))

        inventory.reserve.assert_not_called()
        payment.charge.assert_not_called()
        orders.mark_processed.assert_called_once_with("order-1")

    def test_already_reserved_inventory_still_charges_payment(self, handler, orders, inventory, payment):
        """A redelivery that already reserved inventory doesn't reserve it again, but still pays."""
        orders.get.return_value = {
            "order_id": "order-1",
            "item_id": "sku-1",
            "quantity": 2,
            "inventory_reserved": True,
        }

        handler.handle(_event("order-1"))

        inventory.reserve.assert_not_called()
        payment.charge.assert_called_once_with("order-1")
        orders.mark_processed.assert_called_once_with("order-1")

    def test_insufficient_stock_marks_failed_without_reraising(self, handler, orders, inventory, payment):
        """Insufficient stock is a terminal failure: mark failed, don't retry, don't raise."""
        inventory.reserve.side_effect = app.InsufficientStockError("no stock")

        handler.handle(_event("order-1"))

        orders.mark_failed.assert_called_once_with("order-1")
        orders.revert_to_received.assert_not_called()
        payment.charge.assert_not_called()

    def test_payment_failure_reverts_to_received_and_reraises(self, handler, orders, inventory, payment):
        """A payment failure reverts status to received (so a retry can proceed) and propagates for SQS to redeliver."""
        payment.charge.side_effect = app.PaymentError("simulated failure")

        with pytest.raises(app.PaymentError):
            handler.handle(_event("order-1"))

        orders.revert_to_received.assert_called_once_with("order-1")
        orders.mark_processed.assert_not_called()
        orders.mark_failed.assert_not_called()
