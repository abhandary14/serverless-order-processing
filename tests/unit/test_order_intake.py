import json
from unittest.mock import MagicMock

import pytest

from conftest import load_lambda_module

app = load_lambda_module("order_intake", env={"ORDERS_TABLE_NAME": "test-orders-table"})


def _event(body):
    return {"body": json.dumps(body)}


class TestOrderRequestValidation:
    def test_valid_payload(self):
        request = app.OrderRequest.from_payload(
            {"item_id": "sku-1", "quantity": 2, "customer_email": "a@example.com"}
        )
        assert request == app.OrderRequest(item_id="sku-1", quantity=2, customer_email="a@example.com")

    @pytest.mark.parametrize(
        "body",
        [
            {"quantity": 2, "customer_email": "a@example.com"},
            {"item_id": "sku-1", "customer_email": "a@example.com"},
            {"item_id": "sku-1", "quantity": 0, "customer_email": "a@example.com"},
            {"item_id": "sku-1", "quantity": -1, "customer_email": "a@example.com"},
            {"item_id": "sku-1", "quantity": "2", "customer_email": "a@example.com"},
            {"item_id": "sku-1", "quantity": 2, "customer_email": "not-an-email"},
            {"item_id": "", "quantity": 2, "customer_email": "a@example.com"},
        ],
    )
    def test_invalid_payload_raises(self, body):
        with pytest.raises(app.ValidationError):
            app.OrderRequest.from_payload(body)


class TestOrderIntakeHandler:
    @pytest.fixture
    def fake_table(self):
        return MagicMock()

    @pytest.fixture
    def handler(self, fake_table):
        repository = app.OrderRepository(fake_table)
        log = app.StructuredLogger(app.logger, "order_intake")
        return app.OrderIntakeHandler(repository, log)

    def test_valid_order_is_saved_and_returns_201(self, handler, fake_table):
        response = handler.handle(_event({"item_id": "sku-1", "quantity": 2, "customer_email": "a@example.com"}))

        assert response["statusCode"] == 201
        fake_table.put_item.assert_called_once()
        saved_item = fake_table.put_item.call_args.kwargs["Item"]
        assert saved_item["status"] == "received"
        assert saved_item["item_id"] == "sku-1"
        assert saved_item["quantity"] == 2

        body = json.loads(response["body"])
        assert body["status"] == "received"
        assert "order_id" in body

    def test_invalid_payload_returns_400_and_does_not_save(self, handler, fake_table):
        response = handler.handle(_event({"item_id": "sku-1"}))

        assert response["statusCode"] == 400
        fake_table.put_item.assert_not_called()

    def test_invalid_json_returns_400(self, handler, fake_table):
        response = handler.handle({"body": "{not-json"})

        assert response["statusCode"] == 400
        fake_table.put_item.assert_not_called()

    def test_repository_failure_returns_500(self, handler, fake_table):
        fake_table.put_item.side_effect = RuntimeError("boom")

        response = handler.handle(_event({"item_id": "sku-1", "quantity": 2, "customer_email": "a@example.com"}))

        assert response["statusCode"] == 500
