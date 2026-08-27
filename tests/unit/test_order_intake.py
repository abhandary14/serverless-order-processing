import json
from unittest.mock import MagicMock

import pytest

from conftest import load_lambda_module

app = load_lambda_module(
    "order_intake",
    env={"ORDERS_TABLE_NAME": "test-orders-table", "ORDERS_QUEUE_URL": "https://example.com/fake-queue"},
)


def _event(body):
    """Build a Function URL-shaped event with a JSON-encoded body."""
    return {"body": json.dumps(body)}


class TestOrderRequestValidation:
    """Tests for OrderRequest.from_payload's validation rules."""

    def test_valid_payload(self):
        """A well-formed payload builds a matching OrderRequest."""
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
        """Missing, wrong-typed, or malformed fields all raise ValidationError."""
        with pytest.raises(app.ValidationError):
            app.OrderRequest.from_payload(body)


class TestOrderIntakeHandler:
    """Tests for OrderIntakeHandler.handle, against a fake DynamoDB table."""

    @pytest.fixture
    def fake_table(self):
        """A mock standing in for the DynamoDB table, so no real AWS call is made."""
        return MagicMock()

    @pytest.fixture
    def fake_sqs(self):
        """A mock standing in for the SQS client, so no real AWS call is made."""
        return MagicMock()

    @pytest.fixture
    def handler(self, fake_table, fake_sqs):
        """An OrderIntakeHandler wired up to the fake table and queue."""
        repository = app.OrderRepository(fake_table)
        queue = app.OrderQueue(fake_sqs, "https://example.com/fake-queue")
        log = app.StructuredLogger(app.logger, "order_intake")
        return app.OrderIntakeHandler(repository, queue, log)

    def test_valid_order_is_saved_and_returns_201(self, handler, fake_table, fake_sqs):
        """A valid order is saved, published to the queue, and returns a 201 with its order_id."""
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

        fake_sqs.send_message.assert_called_once()
        published_body = json.loads(fake_sqs.send_message.call_args.kwargs["MessageBody"])
        assert published_body["order_id"] == body["order_id"]

    def test_invalid_payload_returns_400_and_does_not_save(self, handler, fake_table, fake_sqs):
        """An invalid payload returns 400 and never reaches the table or queue."""
        response = handler.handle(_event({"item_id": "sku-1"}))

        assert response["statusCode"] == 400
        fake_table.put_item.assert_not_called()
        fake_sqs.send_message.assert_not_called()

    def test_invalid_json_returns_400(self, handler, fake_table, fake_sqs):
        """A body that isn't valid JSON returns 400 and never reaches the table or queue."""
        response = handler.handle({"body": "{not-json"})

        assert response["statusCode"] == 400
        fake_table.put_item.assert_not_called()
        fake_sqs.send_message.assert_not_called()

    def test_repository_failure_returns_500(self, handler, fake_table, fake_sqs):
        """An unexpected error saving the order returns 500 instead of propagating, and never publishes."""
        fake_table.put_item.side_effect = RuntimeError("boom")

        response = handler.handle(_event({"item_id": "sku-1", "quantity": 2, "customer_email": "a@example.com"}))

        assert response["statusCode"] == 500
        fake_sqs.send_message.assert_not_called()

    def test_publish_failure_returns_500(self, handler, fake_table, fake_sqs):
        """An unexpected error publishing to the queue returns 500 instead of propagating."""
        fake_sqs.send_message.side_effect = RuntimeError("boom")

        response = handler.handle(_event({"item_id": "sku-1", "quantity": 2, "customer_email": "a@example.com"}))

        assert response["statusCode"] == 500
        fake_table.put_item.assert_called_once()
