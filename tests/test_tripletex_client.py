from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from src.tripletex_client import create_event_subscription, list_event_subscriptions


class TripletexClientTests(unittest.TestCase):
    @patch("src.tripletex_client.requests.get")
    def test_list_event_subscriptions_returns_values(self, mock_get: Mock) -> None:
        response = Mock()
        response.ok = True
        response.json.return_value = {"values": [{"id": 1, "event": "order.create", "status": "ACTIVE"}]}
        mock_get.return_value = response

        result = list_event_subscriptions("session-token")

        self.assertEqual(result, [{"id": 1, "event": "order.create", "status": "ACTIVE"}])
        mock_get.assert_called_once()

    @patch("src.tripletex_client.requests.post")
    def test_create_event_subscription_returns_payload(self, mock_post: Mock) -> None:
        response = Mock()
        response.ok = True
        response.json.return_value = {"id": 99, "event": "order.create", "status": "ACTIVE"}
        mock_post.return_value = response

        result = create_event_subscription(
            "session-token",
            event="order.create",
            target_url="https://example.test/webhooks/tripletex/order",
            auth_header_name="X-Webhook-Secret",
            auth_header_value="secret",
        )

        self.assertEqual(result["id"], 99)
        self.assertEqual(result["event"], "order.create")
        mock_post.assert_called_once()
