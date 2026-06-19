from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

# Configure test DB before importing app modules.
os.environ["DATABASE_URL"] = "sqlite+pysqlite:///./test_sync.db"

from src.db import db_session, engine, init_db  # noqa: E402
from src.models import Base, OrderSync, Tenant  # noqa: E402
from src.sync_service import (  # noqa: E402
    _build_susoft_order_payload,
    get_sendable_orders_for_tenant,
    process_susoft_payment_for_tenant,
    process_tripletex_order_by_id_for_tenant,
    process_tripletex_order_for_tenant,
    retry_failed_orders_for_tenant,
    run_manual_sync_for_tenant,
    sync_paid_orders_to_tripletex_for_tenant,
)


def sample_tripletex_order(order_id: int = 210270345) -> dict[str, object]:
    return {
        "id": order_id,
        "orderDate": "2026-06-19",
        "customer": {"name": "Jon Sigurdarson"},
        "orderLines": [
            {
                "description": "Susoft aPOS 16",
                "count": 1,
                "product": {"id": 69775678, "number": "10001", "name": "Susoft aPOS 16"},
                "unitPriceExcludingVatCurrency": 6990,
                "unitPriceIncludingVatCurrency": 8737.5,
                "amountExcludingVatCurrency": 6990,
                "amountIncludingVatCurrency": 8737.5,
                "vatType": {"percentage": 25},
                "discount": 0,
            }
        ],
    }


def sample_tripletex_order_m10_without_number(order_id: int = 210270425) -> dict[str, object]:
    return {
        "id": order_id,
        "orderDate": "2026-06-19",
        "customer": {"name": "Mikkel Gundersen"},
        "orderLines": [
            {
                "description": "",
                "count": 7.4,
                "product": {"id": 69775686, "number": "", "name": "Susoft M10"},
                "unitPriceExcludingVatCurrency": 3990,
                "unitPriceIncludingVatCurrency": 4987.5,
                "amountExcludingVatCurrency": 29526,
                "amountIncludingVatCurrency": 36907.5,
                "vatType": {"percentage": 25},
                "discount": 0,
            }
        ],
    }


class SyncServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Base.metadata.drop_all(bind=engine)
        init_db()

    def setUp(self) -> None:
        Base.metadata.drop_all(bind=engine)
        init_db()
        with db_session() as session:
            session.add(Tenant(tenant_key="demo-tenant", name="Demo Tenant", active=True))
            session.commit()

    def test_build_susoft_payload_maps_fields(self) -> None:
        mapped = _build_susoft_order_payload(sample_tripletex_order())
        self.assertEqual(mapped["externalRef"], "210270345")
        self.assertEqual(len(mapped["lines"]), 1)
        self.assertEqual(mapped["lines"][0]["lineTaxPercent"], 25.0)
        self.assertIn("salesPriceInclTax", mapped["lines"][0])
        self.assertIn("price", mapped["lines"][0])

    def test_build_susoft_payload_uses_fallback_product_mapping(self) -> None:
        mapped = _build_susoft_order_payload(sample_tripletex_order_m10_without_number())
        self.assertEqual(mapped["externalRef"], "210270425")
        self.assertEqual(mapped["lines"][0]["product"]["id"], "10002")

    @patch("src.sync_service.find_tripletex_order_by_id")
    @patch("src.sync_service.process_tripletex_order_for_tenant")
    def test_process_tripletex_order_by_id_for_tenant(self, mock_process: object, mock_find: object) -> None:
        mock_find.return_value = sample_tripletex_order(210270190)  # type: ignore[attr-defined]
        mock_process.return_value = {"ok": True}  # type: ignore[attr-defined]

        result = process_tripletex_order_by_id_for_tenant("demo-tenant", 210270190)
        self.assertEqual(result, {"ok": True})
        mock_find.assert_called_once()  # type: ignore[attr-defined]
        mock_process.assert_called_once()  # type: ignore[attr-defined]

    @patch("src.sync_service.register_payment")
    @patch("src.sync_service.create_invoice")
    @patch("src.sync_service.create_session_token")
    @patch("src.sync_service.build_basic_headers")
    @patch("src.sync_service.find_cart_by_uuid")
    @patch("src.sync_service.find_order_by_uuid")
    @patch("src.sync_service.susoft_authenticate")
    def test_process_susoft_payment_for_tenant(
        self,
        mock_susoft_auth: object,
        mock_find_order: object,
        mock_find_cart: object,
        mock_build_headers: object,
        mock_tt_token: object,
        mock_create_invoice: object,
        mock_register_payment: object,
    ) -> None:
        mock_susoft_auth.return_value = "susoft-token"  # type: ignore[attr-defined]
        mock_find_cart.return_value = None  # type: ignore[attr-defined]
        mock_find_order.return_value = {  # type: ignore[attr-defined]
            "uuid": "uuid-1",
            "alternativeId": "210270345",
            "payments": [{"amount": 8737.5}],
        }
        mock_tt_token.return_value = "tt-token"  # type: ignore[attr-defined]
        mock_build_headers.return_value = {"Authorization": "Basic X"}  # type: ignore[attr-defined]
        mock_create_invoice.return_value = 12345  # type: ignore[attr-defined]
        mock_register_payment.return_value = {"ok": True}  # type: ignore[attr-defined]

        with db_session() as session:
            tenant = session.query(Tenant).filter(Tenant.tenant_key == "demo-tenant").first()
            assert tenant is not None
            session.add(
                OrderSync(
                    tenant_id=tenant.id,
                    tripletex_order_id="210270345",
                    status="PUSHED_TO_SUSOFT",
                    susoft_uuid="uuid-1",
                    payload_json=json.dumps(sample_tripletex_order(210270345), ensure_ascii=False),
                )
            )
            session.commit()

        result = process_susoft_payment_for_tenant("demo-tenant", "uuid-1", payment_type_id=20756819)
        self.assertTrue(result["matched"])
        self.assertEqual(result["tripletex_order_id"], 210270345)
        self.assertEqual(result["status"], "TT_PAID")

    @patch("src.sync_service.fetch_open_orders")
    @patch("src.sync_service.create_session_token")
    def test_manual_sync_dry_run(self, mock_token: object, mock_fetch: object) -> None:
        mock_token.return_value = "session-token"  # type: ignore[attr-defined]
        mock_fetch.return_value = {"values": [sample_tripletex_order(210270190)]}  # type: ignore[attr-defined]

        result = run_manual_sync_for_tenant("demo-tenant", dry_run=True, limit=50)
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["discovered_orders"], 1)
        self.assertEqual(result["pushed_to_susoft"], 0)

        with db_session() as session:
            row = session.query(OrderSync).filter(OrderSync.tripletex_order_id == "210270190").first()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.status, "DISCOVERED")

    @patch("src.sync_service.create_susoft_order")
    @patch("src.sync_service.fetch_open_orders")
    @patch("src.sync_service.create_session_token")
    def test_manual_sync_execute_pushes_to_susoft(
        self,
        mock_token: object,
        mock_fetch: object,
        mock_create_order: object,
    ) -> None:
        mock_token.return_value = "session-token"  # type: ignore[attr-defined]
        mock_fetch.return_value = {"values": [sample_tripletex_order(210270191)]}  # type: ignore[attr-defined]
        mock_create_order.return_value = {"uuid": "susoft-uuid-1"}  # type: ignore[attr-defined]

        result = run_manual_sync_for_tenant("demo-tenant", dry_run=False, limit=50)
        self.assertEqual(result["pushed_to_susoft"], 1)
        self.assertIn(result["status"], {"SUCCESS", "PARTIAL_SUCCESS"})

        with db_session() as session:
            row = session.query(OrderSync).filter(OrderSync.tripletex_order_id == "210270191").first()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.status, "PUSHED_TO_SUSOFT")
            self.assertEqual(row.susoft_uuid, "susoft-uuid-1")

    @patch("src.sync_service.create_susoft_order")
    @patch("src.sync_service.fetch_open_orders")
    @patch("src.sync_service.create_session_token")
    def test_manual_sync_skips_already_handled_orders(
        self,
        mock_token: object,
        mock_fetch: object,
        mock_create_order: object,
    ) -> None:
        mock_token.return_value = "session-token"  # type: ignore[attr-defined]
        mock_fetch.return_value = {"values": [sample_tripletex_order(210270374)]}  # type: ignore[attr-defined]

        with db_session() as session:
            tenant = session.query(Tenant).filter(Tenant.tenant_key == "demo-tenant").first()
            assert tenant is not None
            session.add(
                OrderSync(
                    tenant_id=tenant.id,
                    tripletex_order_id="210270374",
                    status="PUSHED_TO_SUSOFT",
                    susoft_uuid="existing-uuid",
                    payload_json=json.dumps(sample_tripletex_order(210270374), ensure_ascii=False),
                )
            )
            session.commit()

        result = run_manual_sync_for_tenant("demo-tenant", dry_run=False, limit=50)
        self.assertEqual(result["pushed_to_susoft"], 0)
        mock_create_order.assert_not_called()  # type: ignore[attr-defined]

    @patch("src.sync_service.fetch_open_orders")
    @patch("src.sync_service.create_session_token")
    def test_get_sendable_orders_filters_out_already_handled(
        self,
        mock_token: object,
        mock_fetch: object,
    ) -> None:
        mock_token.return_value = "session-token"  # type: ignore[attr-defined]
        mock_fetch.return_value = {
            "values": [sample_tripletex_order(210270374), sample_tripletex_order(210270500)]
        }  # type: ignore[attr-defined]

        with db_session() as session:
            tenant = session.query(Tenant).filter(Tenant.tenant_key == "demo-tenant").first()
            assert tenant is not None
            session.add(
                OrderSync(
                    tenant_id=tenant.id,
                    tripletex_order_id="210270500",
                    status="PUSHED_TO_SUSOFT",
                    susoft_uuid="existing-uuid",
                    payload_json=json.dumps(sample_tripletex_order(210270500), ensure_ascii=False),
                )
            )
            session.commit()

        result = get_sendable_orders_for_tenant("demo-tenant", limit=50)
        self.assertEqual(result["sendable_count"], 1)
        self.assertEqual(result["already_handled_count"], 1)
        self.assertEqual(result["sendable_orders"][0]["tripletex_order_id"], "210270374")

    @patch("src.sync_service.create_susoft_order")
    def test_retry_failed_orders(self, mock_create_order: object) -> None:
        mock_create_order.return_value = {"uuid": "retry-uuid-1"}  # type: ignore[attr-defined]

        with db_session() as session:
            tenant = session.query(Tenant).filter(Tenant.tenant_key == "demo-tenant").first()
            assert tenant is not None
            session.add(
                OrderSync(
                    tenant_id=tenant.id,
                    tripletex_order_id="210270345",
                    status="FAILED",
                    payload_json=json.dumps(sample_tripletex_order(210270345), ensure_ascii=False),
                    last_error="previous error",
                )
            )
            session.commit()

        result = retry_failed_orders_for_tenant("demo-tenant", limit=20)
        self.assertEqual(result["retried"], 1)
        self.assertEqual(result["succeeded"], 1)
        self.assertEqual(result["failed"], 0)

        with db_session() as session:
            row = session.query(OrderSync).filter(OrderSync.tripletex_order_id == "210270345").first()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.status, "PUSHED_TO_SUSOFT")
            self.assertEqual(row.susoft_uuid, "retry-uuid-1")

    @patch("src.sync_service.register_payment")
    @patch("src.sync_service.create_invoice")
    @patch("src.sync_service.create_session_token")
    @patch("src.sync_service.build_basic_headers")
    @patch("src.sync_service.find_cart_by_uuid")
    @patch("src.sync_service.find_order_by_uuid")
    @patch("src.sync_service.susoft_authenticate")
    def test_sync_paid_orders_to_tripletex(
        self,
        mock_susoft_auth: object,
        mock_find_order: object,
        mock_find_cart: object,
        mock_build_headers: object,
        mock_tt_token: object,
        mock_create_invoice: object,
        mock_register_payment: object,
    ) -> None:
        mock_susoft_auth.return_value = "susoft-token"  # type: ignore[attr-defined]
        mock_find_cart.return_value = None  # type: ignore[attr-defined]
        mock_find_order.return_value = {  # type: ignore[attr-defined]
            "uuid": "uuid-1",
            "alternativeId": "210270345",
            "payments": [{"amount": 8737.5}],
        }
        mock_tt_token.return_value = "tt-token"  # type: ignore[attr-defined]
        mock_build_headers.return_value = {"Authorization": "Basic X"}  # type: ignore[attr-defined]
        mock_create_invoice.return_value = 12345  # type: ignore[attr-defined]
        mock_register_payment.return_value = {"ok": True}  # type: ignore[attr-defined]

        with db_session() as session:
            tenant = session.query(Tenant).filter(Tenant.tenant_key == "demo-tenant").first()
            assert tenant is not None
            session.add(
                OrderSync(
                    tenant_id=tenant.id,
                    tripletex_order_id="210270345",
                    status="PUSHED_TO_SUSOFT",
                    susoft_uuid="uuid-1",
                    payload_json=json.dumps(sample_tripletex_order(210270345), ensure_ascii=False),
                )
            )
            session.commit()

        result = sync_paid_orders_to_tripletex_for_tenant("demo-tenant", limit=20, payment_type_id=20756819)
        self.assertEqual(result["synced_to_tt"], 1)
        self.assertEqual(result["errors"], 0)

        with db_session() as session:
            row = session.query(OrderSync).filter(OrderSync.tripletex_order_id == "210270345").first()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.status, "TT_PAID")


if __name__ == "__main__":
    unittest.main()
