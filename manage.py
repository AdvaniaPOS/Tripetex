from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json

from src.db import db_session, init_db
from src.models import JobRun, Tenant
from src.sync_service import (
    get_sendable_orders_for_tenant,
    retry_failed_orders_for_tenant,
    run_manual_sync_for_tenant,
    sync_paid_orders_to_tripletex_for_tenant,
)


def cmd_init_db(_: argparse.Namespace) -> None:
    init_db()
    print("Database schema initialized.")


def cmd_add_tenant(args: argparse.Namespace) -> None:
    with db_session() as session:
        existing = session.query(Tenant).filter(Tenant.tenant_key == args.tenant_key).first()
        if existing is not None:
            print(f"Tenant already exists: id={existing.id} key={existing.tenant_key}")
            return

        tenant = Tenant(tenant_key=args.tenant_key, name=args.name, active=not args.inactive)
        session.add(tenant)
        session.commit()
        session.refresh(tenant)

    print(f"Tenant created: id={tenant.id} key={tenant.tenant_key} name={tenant.name}")


def cmd_list_tenants(_: argparse.Namespace) -> None:
    with db_session() as session:
        tenants = session.query(Tenant).order_by(Tenant.id.asc()).all()

    if not tenants:
        print("No tenants found.")
        return

    for row in tenants:
        print(f"id={row.id} key={row.tenant_key} name={row.name} active={row.active}")


def cmd_seed_job_run(args: argparse.Namespace) -> None:
    with db_session() as session:
        tenant = session.query(Tenant).filter(Tenant.tenant_key == args.tenant_key).first()
        if tenant is None:
            print(f"Tenant not found: {args.tenant_key}")
            return

        run = JobRun(
            tenant_id=tenant.id,
            job_name=args.job_name,
            status=args.status,
            started_at=datetime.now(UTC),
            finished_at=datetime.now(UTC) if args.status != "RUNNING" else None,
            message=args.message,
        )
        session.add(run)
        session.commit()
        session.refresh(run)

    print(f"Job run inserted: id={run.id} tenant_id={run.tenant_id} status={run.status}")


def cmd_manual_sync(args: argparse.Namespace) -> None:
    dry_run = False if args.execute else args.dry_run
    result = run_manual_sync_for_tenant(args.tenant_key, dry_run=dry_run, limit=args.limit)
    print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_retry_failed(args: argparse.Namespace) -> None:
    result = retry_failed_orders_for_tenant(args.tenant_key, limit=args.limit)
    print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_sync_paid(args: argparse.Namespace) -> None:
    result = sync_paid_orders_to_tripletex_for_tenant(
        args.tenant_key,
        limit=args.limit,
        payment_type_id=args.payment_type_id,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_list_sendable(args: argparse.Namespace) -> None:
    result = get_sendable_orders_for_tenant(args.tenant_key, limit=args.limit)
    print(json.dumps(result, indent=2, ensure_ascii=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local admin commands for TT-Susoft sync service")
    sub = parser.add_subparsers(required=True)

    init_db_parser = sub.add_parser("init-db", help="Initialize DB schema")
    init_db_parser.set_defaults(func=cmd_init_db)

    add_tenant_parser = sub.add_parser("add-tenant", help="Add a tenant")
    add_tenant_parser.add_argument("--tenant-key", required=True)
    add_tenant_parser.add_argument("--name", required=True)
    add_tenant_parser.add_argument("--inactive", action="store_true")
    add_tenant_parser.set_defaults(func=cmd_add_tenant)

    list_tenants_parser = sub.add_parser("list-tenants", help="List tenants")
    list_tenants_parser.set_defaults(func=cmd_list_tenants)

    seed_run_parser = sub.add_parser("seed-job-run", help="Insert a sample job run")
    seed_run_parser.add_argument("--tenant-key", required=True)
    seed_run_parser.add_argument("--job-name", default="manual_sync")
    seed_run_parser.add_argument("--status", choices=["RUNNING", "SUCCESS", "FAILED"], default="SUCCESS")
    seed_run_parser.add_argument("--message", default=None)
    seed_run_parser.set_defaults(func=cmd_seed_job_run)

    manual_sync_parser = sub.add_parser("manual-sync", help="Run manual tenant sync")
    manual_sync_parser.add_argument("--tenant-key", required=True)
    manual_sync_parser.add_argument("--limit", type=int, default=50)
    mode_group = manual_sync_parser.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true")
    mode_group.add_argument("--execute", action="store_true")
    manual_sync_parser.set_defaults(dry_run=True)
    manual_sync_parser.set_defaults(func=cmd_manual_sync)

    retry_failed_parser = sub.add_parser("retry-failed", help="Retry failed orders for tenant")
    retry_failed_parser.add_argument("--tenant-key", required=True)
    retry_failed_parser.add_argument("--limit", type=int, default=50)
    retry_failed_parser.set_defaults(func=cmd_retry_failed)

    sync_paid_parser = sub.add_parser("sync-paid", help="Sync paid orders from Susoft to Tripletex")
    sync_paid_parser.add_argument("--tenant-key", required=True)
    sync_paid_parser.add_argument("--limit", type=int, default=50)
    sync_paid_parser.add_argument("--payment-type-id", type=int, default=20756819)
    sync_paid_parser.set_defaults(func=cmd_sync_paid)

    sendable_parser = sub.add_parser("list-sendable", help="List orders that are effectively sendable now")
    sendable_parser.add_argument("--tenant-key", required=True)
    sendable_parser.add_argument("--limit", type=int, default=50)
    sendable_parser.set_defaults(func=cmd_list_sendable)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
