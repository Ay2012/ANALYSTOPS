from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg

from analystops.gold import GoldLoadError, load_gold


TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL is not configured")
class GoldLayerTests(unittest.TestCase):
    def test_manifest_load_builds_reconciled_sales_views_atomically(self) -> None:
        client_a, client_b = uuid4(), uuid4()
        self.addCleanup(_cleanup, client_a, client_b)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_id = "a" * 64
            canonical = root / "silver.jsonl"
            profile = root / "profile.json"
            manifest = root / "manifest.json"
            canonical_rows = [
                {
                    "invoice_id": "INV-1",
                    "product_id": "SKU-1",
                    "product_description": "Widget",
                    "quantity": 2,
                    "transaction_timestamp": "2026-01-02T03:04:05",
                    "unit_price": "3.50",
                    "customer_id": "C-1",
                    "country": "Canada",
                    "source_file_id": source_id,
                    "source_row_number": 2,
                },
                {
                    "invoice_id": "C-2",
                    "product_id": "SKU-1",
                    "product_description": "Widget return",
                    "quantity": -1,
                    "transaction_timestamp": "2026-01-03T03:04:05",
                    "unit_price": "3.50",
                    "customer_id": "C-1",
                    "country": "Canada",
                    "source_file_id": source_id,
                    "source_row_number": 3,
                },
            ]
            canonical.write_text(
                "".join(json.dumps(row) + "\n" for row in canonical_rows)
            )
            profile.write_text(
                json.dumps(
                    {
                        "source_file_id": source_id,
                        "canonical_path": str(canonical.resolve()),
                        "reporting_month": "2026-01",
                        "country": "Canada",
                        "row_count": 2,
                        "publication_state": "PUBLISHABLE",
                        "metrics": {
                            "net_revenue": "3.50",
                            "gross_sales": "7.00",
                        },
                    }
                )
            )
            entry = {
                "source_file_id": source_id,
                "canonical_path": str(canonical),
                "profile_path": str(profile),
                "canonical_sha256": _hash(canonical),
                "profile_sha256": _hash(profile),
                "reporting_month": "2026-01",
                "country": "Canada",
                "row_count": 2,
            }
            manifest.write_text(json.dumps({"entries": [entry]}))

            first = _load(client_a, manifest)
            second = _load(client_a, manifest)
            _load(client_b, manifest)

            with psycopg.connect(TEST_DATABASE_URL) as connection:
                facts = connection.execute(
                    """
                    SELECT source_row_number FROM analystops.fact_sales_line
                    WHERE client_id = %s ORDER BY source_row_number
                    """,
                    (client_a,),
                ).fetchall()
                orders = connection.execute(
                    """
                    SELECT invoice_id, net_revenue, return_lines
                    FROM analystops.gold_order WHERE client_id = %s
                    ORDER BY invoice_id
                    """,
                    (client_a,),
                ).fetchall()
                product = connection.execute(
                    """
                    SELECT net_revenue, units_sold, units_returned, order_count,
                           customer_count
                    FROM analystops.gold_product_monthly WHERE client_id = %s
                    """,
                    (client_a,),
                ).fetchone()
                customer = connection.execute(
                    """
                    SELECT net_revenue, order_count, product_count, last_order_date
                    FROM analystops.gold_customer_monthly WHERE client_id = %s
                    """,
                    (client_a,),
                ).fetchone()

            self.assertEqual(facts, [(2,), (3,)])
            self.assertEqual(
                orders,
                [("C-2", Decimal("-3.50"), 1), ("INV-1", Decimal("7.00"), 0)],
            )
            self.assertEqual(product, (Decimal("3.50"), 2, 1, 2, 1))
            self.assertEqual(customer, (Decimal("3.50"), 2, 1, date(2026, 1, 3)))
            self.assertEqual(first["database_backend"], "postgresql")
            self.assertEqual(first["changed_items"], 1)
            self.assertEqual(second["changed_items"], 0)

            with psycopg.connect(TEST_DATABASE_URL) as connection:
                with connection.transaction():
                    connection.execute("SET LOCAL ROLE analystops_app")
                    connection.execute(
                        "SELECT set_config('app.client_id', %s, true)",
                        (str(client_a),),
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT DISTINCT client_id FROM analystops.fact_sales_line"
                        ).fetchall(),
                        [(client_a,)],
                    )

            canonical_rows[0]["quantity"] = "invalid"
            canonical.write_text(
                "".join(json.dumps(row) + "\n" for row in canonical_rows)
            )
            entry["canonical_sha256"] = _hash(canonical)
            manifest.write_text(json.dumps({"entries": [entry]}))
            with self.assertRaisesRegex(GoldLoadError, "Invalid quantity"):
                _load(client_a, manifest)

            with psycopg.connect(TEST_DATABASE_URL) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM analystops.fact_sales_line "
                        "WHERE client_id = %s",
                        (client_a,),
                    ).fetchone()[0],
                    2,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT status FROM analystops.gold_run "
                        "WHERE client_id = %s ORDER BY started_at",
                        (client_a,),
                    ).fetchall(),
                    [("SUCCESS",), ("SUCCESS",), ("FAILED",)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT run_id::text, changed_items, current_row_count "
                        "FROM analystops.gold_status WHERE client_id = %s",
                        (client_a,),
                    ).fetchone(),
                    (second["run_id"], 0, 2),
                )


def _load(client_id: object, manifest: Path) -> dict[str, object]:
    return load_gold(
        client_id,
        client_name="Gold Test",
        manifest_path=manifest,
        database_url=str(TEST_DATABASE_URL),
    )


def _cleanup(*client_ids: object) -> None:
    if not TEST_DATABASE_URL:
        return
    with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as connection:
        for table in ("fact_sales_line", "gold_input_item", "gold_run"):
            connection.execute(
                f"DELETE FROM analystops.{table} WHERE client_id = ANY(%s)",
                (list(client_ids),),
            )
        connection.execute(
            "DELETE FROM analystops.clients WHERE id = ANY(%s)",
            (list(client_ids),),
        )


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    unittest.main()
