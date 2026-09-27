from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from analystops.agents.silver_remediation import PROPOSAL_SCHEMA_VERSION
from analystops.workflows.silver_remediation import (
    SilverRemediationError,
    document_hash,
    execute_silver_remediation,
)


class SilverRemediationExecutorTests(unittest.TestCase):
    def test_approval_cannot_be_reused_after_profile_changes(self) -> None:
        profile = {
            "publication_state": "REVIEW_REQUIRED",
            "source_file_id": "a" * 64,
            "bronze_record_path": "/not/reached.json",
            "row_count": 10,
        }
        proposal = {
            "schema_version": PROPOSAL_SCHEMA_VERSION,
            "run_id": "proposal-1",
            "source_file_id": profile["source_file_id"],
            "validation_profile_hash": document_hash(profile),
            "resolutions": [
                {
                    "finding_code": "reporting_month_mismatch",
                    "decision": "PROPOSE",
                    "action": "repartition_by_country_month",
                    "defer_reason": None,
                }
            ],
        }
        approval = {
            "decision": "APPROVE",
            "reviewed_by": "analyst@example.com",
            "reviewed_at": "2026-09-26T12:00:00+00:00",
            "proposal_run_id": proposal["run_id"],
            "proposal_hash": document_hash(proposal),
            "validation_profile_hash": document_hash(profile),
        }
        profile["row_count"] = 11

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(
                SilverRemediationError, "not bound to this validation profile"
            ):
                execute_silver_remediation(
                    profile, proposal, approval, output_dir=Path(tmpdir)
                )


if __name__ == "__main__":
    unittest.main()
