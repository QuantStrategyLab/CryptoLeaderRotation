from __future__ import annotations

import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = PROJECT_ROOT / ".github" / "workflows" / "monthly_publish.yml"


class MonthlyPublishWorkflowConfigTests(unittest.TestCase):
    def test_publish_targets_use_vars_only(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("GCP_PROJECT_ID: ${{ vars.GCP_PROJECT_ID }}", workflow)
        self.assertIn("GCS_BUCKET: ${{ vars.GCS_BUCKET }}", workflow)
        self.assertIn("credentials_json: ${{ secrets.GCP_SERVICE_ACCOUNT_KEY }}", workflow)
        self.assertIn("issues: write", workflow)
        self.assertNotIn("secrets.GCP_PROJECT_ID", workflow)
        self.assertNotIn("secrets.GCS_BUCKET", workflow)

    def test_monthly_review_issue_creation_does_not_require_gh_cli(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertNotIn("gh label create", workflow)
        self.assertNotIn("gh issue create", workflow)
        self.assertIn("https://api.github.com/repos/{repository}", workflow)
        self.assertIn('GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}', workflow)


if __name__ == "__main__":
    unittest.main()
