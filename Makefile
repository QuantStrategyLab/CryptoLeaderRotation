.PHONY: monthly-shadow-build monthly-shadow-check monthly-build-telegram monthly-review-briefing release-status-summary monthly-report-bundle

monthly-shadow-build:
	.venv/bin/python scripts/run_monthly_shadow_build.py

monthly-shadow-check:
	.venv/bin/python -m unittest tests.test_walkforward_validation tests.test_shadow_release_history tests.test_monthly_build_telegram tests.test_monthly_review_briefing

monthly-build-telegram:
	.venv/bin/python scripts/run_monthly_build_telegram.py

monthly-review-briefing:
	.venv/bin/python scripts/run_monthly_review_briefing.py

release-status-summary:
	.venv/bin/python scripts/run_release_status_summary.py

monthly-report-bundle:
	.venv/bin/python scripts/run_release_status_summary.py
	.venv/bin/python scripts/run_monthly_review_briefing.py
	.venv/bin/python scripts/run_monthly_build_telegram.py --print-only --output-path data/output/monthly_telegram.txt
	.venv/bin/python scripts/run_monthly_report_bundle.py
