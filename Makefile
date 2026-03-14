.PHONY: monthly-shadow-build monthly-shadow-check

monthly-shadow-build:
	.venv/bin/python scripts/run_monthly_shadow_build.py

monthly-shadow-check:
	.venv/bin/python -m unittest tests.test_walkforward_validation tests.test_shadow_release_history
