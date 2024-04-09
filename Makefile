lint:
	black .
	isort .
	mypy .
	ruff check . --fix

verify:
	black --check .
	isort --check-only .
	mypy .
	ruff check .