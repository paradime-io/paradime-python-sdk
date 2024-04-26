lint:
	black .
	isort .
	ruff check . --fix

verify:
	black --check .
	isort --check-only .
	mypy . --exclude dist
	ruff check .