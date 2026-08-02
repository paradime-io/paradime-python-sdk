lint:
	black .
	isort .
	ruff check . --fix
	flake8 .

test:
	pytest tests

verify:
	black --check .
	isort --check-only .
	mypy . --exclude dist
	ruff check .
	flake8 .
	pytest tests