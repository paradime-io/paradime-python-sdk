lint:
	black .
	isort .
	ruff check . --fix
	flake8 .

verify:
	black --check .
	isort --check-only .
	mypy . --exclude dist
	ruff check .
	flake8 .