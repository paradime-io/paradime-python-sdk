lint:
	black .
	isort .
	ruff . --fix
	mypy .

verify:
	black --check .
	isort --check-only .
	ruff check .
	mypy .