lint:
	black .
	isort .
	mypy .

verify:
	black --check .
	isort --check-only .
	mypy .