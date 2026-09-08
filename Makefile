.PHONY: install test test-integration lint run compose-up compose-down

install:
	poetry install

test:
	poetry run pytest -m "not integration"

test-integration:
	poetry run pytest -m integration

lint:
	poetry run ruff check src tests benchmarks

run:
	poetry run uvicorn dalmatian.api:app --reload --host 0.0.0.0 --port 8000

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v
