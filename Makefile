export UV_ENV_FILE := .env

.PHONY: create-venv remove-venv sync reinstall-project format

create-venv:
	uv venv --python 3.13

remove-venv:
	rm -rf .venv

sync:
	uv sync

reinstall-project: remove-venv create-venv sync

format:
	uv run ruff format .