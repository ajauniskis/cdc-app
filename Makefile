venv_dir = .venv
venv_activate = . $(venv_dir)/bin/activate

install: venv_setup
	poetry install

venv_setup:
	python3 -m venv $(venv_dir)

run:
	docker-compose up
