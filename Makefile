venv_dir = .venv
venv_activate = . $(venv_dir)/bin/activate


install: venv_setup astro
	poetry install

venv_setup:
	python3 -m venv $(venv_dir)

astro:
	curl -sSL install.astronomer.io | sudo bash -s

run:
	cd airflow; astro dev start

stop:
	cd airflow; astro dev stop
