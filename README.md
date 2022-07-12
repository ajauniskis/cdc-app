# Prerequisites
Make sure you have [Docker](https://docs.docker.com/get-docker/) running and [Poetry](https://python-poetry.org/docs/#osx--linux--bashonwindows-install-instructions) installed, ports `8080` and `5432` free.

# Commands
To setup python venv, install [astro cli](https://docs.astronomer.io/software/install-cli) and all dependencies run:
```sh
$ make install
```
To start the compose:
```sh
$ make start
```
And finally, to stop the compose:
```sh
$ make stop
```

# Usage
The compose consists of `airflow` stack and `init` container, which populates the database with initial data.
To run the code, go to http://localhost:8080/home (admin:admin) and trigger the dag manualy (make sure it is unpaused) or alternatively run the following command from `./airflow` directory:
```sh
$ astro dev run dags trigger main
```
You can find the data in psql database `postgresql://postgres:postgres@postgres:5432/postgres` or in this [file](result.csv).
