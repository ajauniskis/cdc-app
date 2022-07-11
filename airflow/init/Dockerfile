FROM python:3.10.4-bullseye
RUN mkdir /opt/app
WORKDIR /opt/app
COPY poetry.lock pyproject.toml ./
RUN python3 -m pip install poetry
RUN poetry export -o requirements.txt --without-hashes
RUN python3 -m pip install -r requirements.txt
RUN echo "Build done"
