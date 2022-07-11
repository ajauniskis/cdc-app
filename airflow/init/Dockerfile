FROM python:3.10.4-bullseye
RUN mkdir /opt/app
WORKDIR /opt/app
COPY requirements.txt ./
RUN python3 -m pip install -r requirements.txt
RUN echo "Build done"
