import os

from sqlalchemy.engine import Engine
from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import OperationalError

from utils.logger import logger


def create_db_engine(
    host: str | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> Engine:
    # try:
    #     HOST = host or os.environ["POSTGRES_HOST"]
    #     DATABASE = database or os.environ["POSTGRES_DB"]
    #     USER = user or os.environ["POSTGRES_USER"]
    #     PASSWORD = password or os.environ["POSTGRES_PASSWORD"]
    # except KeyError as e:
    #     logger.info(f"Could not get database credentials: {e}")
    #     raise KeyError(f"Could not get database credentials: {e}")

    engine = create_engine(f"postgresql://postgres:postgres@postgres:5432/postgres")

    try:
        conn = engine.connect()
        conn.execute("commit")
        conn.execute("create schema if not exists staging")
        conn.close()
    except OperationalError as e:
        logger.error(f"Failed to create database engine: {e}")
        raise e

    logger.info("Database engine created")
    return engine


engine = create_db_engine()

metadata = MetaData(schema="staging")
