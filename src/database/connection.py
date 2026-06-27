import os

from src.utils.config import DEFAULT_POSTGRES_HOST
from src.utils.config import DEFAULT_POSTGRES_PORT
from src.utils.config import DEFAULT_POSTGRES_DATABASE
from src.utils.config import DEFAULT_POSTGRES_USER
from src.utils.config import DEFAULT_POSTGRES_PASSWORD


def get_postgres_config():
    database_name = os.getenv("POSTGRES_DATABASE")

    if database_name is None:
        database_name = os.getenv("POSTGRES_DB")

    if database_name is None:
        database_name = DEFAULT_POSTGRES_DATABASE

    config = {
        "host": os.getenv("POSTGRES_HOST", DEFAULT_POSTGRES_HOST),
        "port": os.getenv("POSTGRES_PORT", DEFAULT_POSTGRES_PORT),
        "database": database_name,
        "user": os.getenv("POSTGRES_USER", DEFAULT_POSTGRES_USER),
        "password": os.getenv("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
    }

    return config


def build_connection_arguments(config=None):
    if config is None:
        config = get_postgres_config()

    connection_arguments = {
        "host": config["host"],
        "port": config["port"],
        "dbname": config["database"],
        "user": config["user"],
        "password": config["password"],
    }

    return connection_arguments


def open_postgres_connection(config=None):
    import psycopg2

    connection_arguments = build_connection_arguments(config)
    connection = psycopg2.connect(**connection_arguments)

    return connection


def close_postgres_connection(connection):
    if connection is None:
        return

    connection.close()


def describe_postgres_connection(config=None):
    if config is None:
        config = get_postgres_config()

    description = (
        config["user"]
        + "@"
        + config["host"]
        + ":"
        + str(config["port"])
        + "/"
        + config["database"]
    )

    return description
