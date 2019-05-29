import os

from platform_sdk.domain.reader.orms.peewee import Peewee


DATABASE = {
    'database': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('POSTGRES_HOST'),
    'port': os.environ.get('POSTGRES_PORT', 5432),
}

SCHEMA = {
    'uri': os.environ.get('SCHEMA_URI', 'http://domain_schema/schema/'),
}

ORM = Peewee

BASE_URI = {
    1: '/reader/api/v1/'
}
