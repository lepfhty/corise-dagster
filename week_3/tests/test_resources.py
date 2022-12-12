import pickle

from dagster import build_init_resource_context, build_output_context

from workspaces.challenge.week_3_challenge import PostgresBlobIOManager, postgres_blob_manager
from workspaces.resources import Postgres, S3, Redis, postgres_resource, redis_resource, s3_resource


def test_s3_resource():
    resource = s3_resource(
        build_init_resource_context(
            config={
                "bucket": "test",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localhost:4566",
            }
        )
    )
    assert type(resource) is S3


def test_redis_resource():
    resource = redis_resource(
        build_init_resource_context(
            config={
                "host": "test",
                "port": 6379,
            }
        )
    )
    assert type(resource) is Redis


def test_postgres_resource():
    resource = postgres_resource(
        build_init_resource_context(
            config={
                'host': 'test',
                'user': 'test',
                'password': 'test',
                'database': 'test'
            }
        )
    )
    assert type(resource) is Postgres


def test_postgres_blob_manager():
    postgres = postgres_resource(
        build_init_resource_context(
            config={
                'host': 'test',
                'user': 'test',
                'password': 'test',
                'database': 'test'
            }
        )
    )
    manager = postgres_blob_manager(
        build_init_resource_context(
            resources={'database': postgres},
            config={
                'blob_table': 'blob',
                'data_table': 'data'
            }
        )
    )
    assert type(manager) is PostgresBlobIOManager
    #
    # data = [{'column_1': 1, 'column_2': 2, 'column_3': 3}]
    # manager.handle_output(
    #     context=build_output_context(step_key='step1', run_id='run1', name='result'),
    #     obj=data
    # )
    #
    # result = postgres.execute('select * from blob')
    # assert result.rowcount == 1
    # first = result.first()
    # assert first['id'] == 'step1/run1/result'
    # assert first['blob'] == pickle.dumps(data)
