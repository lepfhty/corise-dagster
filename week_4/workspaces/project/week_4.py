import json
from typing import List

from dagster import Nothing, OpExecutionContext, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(config_schema={'s3_key': String},
       required_resource_keys={'s3'},
       group_name='corise')
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    key = context.op_config['s3_key']
    rows = context.resources.s3.get_data(key)
    return [Stock.from_list(row) for row in rows]


@asset(group_name='corise')
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    high_stock = max(get_s3_data, key=lambda s: s.high)
    return Aggregation(date=high_stock.date, high=high_stock.high)


@asset(required_resource_keys={'redis'},
       group_name='corise')
def put_redis_data(context: OpExecutionContext, process_data: Aggregation) -> None:
    context.resources.redis.put_data(name=str(process_data.date),
                                     value=process_data.json())


@asset(required_resource_keys={'s3'},
       group_name='corise')
def put_s3_data(context: OpExecutionContext, process_data: Aggregation) -> None:
    context.resources.s3.put_data(key_name=f'aggregation_{process_data.date}.json',
                                  data=process_data)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={'s3': s3_resource,
                   'redis': redis_resource},
    resource_config_by_key={
        's3': {
            'config': {
                'bucket': 'dagster',
                'access_key': 'test',
                'secret_key': 'test',
                'endpoint_url': 'http://localstack:4566'
            }
        },
        'redis': {
            'config': {
                'host': 'redis',
                'port': 6379
            }
        }
    }
)
