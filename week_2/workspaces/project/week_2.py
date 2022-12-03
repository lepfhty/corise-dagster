from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={'s3_key': String},
    required_resource_keys={'s3'},
    out=Out(dagster_type=List[Stock], description="List of Stocks")
)
def get_s3_data(context) -> List[Stock]:
    return list(map(lambda values: Stock.from_list(values),
                    context.resources.s3.get_data(context.op_config['s3_key'])))


@op(
    ins={'stocks': In(dagster_type=List[Stock], description="List of Stocks")},
    out=Out(dagster_type=Aggregation, description="Aggregation of Stock with largest high value")
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    max_stock = max(stocks, key=lambda x: x.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    required_resource_keys={'redis'},
    ins={'agg': In(dagster_type=Aggregation, description="Aggregation to store in redis")}
)
def put_redis_data(context, agg: Aggregation) -> None:
    context.resources.redis.put_data(name=str(agg.date), value=agg.high)


@op(
    required_resource_keys={'s3'},
    ins={'agg': In(dagster_type=Aggregation, description="Aggregation to write to S3 as json")}
)
def put_s3_data(context, agg: Aggregation) -> None:
    name = f'aggregation_{agg.date}'
    context.resources.s3.put_data(key_name=name, data=agg)


@graph
def week_2_pipeline():
    agg = process_data(get_s3_data())
    put_redis_data(agg)
    put_s3_data(agg)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={'s3': mock_s3_resource,
                   'redis': ResourceDefinition.mock_resource()}
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={'s3': s3_resource,
                   'redis': redis_resource}
)
