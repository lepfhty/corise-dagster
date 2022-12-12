from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext, Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext, SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)

from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={'s3_key': str},
    required_resource_keys={'s3'},
    out=Out(List[Stock])
)
def get_s3_data(context: OpExecutionContext):
    key = context.op_config['s3_key']
    context.log.info(key)
    for x in context.resources.s3.list_keys():
        context.log.info(x)
    return [Stock.from_list(row) for row in context.resources.s3.get_data(key)]


@op(
    ins={'stocks': In(List[Stock])},
    out=Out(Aggregation)
)
def process_data(context: OpExecutionContext, stocks: List[Stock]) -> Aggregation:
    max_stock = max(stocks, key=lambda s: s.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    required_resource_keys={'redis'},
    ins={'agg': In(Aggregation)},
    out=Out(Nothing)
)
def put_redis_data(context: OpExecutionContext, agg: Aggregation):
    context.resources.redis.put_data(name=str(agg.date),
                                     value=str(agg.high))


@op(
    required_resource_keys={'s3'},
    ins={'agg': In(Aggregation)},
    out=Out(Nothing)
)
def put_s3_data(context, agg: Aggregation):
    filename = f'aggregation_{agg.date}.json'
    context.resources.s3.put_data(key_name=filename,
                                  data=agg)


@graph
def week_3_pipeline():
    agg = process_data(get_s3_data())
    put_redis_data(agg)
    put_s3_data(agg)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

PARTS = [str(i) for i in range(1, 11)]


@static_partitioned_config(
    partition_keys=PARTS
)
def docker_config(partition_key: str = None, full_s3_key: str = None):
    key = full_s3_key if full_s3_key else f'prefix/stock_{partition_key}.csv'
    config = docker.copy()
    config['ops']['get_s3_data']['config']['s3_key'] = key
    return config


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={'s3': mock_s3_resource,
                   'redis': ResourceDefinition.mock_resource()}
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={'s3': s3_resource,
                   'redis': redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local,
                                           cron_schedule="*/15 * * * *")


@schedule(
    cron_schedule="0 * * * *",
    job=week_3_pipeline_docker
)
def week_3_schedule_docker():
    for part in PARTS:
        yield week_3_pipeline_docker.run_request_for_partition(partition_key=part, run_key=part)


@sensor(
    job=week_3_pipeline_docker,
)
def week_3_sensor_docker(context: SensorEvaluationContext):
    keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url='http://localstack:4566')
    if len(keys) == 0:
        yield SkipReason('No new s3 files found in bucket.')
    else:
        for key in keys:
            yield RunRequest(run_key=key,
                             run_config=docker_config(full_s3_key=key))
