from random import randint

from dagster import In, Nothing, Out, Output, String, graph, op
from dagster_dbt import DbtCliOutput, dbt_cli_resource, dbt_run_op, dbt_test_op

from workspaces.resources import postgres_resource

DBT_PROJECT_PATH = "/opt/dagster/dagster_home/dbt_test_project/."


@op(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def create_dbt_table(context) -> String:
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op(
    ins={"table_name": In(dagster_type=String)},
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def insert_dbt_data(context, table_name):
    sql = f"INSERT INTO {table_name} (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 100)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@op(
    required_resource_keys={'dbt'},
    ins={'start_after': In(Nothing)},
    out={'success': Out(is_required=False),
         'failure': Out(is_required=False)}
)
def dbt_test(context):
    result = context.resources.dbt.test()
    if result.return_code == 0:
        yield Output(None, 'success')
    else:
        yield Output(None, 'failure')


@op(
    ins={'result': In(dagster_type=DbtCliOutput)}
)
def dbt_result(context, result: DbtCliOutput):
    if result.return_code == 0:
        context.log.info('Success!')
    else:
        context.log.error('Failure!')


@op(
    ins={'start_after': In(Nothing)}
)
def dbt_success(context) -> None:
    context.log.info('Success!')


@op(
    ins={'start_after': In(Nothing)}
)
def dbt_failure(context) -> None:
    context.log.error('Failure')


@graph
def prepare_and_run_dbt():
    table = create_dbt_table()
    insert_done = insert_dbt_data(table)
    return dbt_run_op(start_after=insert_done)


@graph
def week_2_challenge():
    run_done = prepare_and_run_dbt()

    # use custom op to run test and branch to separate ops
    success, failure = dbt_test(start_after=run_done)
    dbt_success(start_after=success)
    dbt_failure(start_after=failure)


@graph
def week_2_challenge_single_op():
    run_done = prepare_and_run_dbt()

    # use dbt_test_op and single op to log results
    test_result = dbt_test_op(start_after=run_done)
    dbt_result(test_result)


docker = {
    "resources": {
        "database": {
            "config": {
                "host": "postgresql",
                "user": "postgres_user",
                "password": "postgres_password",
                "database": "postgres_db",
            }
        },
        "dbt": {
            "config": {
                "project_dir": DBT_PROJECT_PATH,
                "profiles_dir": DBT_PROJECT_PATH,
                "ignore_handled_error": True,
                "target": "test",
            },
        },
    },
    "ops": {
        "prepare_and_run_dbt": {
            "ops": {
                "create_dbt_table": {
                    "config": {
                        "table_name": "analytics.dbt_table"
                    }
                }
            }
        }
    }
}

week_2_challenge_docker = week_2_challenge.to_job(
    name="week_2_challenge_docker",
    config=docker,
    resource_defs={
        'database': postgres_resource,
        'dbt': dbt_cli_resource
    }
)

week_2_challenge_single_op_docker = week_2_challenge_single_op.to_job(
    name="week_2_challenge_single_op_docker",
    config=docker,
    resource_defs={
        'database': postgres_resource,
        'dbt': dbt_cli_resource
    }
)
