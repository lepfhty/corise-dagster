import json
import logging
import pickle
import random
from typing import Any, Dict, List, Union

from dagster import IOManager, In, InitResourceContext, InputContext, OpExecutionContext, Out, \
    OutputContext, String, \
    graph, io_manager, op
from sqlalchemy import column, table

from workspaces.resources import Postgres, postgres_resource

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


class PostgresIOManager(IOManager):
    def __init__(self, table_str: str):
        if "." in table_str:
            schema, table_name = table_str.split('.')
        else:
            schema, table_name = None, table_str
        self.table = table(table_name,
                           column('column_1'),
                           column('column_2'),
                           column('column_3'),
                           schema=schema)

    def handle_output(self, context: OutputContext, values: List[Dict[str, str]]):
        db = context.resources.database
        stmt = self.table.insert().values(values)
        db.execute_query(stmt)

    def load_input(self, context: InputContext):
        db = context.resources.database
        # warning: anything could have inserted into this table!!! x_X
        stmt = self.table.select()
        result = db.execute_query(stmt)
        return [dict(row) for row in result]


class PostgresBlobIOManager(IOManager):
    def __init__(self, database: Postgres, blob_table: str, data_table: str):
        self.database = database
        self.blob_table = blob_table
        self.data_table = data_table
        self.table = table(self.blob_table, column('id'), column('pickle_data'))

    def _create_table(self):
        sql = f'''CREATE TABLE IF NOT EXISTS "{self.blob_table}"
        (id varchar primary key, pickle_data bytea)'''
        self.database.execute_query(sql)

    def _store_result(self, id_str: str, obj: Any) -> None:
        self._create_table()
        stmt = self.table.insert().values(id=id_str, pickle_data=pickle.dumps(obj))
        self.database.execute_query(stmt)

    def _retrieve_result(self, id_str: str) -> Any:
        stmt = self.table.select().where(column('id') == id_str)
        result = self.database.execute_query(stmt)
        if result.rowcount == 1:
            row = result.first()
            data = pickle.loads(row['pickle_data'])
            stmt = self.table.delete().where(column('id') == id_str)
            self.database.execute_query(stmt)
            return data

    def _store_data(self, table_arg: str, data: List[Dict[str, Any]]) -> None:
        if len(data) == 0:
            return None
        columns = [column(key) for key in data[0].keys()]
        if "." in table_arg:
            schema, table_name = table_arg.split('.')
        else:
            schema, table_name = None, table_arg
        stmt = table(table_name, *columns, schema=schema).insert().values(data)
        self.database.execute_query(stmt)

    def _id_str(self, context: Union[InputContext, OutputContext]) -> str:
        if context.has_asset_key:
            ids = context.get_asset_identifier()
        else:
            ids = context.get_identifier()
        return '/'.join(ids)

    def handle_output(self, context: OutputContext, obj: List[Dict[str, str]]) -> None:
        self._store_result(self._id_str(context), obj)
        self._store_data(self.data_table, obj)

    def load_input(self, context: InputContext) -> Any:
        return self._retrieve_result(self._id_str(context))


@io_manager(required_resource_keys={"database"})
def postgres_blob_manager(init_context: InitResourceContext):
    db = init_context.resources.database
    blob_table = init_context.resource_config['blob_table']
    data_table = init_context.resource_config['data_table']
    return PostgresBlobIOManager(database=db,
                                 blob_table=blob_table,
                                 data_table=data_table)


@io_manager(required_resource_keys={"database"})
def postgres_io_manager(init_context):
    table_name = init_context.resource_config['table_name']
    return PostgresIOManager(table_str=table_name)


@op(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    out=Out(String),
    tags={"kind": "postgres"},
)
def create_table(context: OpExecutionContext) -> String:
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op(
    ins={'table_name': In(String)},
    out=Out(List[Dict[str, str]], io_manager_key='postgres_io'),
)
def insert_data(context: OpExecutionContext, table_name: str) -> List[Dict[str, str]]:
    result = []
    for _ in range(10):
        row = dict()
        for col in range(3):
            row[f'column_{col + 1}'] = str(random.random())
        result.append(row)
    context.add_output_metadata({'table': table_name})
    return result


@op(required_resource_keys={'database'},
    ins={'data': In(input_manager_key='postgres_io')})
def table_count(context: OpExecutionContext, data):
    context.log.info(json.dumps(data))
    context.log.info(len(data))
    db_count = context.resources.database.execute_query('select count(*) from analytics.table')
    context.log.info(db_count.scalar())
    return len(data)


@graph
def week_3_challenge():
    table = create_table()
    data = insert_data(table)
    count = table_count(data)


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
        'postgres_io': {
            'config': {
                'table_name': 'analytics.table'
            }
        },
        'postgres_blob': {
            'config': {
                'blob_table': 'pgio_blob',
                'data_table': 'analytics.table'
            }
        }
    },
    "ops": {"create_table": {"config": {"table_name": "analytics.table"}}},
}

week_3_challenge_docker = week_3_challenge.to_job(
    name='week_3_challenge_docker',
    config=docker,
    resource_defs={'database': postgres_resource,
                   'postgres_io': postgres_io_manager,
                   'postgres_blob': postgres_blob_manager},
)
