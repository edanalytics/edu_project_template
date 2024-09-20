import logging

from typing import List, Union

from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

from tn_edu_airflow.callables import airflow_util


def insert_into_databricks(
        databricks_conn_id: str,
        table_name: str,
        columns: List[str],
        values: Union[list, List[list]]
):
    """

    :param databricks_conn_id:
    :param table_name:
    :param columns:
    :param values:
    :return:
    """
    # Force a single record into a list for iteration below.
    if not all(isinstance(val, (list, tuple)) for val in values):
        values = [values]

    # Retrieve the database and schema from the Snowflake hook.
    database, schema = airflow_util.get_params_from_conn(databricks_conn_id, "extra__databricks__database")

    logging_string = f"Inserting the following values into table `{database}.{schema}.{table_name}`\nCols: {columns}\n"
    for idx, value in enumerate(values, start=1):
        logging_string += f"   {idx}: {value}\n"
    logging.info(logging_string)

    databricks_hook = DatabricksSqlHook(databricks_conn_id=databricks_conn_id)
    databricks_hook.insert_rows(
        table=f"{database}.{schema}.{table_name}",
        rows=values,
        target_fields=columns,
    )
