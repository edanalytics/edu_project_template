import croniter
import logging

from typing import List, Optional, Tuple, Union

from airflow.exceptions import AirflowFailException
from airflow.models import Connection
from airflow.models.baseoperator import chain

from edfi_api_client import camel_to_snake

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models.operator import Operator


def get_context_variable(context, variable_name: str, default: object):
    """
    Generic method to search the DAG Run conf and params for a context variable.

    :param context:
    :param variable_name:
    :param default:
    :return:
    """
    # Check Airflow 2.6 params first
    if 'params' in context and variable_name in context['params']:
        return context['params'][variable_name]

    elif context['dag_run'].conf and variable_name in context['dag_run'].conf:
        return context['dag_run'].conf[variable_name]

    else:
        return default


def is_full_refresh(context) -> bool:
    """

    :param context:
    :return:
    """
    # If user_defined_macros are not defined in DAG init, their context attribute is None.
    if full_refresh_macro := (context["dag"].user_defined_macros or {}).get("is_scheduled_full_refresh", False):
        if full_refresh_macro(**context):
            logging.info("Scheduled full-refresh criteria met! Triggering a full-refresh run.")
            return True

    return get_context_variable(context, 'full_refresh', default=False)


def run_matches_cron(cron: Optional[str], **context) -> bool:
    """
    Simple helper to check whether the execution timestamp of the DAG-run matches a specified cron expression.
    Note that default cron behavior uses boolean OR logic to handle day and day-of-week entries, so we set day_or=False.

    Advanced cron examples:
        "2 4 1 * wed        ": 04:02 on every 1st day of the month if it is a Wednesday
        "0 0 * * sat#1,sun#2": 00:00 on the 1st Saturday and 2nd Sunday of the month
        "0 0 * * 5#3,L5     ": 00:00 on the 3rd and last Friday of the month
    """
    if not cron:
        return False

    if not croniter.croniter.is_valid(cron):
        raise ValueError("Provided cron syntax to match against current DAG run is invalid!")

    return croniter.croniter.match(cron, context["dag_run"].logical_date, day_or=False)


def get_config_endpoints(context) -> List[str]:
    """

    :param context:
    :return:
    """
    # Apply camel_to_snake transform on all specified endpoints to circumvent user-input error.
    raw_endpoints = get_context_variable(context, 'endpoints', default=[])
    return list(map(camel_to_snake, raw_endpoints))


def xcom_pull_template(
        task_ids: Union[str, 'Operator', List[str], List['Operator']],
        key: str = 'return_value',
        prefix: str = "",
        suffix: str = ""
) -> str:
    """
    Generate a Jinja template to pull a particular xcom key from a task_id
    :param task_ids: An upstream task to pull xcom from
    :param key: The key to retrieve. Default: return_value
    :return: A formatted Jinja string for the xcom pull
    """
    # Retrieve string representations for each XCom in a list.
    if isinstance(task_ids, (list, tuple)):
        task_ids = [
            task_id.task_id if hasattr(task_id, 'task_id') else task_id
            for task_id in task_ids
        ]
        task_ids_string = "['{}']".format("','".join(task_ids))

    # Or retrieve string representation of a single XCom.
    else:
        if hasattr(task_ids, 'task_id'):
            task_ids = task_ids.task_id
        task_ids_string = f"'{task_ids}'"

    xcom_string = f"{prefix} ti.xcom_pull(task_ids={task_ids_string}, key='{key}') {suffix}"
    return '{{ ' + xcom_string + ' }}'


def get_snowflake_params_from_conn(
        snowflake_conn_id: str
) -> Tuple[str, str]:
    undefined_snowflake_error = ValueError(
        f"Snowflake `extra__snowflake__database` and `schema` must be defined within `{snowflake_conn_id}`."
    )

    try:
        snowflake_conn = Connection.get_connection_from_secrets(snowflake_conn_id)

        database = snowflake_conn.extra_dejson['extra__snowflake__database']
        schema = snowflake_conn.schema

        if database is None or schema is None:
            raise undefined_snowflake_error

        return database, schema

    except KeyError:
        raise undefined_snowflake_error


def fail_if_any_task_failed(**context):
    """
    Simple Python callable that raises an AirflowFailException if any task in the DAG has failed.
    """
    for ti in context["dag_run"].get_task_instances():
        if ti.state == "failed":
            raise AirflowFailException("One or more tasks in the DAG failed.")


def chain_tasks(*tasks):
    """
    Alias of airflow's built-in chain, but recursively remove Nones if present.
    """
    chain(*recursive_filter(None, tasks))


def recursive_filter(func, iterable):
    """
    Taken from here: https://www.mycompiler.io/view/CDbwWjY
    """
    results = []
    for elem in iterable:
        if isinstance(elem, (list, tuple)):
            result = recursive_filter(func, elem)
            if result:
                results.append(result)
        elif func is None:
            if elem:
                results.append(elem)
        elif func(elem):
            results.append(elem)
    return results


def get_params_from_conn(conn_id: str, extra_params: str) -> Tuple[str, str]:
    undefined_connection_error = ValueError(
        f"Connection: '{extra_params}' and `schema` must be defined within `{conn_id}`."
    )

    try:
        conn = Connection.get_connection_from_secrets(conn_id)
        database = conn.extra_dejson[extra_params]
        schema = conn.schema

        if database is None or schema is None:
            raise undefined_connection_error

        return database, schema

    except KeyError:
        raise undefined_connection_error
