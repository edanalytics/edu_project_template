import logging

from typing import Dict, List, Tuple, Optional

from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

from tn_edu_airflow.callables.databricks import insert_into_databricks
from tn_edu_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


def get_newest_edfi_change_version(edfi_conn_id: str, **kwargs) -> int:
    """

    :return:
    """
    ### Connect to EdFi ODS and verify EdFi3.
    edfi_conn = EdFiHook(edfi_conn_id=edfi_conn_id).get_conn()

    # Break off prematurely if change versions not supported.
    if edfi_conn.is_edfi2():
        logging.warning("Change versions are only supported in EdFi 3+!")
        return None

    # Pull current max change version from EdFi.
    max_change_version = edfi_conn.get_newest_change_version()
    logging.info(f"Current max change version is `{max_change_version}`.")

    return max_change_version


def reset_change_versions(
        tenant_code : str,
        api_year    : int,

        *,
        databricks_conn_id: str,
        change_version_table: str,

        **kwargs
) -> None:
    # Airflow-skip if run not marked for a full-refresh.
    if not airflow_util.is_full_refresh(kwargs):
        raise AirflowSkipException(f"Full refresh not specified. Change version table `{change_version_table}` unchanged.")

    ### Prepare the SQL query.
    # Retrieve the database and schema from the Snowflake hook, and raise an exception if undefined.
    database, schema = airflow_util.get_params_from_conn(databricks_conn_id, "extra__databricks__database")

    # Reset all resources in this tenant-year.
    qry_mark_inactive = f"""
        update {database}.{schema}.{change_version_table}
            set is_active = FALSE
        where tenant_code = '{tenant_code}'
        and api_year = {api_year}
        and is_active
    """

    # Filter only to inactive endpoints to those specified in DAG-configs, if defined.
    if config_endpoints := airflow_util.get_config_endpoints(kwargs):
        qry_mark_inactive += "    and name in ('{}')".format("', '".join(config_endpoints))

    ### Connect to Snowflake and execute the query.
    logging.info("Full refresh: marking previous pulls inactive.")
    DatabricksSqlHook(databricks_conn_id).run(qry_mark_inactive)


def get_previous_change_versions(
        tenant_code: str,
        api_year: int,
        endpoints: List[Tuple[str, str]],

        *,
        databricks_conn_id: str,
        change_version_table: str,

        get_deletes: bool = False,
        get_key_changes: bool = False,

        **context
) -> None:
    """
    We separate getting the most recent EdFi change version into a separate function.
    We need pulls to be consistent by change version for each run.
    Otherwise, the raw schema will passively drift across resources around versions.

    With every pull from EdFi 3+ to Snowflake, the change-version table is updated for the resource.
    The change version documents the timestamp of the last pull.

    This operator retrieves the most recent versions found in Snowflake for all resources.
    Use this and the most recent change version from EdFi to build an ingestion window.
    At the end of the ingest, the change version table is updated with the most recent version found in EdFi.

    If a full refresh is being completed, all change versions for this tenant-year have been set to inactive.
    """
    # Skip deletes/key-changes if a full-refresh.
    if airflow_util.is_full_refresh(context) and (get_deletes or get_key_changes):
        raise AirflowSkipException("Skipping deletes/key-changes pull for full_refresh run.")

    logging.info("Retrieving max previously-ingested change versions from Snowflake.")

    ### Prepare the SQL query.
    # Retrieve the database and schema from the Snowflake hook, and raise an exception if undefined.
    database, schema = airflow_util.get_params_from_conn(databricks_conn_id, "extra__databricks__database")

    # Retrieve the previous max change versions for this tenant-year.
    if get_deletes:
        filter_clause = "is_deletes"
    elif get_key_changes:
        filter_clause = "is_key_changes"
    else:
        filter_clause = " AND NOT COALESCE(is_deletes, false) AND NOT COALESCE(is_key_changes, false)"
    logging.info(f"filter_clause: {filter_clause}")

    qry_prior_max = f"""
        select name, max(max_version) as max_version
        from {database}.{schema}.{change_version_table}
        where tenant_code = '{tenant_code}'
            and api_year = {api_year}
            and is_active
            and {filter_clause}
        group by all
    """

    ### Retrieve previous endpoint-level change versions and push as an XCom.
    prior_change_versions = dict(DatabricksSqlHook(databricks_conn_id).get_records(qry_prior_max))
    logging.info(
        f"Collected prior change versions for {len(prior_change_versions)} endpoints."
    )

    return_tuples = []

    for namespace, endpoint in endpoints:
        if (last_max_version := prior_change_versions.get(endpoint, 0)):
            logging.info(f"    {namespace}/{endpoint}: {last_max_version}")

        return_tuples.append((endpoint, last_max_version))

    return return_tuples


def get_previous_change_versions_with_deltas(
        tenant_code: str,
        api_year: int,
        endpoints: List[Tuple[str, str]],

        *,
        databricks_conn_id: str,
        change_version_table: str,

        edfi_conn_id: Optional[str],
        max_change_version: Optional[int],

        get_deletes: bool = False,
        get_key_changes: bool = False,

        **context
) -> None:
    """
    Overload get_previous_change_versions() with a check against the Ed-Fi API to only return endpoints with new data to process.
    """
    return_tuples = get_previous_change_versions(
        tenant_code=tenant_code, api_year=api_year, endpoints=endpoints,
        databricks_conn_id=databricks_conn_id, change_version_table=change_version_table,
        get_deletes=get_deletes, get_key_changes=get_key_changes,
        **context
    )

    edfi_conn = EdFiHook(edfi_conn_id=edfi_conn_id).get_conn()

    # Only ping the API if the endpoint is specified in the run.
    config_endpoints = airflow_util.get_config_endpoints(context)

    # Convert (namespace, endpoint) tuples to a {endpoint: namespace} dictionary.
    endpoint_namespaces = {endpoint: namespace for namespace, endpoint in endpoints}

    # Track which endpoints have deltas or failed total-count gets.
    logging.info("Checking each endpoint for new records...")

    delta_endpoints = []
    failed_endpoints = []

    logging.info(f"Tuples: {return_tuples}")

    for endpoint, last_max_version in return_tuples:

        # If a subset of endpoints have been selected, only get CV counts for these.
        if config_endpoints and endpoint not in config_endpoints:
            continue

        if last_max_version == max_change_version:
            continue

        namespace = endpoint_namespaces[endpoint]
        logging.info(f"Endpoint: {endpoint}")
        try:
            resource = edfi_conn.resource(
                endpoint, namespace=namespace,
                get_deletes=get_deletes, get_key_changes=get_key_changes,
                min_change_version=last_max_version,
                max_change_version=max_change_version
            )

            logging.info(f"Resource: {resource}")

            if not (delta_record_count := resource.total_count()):
                continue

            logging.info(f"    {namespace}/{endpoint}: {delta_record_count} new records")
            delta_endpoints.append((endpoint, last_max_version))

        except Exception as e:
            print(f"Caught exception: {type(e).__name__} - {e}")
            logging.warning(
                f"    Unable to retrieve record count for endpoint: {namespace}/{endpoint}"
            )
            failed_endpoints.append(endpoint)  # Still return the tuples, but mark as failed in the UI.
            continue

    # Always push the xcom, but return a second xcom if at least one endpoint failed.
    # This should NOT be necessary, but we encountered a bug where a downstream "none_skipped" task skipped with "upstream_failed" status.
    if failed_endpoints:
        logging.info(
            f"Failed getting delta row count for one or more endpoints: {failed_endpoints}"
        )
        context['ti'].xcom_push(key='failed_endpoints', value=failed_endpoints)

    if not delta_endpoints:
        raise AirflowSkipException("No endpoints to process were found. Skipping downstream ingestion.")

    return delta_endpoints


def update_change_versions(
        tenant_code: str,
        api_year   : int,

        *,
        databricks_conn_id: str,
        change_version_table: str,

        edfi_change_version: int,
        endpoints: List[str],
        get_deletes: bool,
        get_key_changes: bool,

        **kwargs
):
    """

    :return:
    """
    if not endpoints:
        raise AirflowSkipException(
            "There are no new change versions to update for any endpoints. All upstream tasks skipped or failed."
        )

    logging.info(f"Collected updated change versions for {len(endpoints)} endpoints.")

    # Deletes and KeyChanges are mutually-exclusive. Delete-status is original and required to output.
    # Optionally adding KeyChanges removes the need to update the change-version table until necessary (default False).
    columns = [
        "tenant_code", "api_year", "name",
        "pull_date", "pull_timestamp",
        "max_version", "is_active",
        "is_deletes",
    ]

    if get_key_changes:
        columns.append("is_key_changes")

    # Build and insert row tuples for each endpoint.
    rows_to_insert = []

    for endpoint in endpoints:
        row = [
            tenant_code, api_year, endpoint,
            kwargs["ds"], kwargs["ts"],
            edfi_change_version, True,
            get_deletes,
        ]

        if get_key_changes:
            row.append(get_key_changes)

        rows_to_insert.append(row)

    insert_into_databricks(
        databricks_conn_id=databricks_conn_id,
        table_name=change_version_table,
        columns=columns,
        values=rows_to_insert
    )

    return True
