import json
import logging
import os

from typing import Any, Optional

from airflow.exceptions import AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.utils.decorators import apply_defaults

from tn_edu_airflow.callables import airflow_util
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook


class ADLSToDatabricksOperator(BaseOperator):
    """
    Copy the Ed-Fi files saved to ADLS to Databricks raw resource tables.
    """
    template_fields = (
    'resource', 'table_name', 'adls_destination_key', 'adls_destination_dir', 'adls_destination_filename',
    'xcom_return',)

    @apply_defaults
    def __init__(self,
                 *,
                 tenant_code: str,
                 api_year: int,
                 resource: str,
                 table_name: str,

                 adls_destination_key: Optional[str] = None,
                 adls_destination_dir: Optional[str] = None,
                 adls_destination_filename: Optional[str] = None,
                 adls_storage_account: Optional[str] = None,
                 adls_container: Optional[str] = None,
                 databricks_conn_id: str,

                 edfi_conn_id: Optional[str] = None,
                 ods_version: Optional[str] = None,
                 data_model_version: Optional[str] = None,

                 full_refresh: bool = False,
                 xcom_return: Optional[Any] = None,
                 **kwargs
                 ) -> None:
        super(ADLSToDatabricksOperator, self).__init__(**kwargs)

        self.edfi_conn_id = edfi_conn_id
        self.databricks_conn_id = databricks_conn_id

        self.tenant_code = tenant_code
        self.api_year = api_year
        self.resource = resource
        self.table_name = table_name

        self.adls_destination_key = adls_destination_key
        self.adls_destination_dir = adls_destination_dir
        self.adls_destination_filename = adls_destination_filename
        self.adls_storage_account = adls_storage_account
        self.adls_container = adls_container

        self.ods_version = ods_version
        self.data_model_version = data_model_version

        self.full_refresh = full_refresh
        self.xcom_return = xcom_return

    def execute(self, context):
        """

        :param context:
        :return:
        """
        ### Optionally set destination key by concatting separate args for dir and filename
        if not self.adls_destination_key:
            if not (self.adls_destination_dir and self.adls_destination_filename):
                raise ValueError(
                    f"Argument `adls_destination_key` has not been specified, and `adls_destination_dir` or `adls_destination_filename` is missing."
                )
            self.adls_destination_key = os.path.join(self.adls_destination_dir, self.adls_destination_filename)

        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()

        # Build and run the SQL queries to Snowflake. Delete first if EdFi2 or a full-refresh.
        self.run_sql_queries(
            name=self.resource, table=self.table_name,
            adls_key=self.adls_destination_key, full_refresh=airflow_util.is_full_refresh(context)
        )

        return self.xcom_return

    def set_edfi_attributes(self):
        """
        Retrieve the Ed-Fi, ODS, and data model versions if not provided.
        This needs to occur in execute to not call the API at every Airflow synchronize.
        """
        if self.edfi_conn_id:
            edfi_conn = EdFiHook(edfi_conn_id=self.edfi_conn_id).get_conn()
            if is_edfi2 := edfi_conn.is_edfi2():
                self.full_refresh = True

            if not self.ods_version:
                self.ods_version = 'ED-FI2' if is_edfi2 else edfi_conn.get_ods_version()

            if not self.data_model_version:
                self.data_model_version = 'ED-FI2' if is_edfi2 else edfi_conn.get_data_model_version()

        if not (self.ods_version and self.data_model_version):
            raise Exception(
                f"Arguments `ods_version` and `data_model_version` could not be retrieved and must be provided."
            )

    def run_sql_queries(self, name: str, table: str, adls_key: str, full_refresh: bool = False):
        """

        """
        databricks_hook = DatabricksSqlHook(databricks_conn_id=self.databricks_conn_id)
        database, schema = airflow_util.get_params_from_conn(self.databricks_conn_id, "extra__databricks__database")

        ### Build the SQL queries to be passed into `Hook.run()`.
        qry_delete = f"""
            DELETE FROM {database}.{schema}.{table}
            WHERE tenant_code = '{self.tenant_code}'
            AND api_year = '{self.api_year}'
            AND name = '{name}'
        """

        qry_create_table = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table}_stage
        """

        qry_drop_table = f"""
            DROP TABLE {database}.{schema}.{table}_stage
        """
        qry_copy_into = f"""
            COPY INTO {database}.{schema}.{table}_stage
            FROM 'abfss://{self.adls_container}@{self.adls_storage_account}.dfs.core.windows.net/{adls_key}'
            FILEFORMAT = TEXT
            COPY_OPTIONS ('force' = 'true', 'mergeSchema' = 'true')"""

        qry_insert = f"""
            INSERT INTO {database}.{schema}.{table}
            SELECT 
                parse_json(value) as v, 
                '{self.tenant_code}' as tenant_code,
                '{self.api_year}' as `api_year`, 
                current_date() as `pull_date`, 
                current_timestamp() as `pull_timestamp`, 
                ROW_NUMBER() OVER (ORDER BY value) as `file_row_number`, 
                '{adls_key}' as `filename`,
                '{name}' as name, 
                '{self.ods_version}' as `ods_version`, 
                '{self.data_model_version}' as `data_model_version`
            FROM {database}.{schema}.{table}_stage    
        """

        # Incremental runs are only available in EdFi 3+.
        if self.full_refresh or full_refresh:
            databricks_hook.run(
                sql=[qry_delete, qry_create_table, qry_copy_into, qry_insert, qry_drop_table],
                autocommit=False
            )
        else:
            databricks_hook.run(
                sql=[qry_create_table, qry_copy_into, qry_insert, qry_drop_table]
            )


class BulkADLSToDatabricksOperator(ADLSToDatabricksOperator):
    """
    Copy the Ed-Fi files saved to S3 to Snowflake raw resource tables.
    """

    def execute(self, context):
        """

        :param context:
        :return:
        """
        if not self.resource:
            raise AirflowSkipException("There are no endpoints to copy to Snowflake. Skipping task...")

        # Force potential string columns into lists for zipping in execute.
        if isinstance(self.resource, str):
            raise ValueError("Bulk operators require lists of resources to be passed.")

        if isinstance(self.table_name, str):
            self.table_name = [self.table_name] * len(self.resource)

        ### Optionally set destination key by concatting separate args for dir and filename
        if not self.adls_destination_key:
            if not (self.adls_destination_dir and self.adls_destination_filename):
                raise ValueError(
                    f"Argument `adls_destination_key` has not been specified, and `adls_destination_dir` or `adls_destination_filename` is missing."
                )

            if isinstance(self.adls_destination_filename, str):
                raise ValueError(
                    "Bulk operators require argument `adls_destination_filename` to be a list."
                )

            self.adls_destination_key = [
                os.path.join(self.adls_destination_dir, filename)
                for filename in self.adls_destination_filename
            ]

        elif isinstance(self.adls_destination_key, str):
            raise ValueError(
                "Bulk operators require argument `adls_destination_key` to be a list."
            )

        ### Retrieve the Ed-Fi, ODS, and data model versions in execute to prevent excessive API calls.
        self.set_edfi_attributes()

        # Build and run the SQL queries to Snowflake. Delete first if EdFi2 or a full-refresh.
        xcom_returns = []

        for idx, (resource, table, adls_destination_key) in enumerate(
                zip(self.resource, self.table_name, self.adls_destination_key), start=1):
            logging.info(f"[ENDPOINT {idx} / {len(self.resource)}]")
            self.run_sql_queries(
                name=resource, table=table,
                adls_key=adls_destination_key, full_refresh=airflow_util.is_full_refresh(context)
            )

        # Send the prebuilt-output if specified; otherwise, send the compiled list created above.
        # This only exists to maintain backwards-compatibility with original S3ToSnowflakeOperator.
        if self.xcom_return:
            return self.xcom_return
        else:
            return xcom_returns
