import copy
import logging
import os
from functools import partial
from typing import Dict, List, Optional, Set, Tuple, Union

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ea_airflow_util import slack_callbacks, update_variable
from edfi_api_client import camel_to_snake

from tn_edu_airflow.callables import change_version
from ea_airflow_util import EACustomDAG
from tn_edu_airflow.callables import airflow_util
from tn_edu_airflow.providers.edfi.transfers.edfi_to_adls import EdFiToADLSOperator, BulkEdFiToADLSOperator
from tn_edu_airflow.providers.databricks.transfers.adls_to_databricks import BulkADLSToDatabricksOperator


class TNEdFiResourceDAG:
    """
    If use_change_version is True, initialize a change group that retrieves the latest Ed-Fi change version.
    If full_refresh is triggered in DAG configs, reset change versions for the resources being processed in Databricks.

    DAG Structure:
        (Ed-Fi3 Change Version Window) >> [Ed-Fi Resources/Descriptors (Deletes/KeyChanges)] >> (increment_dbt_variable) >> dag_state_sentinel

    "Ed-Fi3 Change Version Window" TaskGroup:
        get_latest_edfi_change_version >> reset_previous_change_versions_in_databricks

    "Ed-Fi Resources/Descriptors (Deletes/KeyChanges)" TaskGroup:
        (get_cv_operator) >> [Ed-Fi Endpoint Task] >> copy_all_endpoints_into_databricks >> (update_change_versions_in_databricks)

    There are three types of Ed-Fi Endpoint Tasks. All take the same inputs and return the same outputs (i.e., polymorphism).
        "Default" TaskGroup
        - Create one task per endpoint.
        "Dynamic" TaskGroup
        - Dynamically task-map all endpoints. This function presumes that only endpoints with deltas to ingest are passed as input.
        "Bulk" TaskGroup
        - Loop over each endpoint in a single task.

    All task groups receive a list of (endpoint, last_change_version) tuples as input.
    All that successfully retrieve records are passed onward as a (endpoint, filename) tuples to the ADLSToDatabricks and UpdateDatbricksCV operators.
    """
    DEFAULT_CONFIGS = {
        'namespace': 'ed-fi',
        'page_size': 500,
        'change_version_step_size': 50000,
        'num_retries': 5,
        'query_parameters': {},
        'offset': 0
    }

    newest_edfi_cv_task_id = "get_latest_edfi_change_version"  # Original name for historic run compatibility

    def __init__(self,
                 *,
                 tenant_code: str,
                 api_year: int,

                 edfi_conn_id: str,
                 adls_conn_id: str,
                 adls_container: str,
                 adls_storage_account: str,
                 databricks_conn_id: str,

                 pool: str,
                 tmp_dir: str,

                 multiyear: bool = False,
                 schedule_interval_full_refresh: Optional[str] = None,

                 use_change_version: bool = True,
                 get_key_changes: bool = False,
                 run_type: str = "default",
                 resource_configs: Optional[List[dict]] = None,
                 descriptor_configs: Optional[List[dict]] = None,

                 change_version_table: str = '_meta_change_versions',
                 deletes_table: str = '_deletes',
                 key_changes_table: str = '_key_changes',
                 descriptors_table: str = '_descriptors',
                 get_deletes_cv_with_deltas: bool = False,
                 dbt_incrementer_var: Optional[str] = None,

                 **kwargs
                 ) -> None:
        self.run_type = run_type
        self.use_change_version = use_change_version
        self.get_key_changes = get_key_changes

        self.tenant_code = tenant_code
        self.api_year = api_year

        self.edfi_conn_id = edfi_conn_id
        self.adls_conn_id = adls_conn_id
        self.adls_storage_account = adls_storage_account
        self.adls_container = adls_container
        self.databricks_conn_id = databricks_conn_id

        self.pool = pool
        self.tmp_dir = tmp_dir
        self.multiyear = multiyear
        self.schedule_interval_full_refresh = schedule_interval_full_refresh  # Force full-refresh on a scheduled cadence

        self.change_version_table = change_version_table
        self.deletes_table = deletes_table
        self.key_changes_table = key_changes_table
        self.descriptors_table = descriptors_table

        self.dbt_incrementer_var = dbt_incrementer_var

        ### Parse optional config objects (improved performance over adding resources manually).
        resource_configs, resource_deletes, resource_key_changes = self.parse_endpoint_configs(resource_configs)
        descriptor_configs, descriptor_deletes, descriptor_key_changes = self.parse_endpoint_configs(descriptor_configs)
        self.endpoint_configs = {**resource_configs, **descriptor_configs}

        # Build lists of each enabled endpoint type (only collect deletes and key-changes for resources).
        self.resources = set(resource_configs.keys())
        self.descriptors = set(descriptor_configs.keys())
        self.deletes_to_ingest = resource_deletes
        self.key_changes_to_ingest = resource_key_changes
        self.get_deletes_cv_with_deltas = get_deletes_cv_with_deltas

        # Populate DAG params with optionally-defined resources and descriptors; default to empty-list (i.e., run all).
        dag_params = {
            "full_refresh": Param(
                default=False,
                type="boolean",
                description="If true, deletes endpoint data in Databricks before ingestion"
            ),
            "endpoints": Param(
                default=sorted(list(self.resources | self.descriptors)),
                type="array",
                description="Newline-separated list of specific endpoints to ingest (case-agnostic)\n(Bug: even if unused, enter a newline)"
            ),
        }

        user_defined_macros = {
            "is_scheduled_full_refresh": partial(airflow_util.run_matches_cron,
                                                 cron=self.schedule_interval_full_refresh)
        }

        self.dag = EACustomDAG(params=dag_params, user_defined_macros=user_defined_macros, **kwargs)

    def build_endpoint_configs(self, enabled: bool = True, fetch_deletes: bool = True, **kwargs):
        """
        Unify kwargs with default config arguments.
        Add schoolYear filter in multiYear ODSes.
        `enabled` and `fetch_deletes` are not passed into configs.
        """
        configs = {**self.DEFAULT_CONFIGS, **kwargs}
        configs["query_parameters"] = copy.deepcopy(
            configs["query_parameters"])  # Prevent query_parameters from being shared across DAGs.

        ### For a multiyear ODS, we need to specify school year as an additional query parameter.
        # (This is an exception-case; we push all tenants to build year-specific ODSes when possible.)
        if self.multiyear:
            configs['query_parameters']['schoolYear'] = self.api_year

        logging.info(f"Configurations: {configs}")
        return configs

    def parse_endpoint_configs(self, raw_configs: Optional[Union[dict, list]] = None) -> Tuple[
        Dict[str, dict], Set[str], Set[str]]:
        """
        Parse endpoint configs into kwarg bundles if passed.
        Keep list of deletes and keyChanges to fetch.
        Force all endpoints to snake-case for consistency.
        Return only enabled configs (enabled by default).
        """
        if not raw_configs:
            return {}, set(), set()

        # A dictionary of endpoints has been passed with run-metadata.
        elif isinstance(raw_configs, dict):
            configs = {}
            deletes = set()
            key_changes = set()

            for endpoint, kwargs in raw_configs.items():
                if not kwargs.get('enabled', True):
                    continue

                snake_endpoint = camel_to_snake(endpoint)
                configs[snake_endpoint] = self.build_endpoint_configs(**kwargs)
                if kwargs.get('fetch_deletes'):
                    deletes.add(snake_endpoint)
                    key_changes.add(snake_endpoint)

            return configs, deletes, key_changes

        # A list of resources has been passed without run-metadata
        elif isinstance(raw_configs, list):
            # Use default configs and mark all as enabled with deletes/keyChanges.
            configs = {camel_to_snake(endpoint): self.build_endpoint_configs(namespace=ns) for endpoint, ns in
                       raw_configs}
            deletes = set(configs.keys())
            key_changes = set(configs.keys())
            return configs, deletes, key_changes

        else:
            raise ValueError(
                f"Passed configs are an unknown datatype! Expected Dict[endpoint: metadata] or List[(namespace, endpoint)] but received {type(configs)}"
            )

    # Original methods to manually build task-groups (deprecated in favor of `resource_configs` and `descriptor_configs` DAG arguments).
    def add_resource(self, resource: str, **kwargs):
        if kwargs.get('enabled', True):
            snake_resource = camel_to_snake(resource)
            self.resources.add(snake_resource)
            self.endpoint_configs[snake_resource] = self.build_endpoint_configs(**kwargs)

    def add_descriptor(self, resource: str, **kwargs):
        if kwargs.get('enabled', True):
            snake_resource = camel_to_snake(resource)
            self.descriptors.add(snake_resource)
            self.endpoint_configs[snake_resource] = self.build_endpoint_configs(**kwargs)

    def add_resource_deletes(self, resource: str, **kwargs):
        if kwargs.get('enabled', True):
            snake_resource = camel_to_snake(resource)
            self.deletes_to_ingest.add(snake_resource)
            self.key_changes_to_ingest.add(snake_resource)

    def chain_task_groups_into_dag(self):
        """
        Chain the optional endpoint task groups with the change-version operator and DBT incrementer if defined.

        Originally, we chained the empty task groups at init, but tasks are only registered if added to the group before downstream dependencies.
        See `https://github.com/apache/airflow/issues/16764` for more information.

        Ideally, we'd use `airflow.util.helpers.chain()`, but Airflow2.6 logs dependency warnings when chaining already-included tasks.
        See `https://github.com/apache/airflow/discussions/20693` for more information.

        :return:
        """
        ### Initialize resource and descriptor task groups if configs are defined.
        if self.run_type == 'default':
            task_group_callable = self.build_default_edfi_to_databricks_task_group
        elif self.run_type == 'dynamic':
            task_group_callable = self.build_dynamic_edfi_to_databricks_task_group
        elif self.run_type == 'bulk':
            task_group_callable = self.build_bulk_edfi_to_databricks_task_group
        else:
            raise ValueError(f"Run type {self.run_type} is not one of the expected values: [default, dynamic, bulk].")

        # Set parent directory and create subfolders for each task group.
        adls_parent_directory = os.path.join(
            self.tenant_code, str(self.api_year), "{{ ds_nodash }}", "{{ ts_nodash }}"
        )

        # Resources
        resources_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi_Resources",
            endpoints=sorted(list(self.resources)),
            adls_destination_dir=os.path.join(adls_parent_directory, 'resources')
            # Tables are built dynamically from the names of the endpoints.
        )

        # Descriptors
        descriptors_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi_Descriptors",
            endpoints=sorted(list(self.descriptors)),
            table=self.descriptors_table,
            adls_destination_dir=os.path.join(adls_parent_directory, 'descriptors')
        )

        # Resource Deletes
        resource_deletes_task_group: Optional[TaskGroup] = task_group_callable(
            group_id="Ed-Fi_Resource_Deletes",
            endpoints=sorted(list(self.deletes_to_ingest)),
            table=self.deletes_table,
            adls_destination_dir=os.path.join(adls_parent_directory, 'resource_deletes'),
            get_deletes=True,
            get_with_deltas=self.get_deletes_cv_with_deltas
        )

        # Resource Key-Changes (only applicable in Ed-Fi v6.x and up)
        if self.get_key_changes:
            resource_key_changes_task_group: Optional[TaskGroup] = task_group_callable(
                group_id="Ed-Fi Resource Key Changes",
                endpoints=sorted(list(self.key_changes_to_ingest)),
                table=self.key_changes_table,
                adls_destination_dir=os.path.join(adls_parent_directory, 'resource_key_changes'),
                get_key_changes=True
            )
        else:
            resource_key_changes_task_group = None

        ### Chain Ed-Fi task groups into the DAG between CV operators and Airflow state operators.
        edfi_task_groups = [
            resources_task_group,
            descriptors_task_group,
            resource_deletes_task_group,
            resource_key_changes_task_group,
        ]

        # Retrieve current and previous change versions to define an ingestion window.
        if self.use_change_version:
            cv_task_group: TaskGroup = self.build_change_version_task_group()
        else:
            cv_task_group = None

        # Build an operator to increment the DBT var at the end of the run.
        if self.dbt_incrementer_var:
            dbt_var_increment_operator = PythonOperator(
                task_id='increment_dbt_variable',
                python_callable=update_variable,
                op_kwargs={
                    'var': self.dbt_incrementer_var,
                    'value': lambda x: int(x) + 1,
                },
                trigger_rule='one_success',
                dag=self.dag
            )
        else:
            dbt_var_increment_operator = None

        # Create a dummy sentinel to display the success of the endpoint taskgroups.
        dag_state_sentinel = PythonOperator(
            task_id='dag_state_sentinel',
            python_callable=airflow_util.fail_if_any_task_failed,
            trigger_rule='all_done',
            dag=self.dag
        )

        # Chain tasks and taskgroups into the DAG; chain sentinel after all task groups.
        airflow_util.chain_tasks(cv_task_group, edfi_task_groups, dbt_var_increment_operator)
        airflow_util.chain_tasks(edfi_task_groups, dag_state_sentinel)

    ### Internal methods that should probably not be called directly.
    def build_change_version_task_group(self) -> TaskGroup:
        """

        :return:
        """
        with TaskGroup(
                group_id="Ed-Fi3 Change Version Window",
                prefix_group_id=False,
                parent_group=None,
                dag=self.dag
        ) as cv_task_group:
            # Pull the newest change version recorded in Ed-Fi.
            get_newest_edfi_cv = PythonOperator(
                task_id=self.newest_edfi_cv_task_id,  # Class attribute for easier XCom retrieval
                python_callable=change_version.get_newest_edfi_change_version,
                op_kwargs={
                    'edfi_conn_id': self.edfi_conn_id,
                },
                dag=self.dag
            )

            # Reset the Databricks change version table (if a full-refresh).
            reset_databricks_cvs = PythonOperator(
                task_id="reset_previous_change_versions_in_databricks",
                python_callable=change_version.reset_change_versions,
                op_kwargs={
                    'tenant_code': self.tenant_code,
                    'api_year': self.api_year,
                    'databricks_conn_id': self.databricks_conn_id,
                    'change_version_table': self.change_version_table,
                },
                trigger_rule='all_success',
                dag=self.dag
            )

            get_newest_edfi_cv >> reset_databricks_cvs

        return cv_task_group

    def build_change_version_get_operator(self,
                                          task_id: str,
                                          endpoints: List[Tuple[str, str]],
                                          get_deletes: bool = False,
                                          get_key_changes: bool = False,
                                          get_with_deltas: bool = True
                                          ) -> PythonOperator:
        """

        :return:
        """
        get_cv_operator = PythonOperator(
            task_id=task_id,
            python_callable=change_version.get_previous_change_versions_with_deltas if get_with_deltas else change_version.get_previous_change_versions,
            op_kwargs={
                'tenant_code': self.tenant_code,
                'api_year': self.api_year,
                'endpoints': endpoints,
                'databricks_conn_id': self.databricks_conn_id,
                'change_version_table': self.change_version_table,
                'get_deletes': get_deletes,
                'get_key_changes': get_key_changes,
                'edfi_conn_id': self.edfi_conn_id,
                'max_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
            },
            trigger_rule='none_failed',  # Run regardless of whether the CV table was reset.
            dag=self.dag
        )

        # One or more endpoints can fail total-get count. Create a second operator to track that failed status.
        # This should NOT be necessary, but we encountered a bug where a downstream "none_skipped" task skipped with "upstream_failed" status.
        def fail_if_xcom(xcom_value, **context):
            if xcom_value:
                raise AirflowFailException(f"The following endpoints failed when pulling total counts: {xcom_value}")
            else:
                raise AirflowSkipException  # Force a skip to not mark the taskgroup as a success when all tasks skip.

        failed_sentinel = PythonOperator(
            task_id=f"{task_id}__failed_total_counts",
            python_callable=fail_if_xcom,
            op_args=[airflow_util.xcom_pull_template(get_cv_operator, key='failed_endpoints')],
            trigger_rule='all_done',
            dag=self.dag
        )
        get_cv_operator >> failed_sentinel

        return get_cv_operator

    def build_change_version_update_operator(self,
                                             task_id: str,
                                             endpoints: List[str],
                                             get_deletes: bool,
                                             get_key_changes: bool,
                                             **kwargs
                                             ) -> PythonOperator:
        """

        :return:
        """
        return PythonOperator(
            task_id=task_id,
            python_callable=change_version.update_change_versions,
            op_kwargs={
                'tenant_code': self.tenant_code,
                'api_year': self.api_year,
                'databricks_conn_id': self.databricks_conn_id,
                'change_version_table': self.change_version_table,

                'edfi_change_version': airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                'endpoints': endpoints,
                'get_deletes': get_deletes,
                'get_key_changes': get_key_changes,
            },
            provide_context=True,
            dag=self.dag,
            **kwargs
        )

    # Polymorphic Ed-Fi TaskGroups
    @staticmethod
    def xcom_pull_template_map_idx(task_ids, idx: int):
        """
        Many XComs in this DAG are lists of tuples. This overloads xcom_pull_template to retrieve a list of items at a given index.
        """
        return airflow_util.xcom_pull_template(
            task_ids, suffix=f" | map(attribute={idx}) | list"
        )

    @staticmethod
    def xcom_pull_template_get_key(task_ids, key: str):
        """
        Many XComs in this DAG are lists of tuples. This converts the XCom to a dictionary and returns the value for a given key.
        """
        return airflow_util.xcom_pull_template(
            task_ids, prefix="dict(", suffix=f").get('{key}')"
        )

    def build_default_edfi_to_databricks_task_group(self,
                                                    endpoints: List[str],
                                                    group_id: str,

                                                    *,
                                                    adls_destination_dir: str,
                                                    table: Optional[str] = None,
                                                    get_deletes: bool = False,
                                                    get_key_changes: bool = False,
                                                    get_with_deltas: bool = True,
                                                    **kwargs
                                                    ) -> TaskGroup:
        """
        Build one EdFiToS3 task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        :param get_with_deltas:
        :param endpoints:
        :param group_id:
        :param adls_destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :return:
        """
        if not endpoints:
            return None

        with TaskGroup(
                group_id=group_id,
                prefix_group_id=True,
                parent_group=None,
                dag=self.dag
        ) as default_task_group:

            ### EDFI3 CHANGE_VERSION LOGIC
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"get_last_change_versions_from_databricks",
                    endpoints=[(self.endpoint_configs[endpoint]['namespace'], endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    get_with_deltas=get_with_deltas
                )
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)
            else:
                get_cv_operator = None
                enabled_endpoints = endpoints

            ### EDFI TO ADLS: Output Tuple[endpoint, filename] per successful task
            pull_operators_list = []

            for endpoint in endpoints:
                pull_edfi_to_adls = EdFiToADLSOperator(
                    task_id=endpoint,
                    edfi_conn_id=self.edfi_conn_id,
                    resource=endpoint,

                    tmp_dir=self.tmp_dir,
                    adls_conn_id=self.adls_conn_id,
                    adls_destination_dir=adls_destination_dir,
                    adls_destination_filename=f"{endpoint}.jsonl",

                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    min_change_version=self.xcom_pull_template_get_key(get_cv_operator, endpoint) if get_cv_operator else None,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                    reverse_paging=self.get_deletes_cv_with_deltas if get_deletes else True,

                    # Optional config-specified run-attributes (overridden by those in configs)
                    **self.endpoint_configs[endpoint],

                    # Only run endpoints specified at DAG or delta-level.
                    enabled_endpoints=enabled_endpoints,

                    pool=self.pool,
                    trigger_rule='none_skipped',
                    dag=self.dag
                )

                pull_operators_list.append(pull_edfi_to_adls)

            copy_adls_to_databricks = BulkADLSToDatabricksOperator(
                task_id=f"copy_all_endpoints_into_databricks",
                tenant_code=self.tenant_code,
                api_year=self.api_year,

                resource=self.xcom_pull_template_map_idx(pull_operators_list, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_operators_list, 0),
                edfi_conn_id=self.edfi_conn_id,
                databricks_conn_id=self.databricks_conn_id,
                adls_destination_key=self.xcom_pull_template_map_idx(pull_operators_list, 1),
                adls_storage_account=self.adls_storage_account,
                adls_container=self.adls_container,
                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_databricls",
                    endpoints=self.xcom_pull_template_map_idx(pull_operators_list, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_operators_list, copy_adls_to_databricks, update_cv_operator)

        return default_task_group

    def build_dynamic_edfi_to_databricks_task_group(self,
                                                    endpoints: List[str],
                                                    group_id: str,

                                                    *,
                                                    adls_destination_dir: str,
                                                    table: Optional[str] = None,
                                                    get_deletes: bool = False,
                                                    get_key_changes: bool = False,
                                                    get_with_deltas: bool = True,

                                                    **kwargs
                                                    ):
        """
        Build one EdFiToS3 task per endpoint
        Bulk copy the data to its respective table in Snowflake.

        :param get_with_deltas:
        :param endpoints:
        :param group_id:
        :param adls_destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :return:
        """
        if not endpoints:
            return None

        with TaskGroup(
                group_id=group_id,
                prefix_group_id=True,
                parent_group=None,
                dag=self.dag
        ) as dynamic_task_group:

            # If change versions are enabled, dynamically expand the output of the CV operator task into the Ed-Fi partial.
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"get_last_change_versions_from_databricks",
                    endpoints=[(self.endpoint_configs[endpoint]['namespace'], endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    get_with_deltas=get_with_deltas
                )
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)
                kwargs_dicts = get_cv_operator.output.map(lambda endpoint__cv: {
                    'resource': endpoint__cv[0],
                    'min_change_version': endpoint__cv[1],
                    'adls_destination_filename': f"{endpoint__cv[0]}.json",
                    **self.endpoint_configs[endpoint__cv[0]],
                })

            # Otherwise, iterate all endpoints.
            else:
                get_cv_operator = None
                enabled_endpoints = endpoints
                kwargs_dicts = list(map(lambda endpoint: {
                    'resource': endpoint,
                    'min_change_version': None,
                    'adls_destination_filename': f"{endpoint}.json",
                    **self.endpoint_configs[endpoint],
                }, endpoints))

            ### EDFI TO adls: Output Tuple[endpoint, filename] per successful task
            pull_edfi_to_adls = (EdFiToADLSOperator
                                 .partial(
                task_id=f"pull_dynamic_endpoints_to_s3",
                edfi_conn_id=self.edfi_conn_id,

                tmp_dir=self.tmp_dir,
                adls_conn_id=self.adls_conn_id,
                adls_destination_dir=adls_destination_dir,

                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                    reverse_paging=self.get_deletes_cv_with_deltas if get_deletes else True,

                # Only run endpoints specified at DAG or delta-level.
                enabled_endpoints=enabled_endpoints,

                sla=None,  # "SLAs are unsupported with mapped tasks."
                pool=self.pool,
                trigger_rule='none_skipped',
                dag=self.dag
            )
                                 .expand_kwargs(kwargs_dicts)
                                 )

            copy_adls_to_databricks = BulkADLSToDatabricksOperator(
                task_id=f"copy_all_endpoints_into_databricks",
                tenant_code=self.tenant_code,
                api_year=self.api_year,

                resource=self.xcom_pull_template_map_idx(pull_edfi_to_adls, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_edfi_to_adls, 0),
                edfi_conn_id=self.edfi_conn_id,
                databricks_conn_id=self.databricks_conn_id,
                adls_destination_key=self.xcom_pull_template_map_idx(pull_edfi_to_adls, 1),
                adls_storage_account=self.adls_storage_account,
                adls_container=self.adls_container,
                trigger_rule='all_done',
                dag=self.dag
            )

            ### UPDATE DATABRICKS CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_databricks",
                    endpoints=self.xcom_pull_template_map_idx(pull_edfi_to_adls, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_edfi_to_adls, copy_adls_to_databricks, update_cv_operator)

        return dynamic_task_group

    def build_bulk_edfi_to_databricks_task_group(self,
                                                 endpoints: List[str],
                                                 group_id: str,

                                                 *,
                                                 adls_destination_dir: str,
                                                 table: Optional[str] = None,
                                                 get_deletes: bool = False,
                                                 get_key_changes: bool = False,
                                                 get_with_deltas: bool = True,
                                                 **kwargs
                                                 ):
        """
        Build one EdFiToS3 task (with inner for-loop across endpoints).
        Bulk copy the data to its respective table in Snowflake.

        :param get_with_deltas:
        :param endpoints:
        :param group_id:
        :param adls_destination_dir:
        :param table:
        :param get_deletes:
        :param get_key_changes:
        :return:
        """
        if not endpoints:
            return None

        with TaskGroup(
                group_id=group_id,
                prefix_group_id=True,
                parent_group=None,
                dag=self.dag
        ) as bulk_task_group:

            # If change versions are enabled, dynamically expand the output of the CV operator task into the Ed-Fi bulk operator.
            if self.use_change_version:
                get_cv_operator = self.build_change_version_get_operator(
                    task_id=f"get_last_change_versions",
                    endpoints=[(self.endpoint_configs[endpoint]['namespace'], endpoint) for endpoint in endpoints],
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    get_with_deltas=get_with_deltas
                )
                min_change_versions = [
                    self.xcom_pull_template_get_key(get_cv_operator, endpoint)
                    for endpoint in endpoints
                ]
                enabled_endpoints = self.xcom_pull_template_map_idx(get_cv_operator, 0)

            # Otherwise, iterate all endpoints.
            else:
                get_cv_operator = None
                min_change_versions = [None] * len(endpoints)
                enabled_endpoints = endpoints

            # Build a dictionary of lists to pass into bulk operator.
            endpoint_config_lists = {
                key: [self.endpoint_configs[endpoint][key] for endpoint in endpoints]
                for key in self.DEFAULT_CONFIGS.keys()  # Create lists for all keys in config.
            }

            pull_edfi_to_adls = BulkEdFiToADLSOperator(
                task_id=f"pull_all_endpoints_to_adls",
                edfi_conn_id=self.edfi_conn_id,

                tmp_dir=self.tmp_dir,
                adls_conn_id=self.databricks_conn_id,
                adls_destination_dir=adls_destination_dir,

                get_deletes=get_deletes,
                get_key_changes=get_key_changes,
                max_change_version=airflow_util.xcom_pull_template(self.newest_edfi_cv_task_id),
                reverse_paging=self.get_deletes_cv_with_deltas if get_deletes else True,

                # Arguments that are required to be lists in Ed-Fi bulk-operator.
                resource=endpoints,
                min_change_version=min_change_versions,
                adls_destination_filename=[f"{endpoint}.jsonl" for endpoint in endpoints],

                # Optional config-specified run-attributes (overridden by those in configs)
                **endpoint_config_lists,

                # Only run endpoints specified at DAG or delta-level.
                enabled_endpoints=enabled_endpoints,

                pool=self.pool,
                trigger_rule='none_skipped',
                dag=self.dag
            )

            ### COPY FROM S3 TO SNOWFLAKE
            copy_adls_to_databricks = BulkADLSToDatabricksOperator(
                task_id=f"copy_all_endpoints_into_databricks",
                tenant_code=self.tenant_code,
                api_year=self.api_year,

                resource=self.xcom_pull_template_map_idx(pull_edfi_to_adls, 0),
                table_name=table or self.xcom_pull_template_map_idx(pull_edfi_to_adls, 0),
                edfi_conn_id=self.edfi_conn_id,
                databricks_conn_id=self.databricks_conn_id,
                adls_destination_key=self.xcom_pull_template_map_idx(pull_edfi_to_adls, 1),
                adls_container=self.adls_container,
                adls_storage_account=self.adls_storage_account,
                trigger_rule='none_skipped',  # Different trigger rule than default.
                dag=self.dag
            )

            ### UPDATE SNOWFLAKE CHANGE VERSIONS
            if self.use_change_version:
                update_cv_operator = self.build_change_version_update_operator(
                    task_id=f"update_change_versions_in_databricks",
                    endpoints=self.xcom_pull_template_map_idx(pull_edfi_to_adls, 0),
                    get_deletes=get_deletes,
                    get_key_changes=get_key_changes,
                    trigger_rule='all_success'
                )
            else:
                update_cv_operator = None

            ### Chain tasks into final task-group
            airflow_util.chain_tasks(get_cv_operator, pull_edfi_to_adls, copy_adls_to_databricks, update_cv_operator)

        return bulk_task_group
