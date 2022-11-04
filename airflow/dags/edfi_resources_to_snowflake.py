from util import dag_util
from util import io_helpers

from edu_edfi_airflow import EdFiResourceDAG


# Mapping resources to domains made sense before we upgraded Airflow to 2.0+ (i.e., Grid view).
# By default, toggle off this mapping. (Maybe we'll find use for it again in the future.)
GROUP_TASKS_BY_DOMAIN = False


configs_dir =  '/home/airflow/airflow/configs'

# Load Airflow project-level configs to define dynamic DAGs.
airflow_configs_file = 'airflow_config.yml'
AIRFLOW_CONFIGS = io_helpers.safe_load_yaml(configs_dir, airflow_configs_file)

# Load Ed-Fi resources and descriptors, as extracted from Swagger by `edfi_api.generate_templates()`.
# Default to empty dicts if file not yet generated.
edfi_resources_file = 'edfi_resources.yml'
EDFI_RESOURCES = io_helpers.safe_load_yaml(configs_dir, edfi_resources_file, default={})

edfi_descriptors_file = 'edfi_descriptors.yml'
EDFI_DESCRIPTORS = io_helpers.safe_load_yaml(configs_dir, edfi_descriptors_file, default={})

# Load Ed-Fi resource/descriptor domain mappings used to streamline the graph-view of the DAG in the UI.
# Default to None (i.e., no domain task groups) if file is missing.
edfi_domain_mapping_file = 'edfi_domain_mapping.yml'
EDFI_DOMAIN_MAPPING = io_helpers.safe_load_yaml(configs_dir, edfi_domain_mapping_file, default=None)


# DAG-declarations are declared under `dags`.
dag_params = AIRFLOW_CONFIGS.get('edfi_resource_dags')
if dag_params is None:
    raise Exception(
        "Necessary parameter `edfi_resource_dags` is missing from the configs file!"
    )

# Build one resource DAG and one descriptor DAG per tenant-year combination.
for tenant_code, api_year_vars in dag_params.items():

    for api_year, dag_vars in api_year_vars.items():

        ### EdFi Resources DAG: One table per resource
        resources_dag_id = f"edfi_el_{tenant_code}_{api_year}_resources"

        # Reassign `schedule_interval` if a resource-specific value has been provided.
        dag_vars['schedule_interval'] = dag_vars.get('schedule_interval_resources') or dag_vars.get('schedule_interval')

        resources_dag = EdFiResourceDAG(
            dag_id=resources_dag_id,
            tenant_code=tenant_code,
            api_year=api_year,
            **dag_vars
        )

        dag_util.assign_endpoints_to_edfi_dag(
            resources_dag,
            EDFI_RESOURCES,
            domain_mapping=EDFI_DOMAIN_MAPPING if GROUP_TASKS_BY_DOMAIN else None,
            get_deletes=True,
            use_change_version=dag_vars.get('use_change_version', True)
        )

        globals()[resources_dag.dag.dag_id] = resources_dag.dag


        ### EdFi Descriptors DAG: One `descriptors` table
        # Note: Descriptors do not have deletes.
        descriptors_dag_id = f"edfi_el_{tenant_code}_{api_year}_descriptors"

        # Reassign `schedule_interval` if a descriptors-specific value has been provided.
        dag_vars['schedule_interval'] = dag_vars.get('schedule_interval_descriptors') or dag_vars.get('schedule_interval')

        descriptors_dag = EdFiResourceDAG(
            dag_id=descriptors_dag_id,
            tenant_code=tenant_code,
            api_year=api_year,
            full_refresh=True,  # Descriptors should be reset at every run.
            **dag_vars
        )

        dag_util.assign_endpoints_to_edfi_dag(
            descriptors_dag,
            EDFI_DESCRIPTORS,
            domain_mapping=EDFI_DOMAIN_MAPPING if GROUP_TASKS_BY_DOMAIN else None,
            table="_descriptors",
            get_deletes=False,
            use_change_version=False
        )

        globals()[descriptors_dag.dag.dag_id] = descriptors_dag.dag
