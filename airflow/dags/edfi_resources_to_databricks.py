import importlib
import logging

from tn_edu_airflow.util import io_helpers

from airflow.utils.task_group import TaskGroup

from tn_edu_airflow.dags.tn_edfi_resource_dag import TNEdFiResourceDAG

### Optimizing DAG parsing delays during execution (only Airflow 2.4+)
if importlib.metadata.version('apache-airflow') >= '2.4':
    from airflow.utils.dag_parsing_context import get_parsing_context
    __current_dag_id__ = get_parsing_context().dag_id
else:
    __current_dag_id__ = None


# Turning on descriptor DAGs doubles the total number processed by Airflow.
# Only turn these on if they're actually being used.
INGEST_DESCRIPTORS = True


#configs_dir =  '/home/airflow/airflow/configs'
configs_dir = '/opt/airflow/config'

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
        resources_dag_id = f"TN_edfi_el_{tenant_code}_{api_year}_resources"

        # Optimizing DAG parsing delays during execution
        if __current_dag_id__ and __current_dag_id__ != resources_dag_id:
            continue  # skip generation of non-selected DAG

        # Reassign `schedule_interval` if a resource-specific value has been provided.
        dag_vars['schedule_interval'] = dag_vars.get('schedule_interval_resources') or dag_vars.get('schedule_interval')
        logging.info(f"API YEAR:{api_year}")

        resources_dag = TNEdFiResourceDAG(
            dag_id=resources_dag_id,
            tenant_code=tenant_code,
            api_year=api_year,
            **dag_vars
        )

        for endpoint, endpoint_vars in EDFI_RESOURCES.items():

            #Not all resources must be ingested per DAG run.
            if not endpoint_vars.get('enabled'):
                continue

            resources_dag.add_resource(endpoint, **endpoint_vars)

        # Chain task groups at the end of endpoints being added to ensure they are included in dependencies.
        resources_dag.chain_task_groups_into_dag()

        globals()[resources_dag.dag.dag_id] = resources_dag.dag
