from util import io_helpers

from airflow.utils.task_group import TaskGroup

from edu_edfi_airflow import EdFiResourceDAG

# Turning on descriptor DAGs doubles the total number processed by Airflow.
# Only turn these on if they're actually being used.
INGEST_DESCRIPTORS = False


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

        for endpoint, endpoint_vars in EDFI_RESOURCES.items():

            # Not all resources must be ingested per DAG run.
            if not endpoint_vars.get('enabled'):
                continue

            resources_dag.add_resource(endpoint, **endpoint_vars)

            if endpoint_vars.get('fetch_deletes'):
                resources_dag.add_resource_deletes(endpoint, **endpoint_vars)

        globals()[resources_dag.dag.dag_id] = resources_dag.dag


        # Turning on descriptor DAGs doubles the total number processed by Airflow.
        # Only turn these on if they're actually being used.
        if INGEST_DESCRIPTORS:
            ### EdFi Descriptors DAG: One `descriptors` table
            # Note: Descriptors do not have deletes.
            descriptors_dag_id = f"edfi_el_{tenant_code}_{api_year}_descriptors"

            # Reassign `schedule_interval` if a descriptors-specific value has been provided.
            dag_vars['schedule_interval'] = dag_vars.get('schedule_interval_descriptors') or dag_vars.get('schedule_interval')

            descriptors_dag = EdFiResourceDAG(
                dag_id=descriptors_dag_id,
                tenant_code=tenant_code,
                api_year=api_year,
                use_change_version=False,  # Descriptors should be reset at every run.
                **dag_vars
            )

            for endpoint, endpoint_vars in EDFI_DESCRIPTORS.items():

                # Not all resources must be ingested per DAG run.
                if not endpoint_vars.get('enabled'):
                    continue

                descriptors_dag.add_descriptor(endpoint, **endpoint_vars)

            globals()[descriptors_dag.dag.dag_id] = descriptors_dag.dag
