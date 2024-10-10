from tn_edu_airflow.util import io_helpers

from ea_airflow_util import UpdateDbtDocsDag

#configs_dir = '/home/airflow/airflow/configs'
configs_dir = '/opt/airflow/config'

# Load Airflow project-level configs to define dynamic DAGs.
airflow_configs_file = 'airflow_config.yml'
airflow_configs = io_helpers.safe_load_yaml(configs_dir, airflow_configs_file)

if dag_args := airflow_configs.get('dbt_docs_update'):

    dag_id = 'dbt_docs_update'
    dbt_docs_update_dag = UpdateDbtDocsDag(
        dag_id = dag_id,
        **dag_args
    )
    dbt_docs_update_dag.update_dbt_docs()

    globals()[dbt_docs_update_dag.dag.dag_id] = dbt_docs_update_dag.dag
