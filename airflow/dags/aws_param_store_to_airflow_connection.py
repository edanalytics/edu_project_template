from util import io_helpers

from ea_airflow_util import AWSParamStoreToAirflowDAG


configs_dir = '/home/airflow/airflow/configs'

# Load Airflow project-level configs to define dynamic DAGs.
airflow_configs_file = 'airflow_config.yml'
airflow_configs = io_helpers.safe_load_yaml(configs_dir, airflow_configs_file)

dag_args = airflow_configs.get('aws_param_store_dag')
if dag_args is None:
    raise Exception(
        "Necessary parameter `aws_param_store_dag` is missing from the configs file!"
    )


#
dag_id = 'aws_param_store_to_airflow_connections'

param_store_dag = AWSParamStoreToAirflowDAG(
    dag_id = dag_id,
    **dag_args
)

# param_store_dag.globalize()
globals()[param_store_dag.dag.dag_id] = param_store_dag.dag
