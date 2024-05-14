from ea_airflow_util.dags.airflow_db_clean_dag import AirflowDBCleanDAG
from util import io_helpers


# Load Airflow project-level configs to define dynamic DAGs.
configs_dir =  '/home/airflow/airflow/configs'
airflow_configs_file = 'airflow_config.yml'

AIRFLOW_CONFIGS = io_helpers.safe_load_yaml(configs_dir, airflow_configs_file)
dag_params = AIRFLOW_CONFIGS.get('airflow_db_clean')

cleanup_dag = AirflowDBCleanDAG(**dag_params)
globals()[cleanup_dag.dag.dag_id] = cleanup_dag.dag
