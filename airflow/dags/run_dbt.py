from util import io_helpers

from ea_airflow_util import RunDbtDag


configs_dir = '/home/airflow/airflow/configs'

# Load Airflow project-level configs to define dynamic DAGs.
airflow_configs_file = 'airflow_config.yml'
airflow_configs = io_helpers.safe_load_yaml(configs_dir, airflow_configs_file)

if dag_params := airflow_configs.get('dbt_run_dags'):

    # Build one DBT dag per environment.
    for environment, environment_vars in dag_params.items():

        #
        dag_id = f"run_dbt_{environment}"

        dbt_dag = RunDbtDag(
            dag_id = dag_id,
            environment=environment,
            **environment_vars
        )

        dbt_dag.build_dbt_run()

        # param_store_dag.globalize()
        globals()[dbt_dag.dag.dag_id] = dbt_dag.dag
