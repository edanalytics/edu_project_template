from typing import Optional

from airflow_dbt.operators.dbt_operator import DbtSnapshotOperator

from ea_airflow_util.dags.ea_custom_dag import EACustomDAG


class DbtSnapshotDag:
    """
    :param dbt_repo_path:
    :param dbt_target_name:
    :param dbt_bin_path:
    :param slack_conn_id:
    """
    def __init__(self,
        # required dbt paths and target
        dbt_repo_path  : str,
        dbt_target_name: str,
        dbt_bin_path   : str,
        **kwargs
    ):
        # dbt paths
        self.dbt_repo_path = dbt_repo_path
        self.dbt_target_name = dbt_target_name
        self.dbt_bin_path = dbt_bin_path
        self.dag = EACustomDAG(**kwargs)

    
    def dbt_snapshot_run(self, on_success_callback=None, **kwargs):
        dbt_snapshot_task = DbtSnapshotOperator(
            task_id= f'dbt_snapshot',
            dir    = self.dbt_repo_path,
            target = self.dbt_target_name,
            dbt_bin= self.dbt_bin_path,
            dag=self.dag
        )
