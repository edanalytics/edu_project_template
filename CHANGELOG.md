# edu_project_template v0.2.0
## New features
- Add optional `dbt_incrementer_var` to `edu_edfi_airflow.EdFiResourceDAG` and `ea_airflow_util.RunDbtDag` in Airflow config YAMLs
- Refactor `ea_airflow_util.AWSParamStoreToAirflowDAG` arguments in Airflow config YAMLs to match new structure
- Add boolean flag to make compiling Ed-Fi descriptor DAGs optional
- Move Git-repo installation in EC2 from venv subfolder to `code` folder
- Add SSH and SFTP Airflow providers to default provider list at installation
- Add `pysftp` library at installation
- Add `earthmover` and `lightbeam` libraries at installation

## Under the hood
- Refactor `EdFiResourceDAG` instantiation code to match new refactor
- Add dynamic-DAG optimization logic in `EdFiResourceDAG` instantiation code (only applied in Airflow 2.4+)
- Refactor codegen code to use new `edfi_api_client` features
- Make Python version in `venv-airflow-init.sh` defined at runtime

## Fixes
- Fix bug where Python version in `venv-airflow-init.sh` was mislabeled
