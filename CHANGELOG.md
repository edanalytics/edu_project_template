# Unreleased
- Ensure that all resources that are required by `edu_edfi_source` are included in `src_edfi_3.yml` and `sql_source_create_table.sql`

# edu_project_template v0.2.1
## Changes
- Pinned `dbt-core` to `>=1.10.8,<2.0.0` in the venv setup script so new environments install a version compatible with `edu_wh`, rather than just pulling the latest.
- Cleaned up dbt deprecation warnings in the source and codegen templates by moving shared column definitions under `anchors` and `enabled` under `config`. These changes don't affect the generated tables.

## Under the hood
- Removed the deprecated `target-path` setting from `dbt_project.yml` since `target` is already the default.

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
