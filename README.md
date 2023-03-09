# Stadium {Implementation}

This repository was created using the [EDU project template](https://github.com/edanalytics/edu_project_template). It contains code and configuration that extend EDU for  {Implementation}'s Stadium implementation. See our documentation of EDU at [enableDataUnion.org](https://enabledataunion.org).

## Navigating this code repository

### airflow folder
*Useful for: data engineers/airflow developers*

This folder contains the DAGs, python code, and configuration that power this implementation's Airflow instance. For example, configs here determine the schedule and instructions for the EdfiResourceDAG, which is imported from [edu_edfi_airflow](https://github.com/edanalytics/edu_edfi_airflow), and is used to pull from an Ed-Fi API and copy data into Snowflake. There are also DAGs imported from [ea_airflow_util](https://github.com/edanalytics/ea_airflow_util), like the RunDbtDag, which triggers dbt runs on a schedule.

### codegen folder
*Useful for: data engineers working on initial project setup*

This folder contains python scripts that use the Ed-Fi Swagger to generate code needed for initial project setup.

### dbt folder
*Useful for: data/analytics engineers or analysts looking to configure business rules or extend EDU for this implementation*

This folder contains the dbt project for this implementation. You will find csv configuration xwalks under `seeds`, sql scripts for implementation-specific database objects under `models`, and overall project configuration in `dbt_project.yml`. Note that package [edu_wh](https://github.com/edanalytics/edu_wh) is imported in `packages.yml`. 

You can read more about generic dbt project structure [here](https://docs.getdbt.com/docs/build/projects).

### init folder
*Useful for: data engineers working on initial project setup*

This folder contains bash scripts needed for initial setup of the python environment (run by cloud infrastructure).
