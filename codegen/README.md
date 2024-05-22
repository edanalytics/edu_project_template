# Overview

There are a handful of configuration files that are required by this project which can be code-generated from an existing Ed-Fi API's swagger endpoints. 
This code generation allows the project to react to a particular Ed-Fi environment's potentially unique set of API endpoints.

# Running the codegen
- Set up a local Python environment with the [edfi_api_client](https://github.com/edanalytics/edfi_api_client) installed
- Navigate to the codegen directory in your fork of this repo
- Run `python main.py {your Ed-Fi base URL}

# Using the Outputs
This script will produce a handful of output files in the `generated` folder, necessary for configuring Airflow and DBT, and for creating the table shells where raw Ed-Fi data will land.

`src_edfi_3.yml` declares the dbt [sources](https://docs.getdbt.com/docs/build/sources), which are the `raw` tables outside of dbt's control that the dbt models will source their data from.
Append the contents of the generated file to the end of [/dbt/models/staging/src_edfi_3.yml](/dbt/models/staging/src_edfi_3.yml).

`sql_source_create_table.sql` is a series of SQL CREATE TABLE statements that will create all the table shells into which raw Ed-Fi data will be written. This will need to be run on your target database 
(once for each environment, such as `raw`, `dev_raw`, `test_raw`).

`edfi_resources.yml` and `edfi_descriptors.yml` can be copied as is to [/airflow/configs/](/airflow/configs). These configure which endpoints your Airflow dags will sync into the warehouse when they run.
They can be further configured to disable individual resources that are not in use or adjust the page-size.
