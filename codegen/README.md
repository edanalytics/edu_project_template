# Overview

There are a handful of configuration files that are required by this project should be generated from an existing Ed-Fi API's swagger endpoints. 
This code generation allows each EDU implementation to correspond to a particular Ed-Fi ODS' potentially unique set of API endpoints.

# Running the codegen
- Set up a local Python environment with the [edfi_api_client](https://github.com/edanalytics/edfi_api_client) installed
- Navigate to the codegen directory in your fork of this repo
- Run `python main.py {your Ed-Fi base URL}` in the terminal

# Using the Outputs
This script will output a handful of files in the `generated` folder. These are necessary for configuring Airflow and DBT and for creating the create-table statements for raw tables where raw Ed-Fi data will land.

`src_edfi_3.yml` declares the dbt [sources](https://docs.getdbt.com/docs/build/sources), which are the `raw` tables that are referenced in [edu_edfi_source](https://github.com/edanalytics/edu_edfi_source) staging models.
Append the contents of the generated file to the end of [/dbt/models/staging/src_edfi_3.yml](/dbt/models/staging/src_edfi_3.yml).

`sql_source_create_table.sql` is a series of SQL CREATE TABLE statements to create `raw` tables where pulled Ed-Fi data will be written. This will need to be altered and run in each of your environment databases in Snowflake
(i.e., `raw` and `dev_raw`).

`edfi_resources.yml` and `edfi_descriptors.yml` are lists of endpoints extracted from the ODS' Swagger API, each with metadata regarding namespacing and the presence of a deletes endpoint. These files can be manually edited to alter pagination logic or to disable endpoints that are outside claimset scope. These files should be copied as is to [/airflow/configs/](/airflow/configs).
