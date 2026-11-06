echo Beginning venv-dbt-init.sh

### set up dbt environment ###
python3 -m pip install --upgrade pip
pip install wheel setuptools  --quiet
# keep in sync with edu_wh's require-dbt-version
pip install "dbt-core>=1.10.8,<2.0.0" dbt-postgres dbt-snowflake  --quiet
