echo Beginning venv-dbt-init.sh

### set up dbt environment ###
python3 -m pip install --upgrade pip
pip install wheel setuptools  --quiet
pip install dbt-core dbt-postgres dbt-snowflake  --quiet
