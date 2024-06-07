echo Beginning venv-dbt-init.sh

### set up dbt environment ###
python3.8 -m pip install --upgrade pip
pip install wheel setuptools  --quiet
pip install dbt-core==1.7.16 dbt-postgres==1.7.16 dbt-snowflake  --quiet
