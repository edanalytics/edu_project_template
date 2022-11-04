echo Beginning venv-airflow-init.sh

### set up airflow environment ###
python3.8 -m pip install --upgrade pip
pip install wheel setuptools  --quiet

# install airflow, modeled on: https://airflow.apache.org/docs/apache-airflow/2.1.2/installation.html#installation-script
# we should probably pass in airflow (and python version) as args to cloudformation template
# debugging; we were asking python for its version number, which is weird because we set it above... might as well just
# use the same param eventually to reference here too. for some reason using python (no version) inside the herdoc block didn't work.
AIRFLOW_VERSION=2.3.4
PYTHON_VERSION=3.10
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[amazon, snowflake, slack, postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"  --quiet
pip install airflow-dbt

# Install EA-internal packages
pip install -e git+https://github.com/edanalytics/ea_airflow_util.git#egg=ea_airflow_util
pip install -e git+https://github.com/edanalytics/edfi_airflow.git#egg=edfi_airflow
pip install -e git+https://github.com/edanalytics/edfi_api.git#egg=edfi_api
