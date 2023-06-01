# Set versions to install into Airflow virtual environment
AIRFLOW_VERSION=2.6.1

EA_AIRFLOW_UTIL=1.0.0
EDFI_API_CLIENT=1.0.0
EDU_EDFI_AIRFLOW=1.0.0


### set up airflow environment ###
echo Beginning venv-airflow-init.sh

python3.8 -m pip install --upgrade pip
pip install wheel setuptools  --quiet

# install airflow, modeled on: https://airflow.apache.org/docs/apache-airflow/2.1.2/installation.html#installation-script
# we should probably pass in airflow (and python version) as args to cloudformation template
# debugging; we were asking python for its version number, which is weird because we set it above... might as well just
# use the same param eventually to reference here too. for some reason using python (no version) inside the herdoc block didn't work.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"  # Force dynamic introspection of Python environment.
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[amazon, snowflake, slack, postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"  --quiet
pip install airflow-dbt

### Install EDU packages as executable repos in the `code` directory.
cd ~/code

git clone https://github.com/edanalytics/ea_airflow_util.git
git clone https://github.com/edanalytics/edfi_api_client.git
git clone https://github.com/edanalytics/edu_edfi_airflow.git

pip install -e ea_airflow_util
pip install -e edfi_api_client
pip install -e edu_edfi_airflow

# Checkout the most recent tagged releases.
git -C ea_airflow_util  checkout "tags/v${EA_AIRFLOW_UTIL}"
git -C edfi_api_client  checkout "tags/v${EDFI_API_CLIENT}"
git -C edu_edfi_airflow checkout "tags/v${EDU_EDFI_AIRFLOW}"

# Return to the original path.
cd -
