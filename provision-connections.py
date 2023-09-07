import sys
import yaml
import requests
import subprocess

# call this Python script as
# > python make-connections.py [school_year] [stadium_env] [sb_env] [claimset_name] [vendor_id]
# for example
# > python make-connections.py 2023 prod dev-2023 stadium_read 17
# this will
# (1) parse the airflow yaml for a list like
#       env_label,tenant_code,lea_id,year
# (2) prompt for Ed-Fi Admin API credentials and auth with Admin API
# (3) for each row of (1), create an "application" called [env_label]_[tenant_code]_[year]
#       under [vendor_id] with claimset [claimset_name] and educationOrganizationIds [lea_id]
# (4) create airflow connection (called edfi_[tenant_code]_[year]) for each set of creds
#       that were created


# we must register a `!timedelta` handler so the YAML loader doesn't scream:
def timedelta(loader, node):
    value = loader.construct_scalar(node)
    return value
yaml.SafeLoader.add_constructor(u'!timedelta', timedelta)

# parse through the yaml file and extract the tenants and years from Airflow config
def extract_airflow_dags(yaml_file, specified_key):
    try:
        with open(yaml_file, 'r') as file:
            data = yaml.safe_load(file)
            if specified_key in data and isinstance(data[specified_key], dict):
                results = []
                for key, value in data[specified_key].items():
                    if isinstance(value, dict):
                        result = []
                        result.append(key)
                        result.append(list(value.keys())[0])
                        result.append(value[list(value.keys())[0]].get('lea_id',''))
                        results.append(result)
                return results
            else:
                return None
    except FileNotFoundError:
        print(f"File '{yaml_file}' not found.")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
    return None

school_year = sys.argv[1]
stadium_env = sys.argv[2]
sb_env = sys.argv[3]
claimset_name = sys.argv[4]
vendor_id = sys.argv[5]
yaml_file = f"airflow/config/airflow_config_{stadium_env}.yml"
yaml_key = 'edfi_resource_dags'
admin_api_url = f"https://admin-api.{sb_env}.txexchange.startingblocks.org"

print("This script connects to an Ed-Fi Admin API, creates applications for each tenant and year,")
print("and loads the credentials as connections in Airflow.\n")

# (1) parse the airflow yaml
results = extract_airflow_dags(yaml_file, yaml_key)
if results:
    # (2) prompt for Ed-Fi Admin API credentials and auth with Admin API
    print(f"To connect to {admin_api_url} please")
    admin_api_key = input('enter the Admin API key:')
    admin_api_secret = input('enter the Admin API secret:')
    print("")

    # connect to Admin API and obtain auth token:
    auth_payload = {
        "client_id": admin_api_key,
        "client_secret": admin_api_secret,
        "grant_type": "client_credntials",
        "scope": "edfi_admin_api/full_access"
    }
    r = requests.post(admin_api_url + "/connect/token", json=auth_payload)
    if r.status_code!=200:
        raise Exception("Admin API authentication failed... check your credentials?")
    auth_response = r.json()
    access_token = auth_response.get("access_token", "")
    headers = {'Authorization': 'Bearer ' + access_token, 'Accept': 'application/json'}

    for result in results:
        tenant_code = result[0]
        lea_id = result[2]
        year = result[1]

        # skip Stadium years that aren't in this SB environment
        if year!=school_year: continue

        # construct 
        startingblocks_host = f"https://stadium-{lea_id}.{sb_env}.edfi.txedexchange.net"

        # see Ed-Fi Admin API docs at https://techdocs.ed-fi.org/display/ADMINAPI/Endpoints+-+Admin+API
        payload = {
            "applicationName": f"{stadium_env}_{tenant_code}_{year}",
            "vendorId": vendor_id,
            "claimSetName": claimset_name,
            # "profileId": 0,
            "educationOrganizationIds": [
                lea_id
            ]
        }
        r = requests.post(admin_api_url + "/v1/applications/", json=payload)
        if r.status_code!=200:
            raise Exception("Admin API authentication failed... check your credentials?")
        credentials = r.json()
        edfi_api_key = credentials["key"]
        edfi_api_secret = credentials["secret"]

        # create an Airflow connection
        subprocess.run(["airflow", "connections", "add", f"'edfi_{tenant_code}_{year}'",
                                "--conn-type", "'http'",
                                "--conn-host", f"'{startingblocks_host}'",
                                "--conn-login", f"'{edfi_api_key}'",
                                "--conn-password", f"'{edfi_api_secret}'",
                                ], shell=False)
        print(f"Airflow conection `edfi_{tenant_code}_{year}` created")
else:
    print(f"`{yaml_key}` not found or is not a dictionary in {yaml_file}.")

print("All done: have a nice day!")