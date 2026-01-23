# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "edfi-api-client",
#     "pyyaml",
# ]
# ///

import json
import os
import pathlib

import yaml
from edfi_api_client import EdFiClient
from edfi_api_client import camel_to_snake
from edfi_api_client.edfi_swagger import EdFiSwagger

from .template_util import load_template
from .template_util import write_template


def get_swagger_from_path(swagger_path: str) -> dict[str, EdFiSwagger]:
    with open(swagger_path, "r") as fp:
        swagger = json.load(fp)

    return {
        "resources": EdFiSwagger("resources", swagger),
        "descriptors": EdFiSwagger("descriptors", swagger),
    }


def generate_templates_from_swagger_path(swagger_path: str, database=None, schema=None):
    generate_templates_from_swagger_data(
        get_swagger_from_path(swagger_path), database=database, schema=schema
    )


def generate_templates_from_swagger_data(
    swagger_data: dict[str, EdFiSwagger], database=None, schema=None
):
    """
    Populate blank templates with information found in the Swagger doc.
        Note: Each template-generation uses local variables as kwargs to minimize formatting-boilerplate.

    :param base_url:
    :param api_version:
    :return:
    """
    ### Build directories to save filled templates
    base_dir = pathlib.Path(__file__).resolve().parents[1]
    TEMPLATES_DIR = os.path.join(base_dir, "codegen", "blank")
    GENERATED_DIR = os.path.join(base_dir, "codegen", "generated")
    CONFIGS_DIR = os.path.join(base_dir, "airflow", "configs")

    resources_swagger = swagger_data["resources"]
    descriptors_swagger = swagger_data["descriptors"]

    RESOURCES = [
        (namespace, resource)
        for namespace, resource in resources_swagger.endpoints
        if "Descriptor"
        not in resource  # Be extra careful to separate resources from descriptors
    ]
    RESOURCE_DELETES = resources_swagger.deletes
    RESOURCE_DESCRIPTIONS = resources_swagger.descriptions

    DESCRIPTORS = [
        (namespace, descriptor)
        for namespace, descriptor in descriptors_swagger.endpoints
        if "Descriptor"
        in descriptor  # Be extra careful to separate resources from descriptors
    ]

    REFERENCE_SKEYS = {  # Make references snake-cased to match their formatting in the warehouse
        camel_to_snake(reference): columns
        for reference, columns in resources_swagger.reference_skeys.items()
    }

    print("Successfully gathered all namespaces and definitions from Swagger.")

    ### Populate each of the templates.
    # EDFI DESCRIPTOR CONFIG BLOCK
    file = "edfi_descriptors.yml"
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path = os.path.join(CONFIGS_DIR, file)

    template = load_template(template_path)
    formatted = []
    for namespace, name in DESCRIPTORS:
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)

    # EDFI RESOURCE CONFIG BLOCK
    file = "edfi_resources.yml"
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path = os.path.join(CONFIGS_DIR, file)

    template = load_template(template_path)
    formatted = []
    for namespace, name in RESOURCES:
        has_deletes = (namespace, name) in RESOURCE_DELETES
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)

    ### Ensure resources that are required by edu_edfi_source are added to 'src_edfi_3.yml' and 'sql_source_create_table.sql'
    required_resources_path = os.path.join(
        base_dir, "codegen", "required_resources.yml"
    )
    with open(required_resources_path) as fp:
        required_resources = yaml.safe_load(fp)

    for name in required_resources:
        resource = ("ed-fi", name)
        if resource not in RESOURCES:
            RESOURCES.append(resource)
            RESOURCE_DESCRIPTIONS[name] = required_resources[name]

    # DBT SOURCE PROPERTY BLOCK
    file = "src_edfi_3.yml"
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path = os.path.join(GENERATED_DIR, file)

    template = load_template(template_path, indent=2)
    formatted = []
    for namespace, name in RESOURCES:
        snake = camel_to_snake(name)
        description = RESOURCE_DESCRIPTIONS.get(name, "[NO DESCRIPTION FOUND]")
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)

    # SQL SOURCE CREATE TABLE BLOCK
    file = "sql_source_create_table.sql"
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path = os.path.join(GENERATED_DIR, file)

    template = load_template(template_path)
    formatted = []
    for namespace, name in RESOURCES:
        snake = camel_to_snake(name)
        database = database or "raw"
        schema = schema or "edfi3"
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)

    # EDU_EDFI_SOURCE GEN_SKEY REFERENCE COLUMNS
    file = "gen_skey__reference_columns.yml"
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path = os.path.join(GENERATED_DIR, file)

    template = load_template(template_path)
    formatted = []
    for reference, columns in REFERENCE_SKEYS.items():
        snake = camel_to_snake(reference)
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)

    ### All done.
    print("All templates written!")


def generate_templates(base_url, api_version=3, database=None, schema=None):
    """
    Populate blank templates with information found in the Swagger doc.
        Note: Each template-generation uses local variables as kwargs to minimize formatting-boilerplate.

    :param base_url:
    :param api_version:
    :param database:
    :param schema:
    :return:
    """
    ### Build the resources and descriptors swaggers, and extract the necessary info from each
    api = EdFiClient(base_url, api_version=api_version)
    resources_swagger = api.get_swagger("resources")
    descriptors_swagger = api.get_swagger("descriptors")

    swagger_data = {
        "resources": resources_swagger,
        "descriptors": descriptors_swagger,
    }

    generate_templates_from_swagger_data(swagger_data, database=database, schema=schema)


if __name__ == "__main__":
    import sys
    base_url = sys.argv[1]
    database = sys.argv[2]
    schema = sys.argv[3]
    generate_templates(base_url=base_url, database=database, schema=schema)
