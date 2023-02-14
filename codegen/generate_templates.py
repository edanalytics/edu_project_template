import os
import pathlib

from edfi_api_client import EdFiClient
from edfi_api_client import camel_to_snake

from .util import FileTemplate


def generate_templates(base_url, api_version=3):
    """

    :param base_url:
    :param api_version:
    :return:
    """
    # Build directories to save filled templates
    BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
    TEMPLATES_DIR = os.path.join(BASE_DIR, 'codegen', 'blank')
    GENERATED_DIR = os.path.join(BASE_DIR, 'codegen', 'generated')
    CONFIGS_DIR   = os.path.join(BASE_DIR, 'airflow', 'configs')

    # Build the resources and descriptors swaggers
    api = EdFiClient(base_url, api_version=api_version)
    resources_swagger   = api.get_swagger('resources')
    descriptors_swagger = api.get_swagger('descriptors')

    # Extract the necessary information from each, being extra careful to separate resources from descriptors
    RESOURCES = [
        (namespace, resource)
        for namespace, resource in resources_swagger.resources
        if 'Descriptor' not in resource
    ]
    RESOURCE_DELETES = resources_swagger.deletes
    RESOURCE_DESCRIPTIONS = resources_swagger.descriptions

    DESCRIPTORS = [
        (namespace, descriptor)
        for namespace, descriptor in descriptors_swagger.resources
        if 'Descriptor' in descriptor
    ]
    DESCRIPTOR_DELETES = descriptors_swagger.deletes
    DESCRIPTOR_DESCRIPTIONS = descriptors_swagger.descriptions

    REFERENCE_SKEYS = {  # Make references snake-cased to match their formatting in the warehouse
        camel_to_snake(reference): columns
        for reference, columns in resources_swagger.surrogate_keys.items()
    }

    print("Successfully gathered all namespaces and definitions from Swagger.")


    # EDFI DESCRIPTOR CONFIG BLOCK
    template = FileTemplate(TEMPLATES_DIR, 'edfi_descriptors.yml')

    formatted = []
    for namespace, name in DESCRIPTORS:
        formatted.append(template.format())  # Use locals() by default.

    template.write('\n'.join(formatted), CONFIGS_DIR)


    # EDFI RESOURCE CONFIG BLOCK
    template = FileTemplate(TEMPLATES_DIR, 'edfi_resources.yml')

    formatted = []
    for namespace, name in RESOURCES:
        has_deletes = ((namespace, name) in RESOURCE_DELETES)
        formatted.append(template.format())

    template.write('\n'.join(formatted), CONFIGS_DIR)


    # DBT SOURCE PROPERTY BLOCK
    template = FileTemplate(TEMPLATES_DIR, 'src_edfi_3.yml', indent=2)

    formatted = []
    for namespace, name in RESOURCES:
        description = RESOURCE_DESCRIPTIONS.get(name, "[NO DESCRIPTION FOUND]")
        formatted.append(template.format())

    template.write('\n'.join(formatted), GENERATED_DIR)


    # SQL SOURCE CREATE TABLE BLOCK
    template = FileTemplate(TEMPLATES_DIR, 'sql_source_create_table.sql')

    formatted = []
    for namespace, name in RESOURCES:
        snake = camel_to_snake(name)
        database = 'dev_raw'
        schema = 'edfi3'
        formatted.append(template.format())

    template.write(formatted, GENERATED_DIR)


    print("All templates written!")



if __name__ == '__main__':

    import sys

    base_url = sys.argv[1]
    generate_templates(base_url)
