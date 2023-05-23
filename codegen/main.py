import os
import pathlib

from edfi_api_client import EdFiClient
from edfi_api_client import camel_to_snake

from template_util import load_template, write_template


def generate_templates(base_url, api_version=3):
    """
    Populate blank templates with information found in the Swagger doc.
        Note: Each template-generation uses local variables as kwargs to minimize formatting-boilerplate.

    :param base_url:
    :param api_version:
    :return:
    """
    ### Build directories to save filled templates
    base_dir = pathlib.Path(__file__).resolve().parents[1]
    TEMPLATES_DIR = os.path.join(base_dir, 'codegen', 'blank')
    GENERATED_DIR = os.path.join(base_dir, 'codegen', 'generated')
    CONFIGS_DIR   = os.path.join(base_dir, 'airflow', 'configs')


    ### Build the resources and descriptors swaggers, and extract the necessary info from each
    api = EdFiClient(base_url, api_version=api_version)
    resources_swagger   = api.get_swagger('resources')
    descriptors_swagger = api.get_swagger('descriptors')

    RESOURCES = [
        (namespace, resource)
        for namespace, resource in resources_swagger.endpoints
        if 'Descriptor' not in resource  # Be extra careful to separate resources from descriptors
    ]
    RESOURCE_DELETES = resources_swagger.deletes
    RESOURCE_DESCRIPTIONS = resources_swagger.descriptions

    DESCRIPTORS = [
        (namespace, descriptor)
        for namespace, descriptor in descriptors_swagger.endpoints
        if 'Descriptor' in descriptor  # Be extra careful to separate resources from descriptors
    ]

    REFERENCE_SKEYS = {  # Make references snake-cased to match their formatting in the warehouse
        camel_to_snake(reference): columns
        for reference, columns in resources_swagger.reference_skeys.items()
    }

    print("Successfully gathered all namespaces and definitions from Swagger.")


    ### Populate each of the templates.
    # EDFI DESCRIPTOR CONFIG BLOCK
    file = 'edfi_descriptors.yml'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(CONFIGS_DIR, file)

    template = load_template(template_path)
    formatted = []
    for namespace, name in DESCRIPTORS:
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)


    # EDFI RESOURCE CONFIG BLOCK
    file = 'edfi_resources.yml'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(CONFIGS_DIR, file)

    template = load_template(template_path)
    formatted = []
    for namespace, name in RESOURCES:
        has_deletes = ((namespace, name) in RESOURCE_DELETES)
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)


    # DBT SOURCE PROPERTY BLOCK
    file = 'src_edfi_3.yml'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(GENERATED_DIR, file)

    template = load_template(template_path, indent=2)
    formatted = []
    for namespace, name in RESOURCES:
        snake = camel_to_snake(name)
        description = RESOURCE_DESCRIPTIONS.get(name, "[NO DESCRIPTION FOUND]")
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)


    # SQL SOURCE CREATE TABLE BLOCK
    file = 'sql_source_create_table.sql'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(GENERATED_DIR, file)

    template = load_template(template_path)
    formatted = []
    for namespace, name in RESOURCES:
        snake = camel_to_snake(name)
        database = 'raw'
        schema = 'edfi3'
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)


    # EDU_EDFI_SOURCE GEN_SKEY REFERENCE COLUMNS
    file = 'gen_skey__reference_columns.yml'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(GENERATED_DIR, file)

    template = load_template(template_path)
    formatted = []
    for reference, columns in REFERENCE_SKEYS.items():
        snake = camel_to_snake(reference)
        formatted.append(template.format(**locals()))

    write_template(output_path, formatted)


    ### All done.
    print("All templates written!")



if __name__ == '__main__':

    import sys
    base_url = sys.argv[1]
    generate_templates(base_url)
