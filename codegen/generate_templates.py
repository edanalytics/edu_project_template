import os
import pathlib

from util.template_util import format_template

from edfi_swagger import EdFiSwagger


def _write_template(file, contents, mode='w'):
    """

    :param output_dir:
    :param file:
    :param contents:
    :return:
    """
    with open(file, mode) as fp:
        fp.write(contents)

    print(f"Template written to `{file}`.")


def generate_templates(base_url, api_version=3):
    """

    :param base_url:
    :param api_version:
    :return:
    """
    BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
    TEMPLATES_DIR = os.path.join(BASE_DIR, 'codegen', 'blank')
    GENERATED_DIR = os.path.join(BASE_DIR, 'codegen', 'generated')
    CONFIGS_DIR   = os.path.join(BASE_DIR, 'airflow', 'configs')

    swagger = EdFiSwagger(base_url, api_version=api_version)

    resources = swagger.resources.values()
    descriptors = swagger.descriptors.values()
    # surrogate_keys = swagger.surrogate_keys
    print("Successfully gathered all namespaces and definitions from Swagger.")


    # EDFI DESCRIPTOR CONFIG BLOCK
    file = 'edfi_descriptors.yml'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(CONFIGS_DIR, file)

    formatted = '\n'.join(
        format_template(path=template_path, **domain_meta)
        for domain_meta in descriptors
    )
    _write_template(output_path, formatted)


    # EDFI RESOURCE CONFIG BLOCK
    file = 'edfi_resources.yml'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(CONFIGS_DIR, file)

    formatted = '\n'.join(
        format_template(path=template_path, **domain_meta)
        for domain_meta in resources
    )
    _write_template(output_path, formatted)


    # # DBT SOURCE CONFIG BLOCK
    # This is currently unused.
    # file = 'dbt_source_config.yml'
    # template_path = os.path.join(TEMPLATES_DIR, file)
    # output_path = os.path.join(CONFIGS_DIR, file)
    #
    # formatted_resources = '\n'.join(
    #     format_template(path=template_path, indent=2, **domain_meta)
    #     for domain_meta in resources
    # )
    # formatted_descriptors = '\n'.join(
    #     format_template(path=template_path, indent=2, **domain_meta)
    #     for domain_meta in descriptors
    # )
    #
    # formatted = f"resources:\n{formatted_resources}\ndescriptors:\n{formatted_descriptors}"
    # _write_template(output_path, formatted)


    # DBT SOURCE PROPERTY BLOCK
    file = 'src_edfi_3.yml'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(GENERATED_DIR, file)

    formatted = '\n'.join(
        format_template(path=template_path, indent=2, **domain_meta)
        for domain_meta in resources
    )
    _write_template(output_path, formatted)


    # SQL SOURCE CREATE TABLE BLOCK
    file = 'sql_source_create_table.sql'
    template_path = os.path.join(TEMPLATES_DIR, file)
    output_path   = os.path.join(GENERATED_DIR, file)

    formatted = '\n'.join(
        format_template(path=template_path, database='dev_raw', schema='edfi3', **domain_meta)
        for domain_meta in resources
    )
    _write_template(output_path, formatted)


    print(
        "All templates written!"
    )



if __name__ == '__main__':

    import sys

    base_url = sys.argv[1]
    generate_templates(base_url)
