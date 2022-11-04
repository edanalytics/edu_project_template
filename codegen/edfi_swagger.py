from collections import defaultdict
from typing import Callable, Optional

from edfi_api_client import EdFiClient
from edfi_api_client.util import camel_to_snake
from util.dict_util import filter_dict, merge_dicts


class EdFiSwagger:
    """

    """
    def __init__(self,
        base_url: str,
        api_version: int = 3
    ) -> None:
        self.base_url = base_url
        self.api_version = api_version

        self.resources = self.build_domain_metadata(
            'resources', lambda key: 'Descriptor' not in key
        )
        self.descriptors = self.build_domain_metadata(
            'descriptors', lambda key: 'Descriptor' in key
        )
        self.surrogate_keys = self.build_surrogate_keys(
            'resources', lambda key: key.endswith('Reference')
        )


    def get_swagger_spec(self, component: str) -> dict:
        """
        Pulling Swagger specs does not require API credentials.

        :param component   :
        :return:
        """
        conn = EdFiClient(self.base_url, self.api_version)
        return conn.get_swagger(component)


    @staticmethod
    def build_descriptions(swagger: dict) -> dict:
        """
        Descriptions for all EdFi resources and descriptors are found under `tags` as [name, description] JSON objects.
        Their extraction is optional for YAML templates, but they look nice.

        :param swagger: Swagger JSON object
        :return:
        """
        return {
            tag['name']: tag['description']
            for tag in swagger['tags']
        }


    def build_domain_metadata(self,
        domain_type  : str,
        domain_filter: Optional[Callable[[str], bool]] = None
    ) -> defaultdict:
        """
        Swagger's `paths` is a dictionary of Ed-Fi pathing keys (up-to-three keys per resource/descriptor).
        For example:
            '/ed-fi/studentSchoolAssociations'
            '/ed-fi/studentSchoolAssociations/{id}'
            '/ed-fi/studentSchoolAssociations/deletes'

        Each path can be defined as follows:
            '/ed-fi/studentSchoolAssociations/deletes'
             / NAMESPACE / DOMAIN / (TAG)?

        Extract each domain, its namespace, and its optional deletes/ID tag.
        Pass these to the templates to dynamically populate configs.

        Different filter logic classifies resources and descriptors.
        This should not be defined here.

        :param domain_type:
        :param domain_filter: Optional filter to apply before building domains.
        :return:
        """
        #
        swagger = self.get_swagger_spec(domain_type)

        #
        paths = swagger['paths'].keys()
        descriptions = self.build_descriptions(swagger)

        if domain_filter:
            paths = filter(domain_filter, paths)
            descriptions = filter_dict(domain_filter, descriptions)

        # Build a mapping of domains and dictionaries of metadata.
        all_domain_metadata = defaultdict(dict)

        for path in paths:
            splits = path.split('/', 3)

            namespace = splits[1]
            domain = splits[2]
            tag = splits[3] if len(splits) > 3 else None

            domain_metadata = {
                'name' : domain,
                'snake': camel_to_snake(domain),
                'namespace': namespace,
                'has_deletes': bool(tag == 'deletes'),
                'description': descriptions.get(domain),
            }

            all_domain_metadata[domain] = merge_dicts(
                all_domain_metadata[domain], domain_metadata,
                boolean_or=True
            )

        return all_domain_metadata


    def build_surrogate_keys(self,
        domain_type  : str,
        domain_filter: Optional[Callable[[str], bool]] = None
    ) -> dict:
        """
        EdFi References and their surrogate key definition columns.

        :return:
        """
        swagger = self.get_swagger_spec(domain_type)
        definitions = swagger['definitions']

        if domain_filter:
            definitions = filter_dict(domain_filter, definitions)


        # Build definition mappings for each reference object.
        skey_mapping = {}

        for key, val in definitions.items():
            # The structure of reference definition keys is standardized (e.g.`edFi_staffReference`)
            reference = camel_to_snake(key.split('_')[1])

            # Each reference definition has a list of columns in a `properties` subfield.
            # We want all of these except `link`.
            columns_lambda = lambda col: col not in ('link',)
            columns = list(filter(columns_lambda, val['properties'].keys()))

            skey_mapping[reference] = columns

        return skey_mapping



if __name__ == '__main__':
    from pprint import pprint

    edfi_base_url = 'https://prod2122.nsiedfi.edanalytics.org'
    edfi_swagger = EdFiSwagger(edfi_base_url)

    pprint(edfi_swagger.resources)
    pprint(edfi_swagger.descriptors)
    pprint(edfi_swagger.surrogate_keys)
