import os
import textwrap

from typing import List


class TemplateKwargs(dict):
    """
    Extension of dict to add custom logic for missing keys.
    """
    def __missing__(self, key):
        print(f"WARNING: `{key}` is undefined!")
        return None


class FileTemplate:
    """

    """
    def __init__(self, dir: str, file: str, indent: int = 0):
        """
        Load a blank template and apply indentation if specified.

        """
        self.file = file
        self.indent = indent

        self.template: str = self.load_template(dir)


    def load_template(self, dir: str) -> str:
        """

        :param dir:
        :param file:
        """
        filepath = os.path.join(dir, self.file)
        with open(filepath, 'r') as fp:
            template = fp.read().strip()

        return textwrap.indent(template, self.indent * ' ')


    def write(self, formatted: str, dir: str, file: str = self.file, mode='w'):
        """

        :param formatted:
        :param dir:
        :param file:
        :param mode:
        :return:
        """
        filepath = os.path.join(dir, file)
        with open(filepath, mode) as fp:
            fp.write(formatted)

        print(f"Template written to `{filepath}`.")


    def format(defaults=locals(), **kwargs) -> str:
        """
        Dynamically format a template string using objects from the Python namespace.
        Use local variables as defaults, with optional overwrite kwargs as parameters.

        Note: `locals()` must be declared in the function definition.
        Otherwise, the only local variables are `template` and `kwargs`.

        :param defaults:
        :param kwargs:
        :return:
        """
        template_kwargs = TemplateKwargs({**defaults, **kwargs})  # If 3.9+, use `kwargs = defaults | kwargs`.
        return _template.format_map(template_kwargs)
