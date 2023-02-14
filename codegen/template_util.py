import textwrap

from typing import Union


def load_template(filepath: str, *, indent: int = 0) -> str:
    """
    Load a blank template and apply indentation if specified.

    :param filepath:
    :param indent:
    """
    with open(filepath, 'r') as fp:
        template = fp.read().strip()

    return textwrap.indent(template, indent * ' ')


def write_template(
    filepath: str,
    template: Union[str, list],
    *,
    mode='w',
    sep='\n'
):
    """
    Write-helper for outputting formatted templates to disk.

    :param filepath:
    :param template:
    :param mode:
    :param sep:
    :return:
    """
    # If given a list, join before writing to disk.
    if not isinstance(template, str):
        template = sep.join(template)

    with open(filepath, mode) as fp:
        fp.write(template)

    print(f"Template written to `{filepath}`.")
