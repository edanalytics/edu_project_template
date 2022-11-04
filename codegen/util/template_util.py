import textwrap


class TemplateKwargs(dict):
    """
    Extension of dict to add custom logic for missing keys.
    """
    def __missing__(self, key):
        print(f"WARNING: `{key}` is undefined!")
        return None


def clean_string(string, indent=0) -> str:
    """
    Apply dedent and strip logic to a filled template.
    Apply optional indentation.
    """
    # Python's indentation syntax leads to unexpected whitespace to remove.
    cleaned = textwrap.dedent(string).strip()

    # Sometimes, we still want the entire template indented.
    cleaned = textwrap.indent(cleaned, indent * ' ')

    return cleaned


def format_template(template=None, path=None, defaults=locals(), indent=0, **kwargs):
    """
    Dynamically format a template string using objects from the Python namespace.
    Use local variables as defaults, with optional overwrite kwargs as parameters.

    Note: `locals()` must be declared in the function definition.
    Otherwise, the only local variables are `template` and `kwargs`.

    :param template:
    :param path:
    :param defaults:
    :param indent:
    :param kwargs:
    :return:
    """
    if template is None and path is None:
        raise Exception(
            "!!![Internal Error]: No template or path provided to `format_template()`."
        )

    # Read in local template files or manually-provided template strings.
    if path:
        with open(path, 'r') as fp:
            template = fp.read()

    # Fix Python whitespacing and add optional indentation.
    cleaned_template = clean_string(template, indent=indent)

    kwargs = {**defaults, **kwargs}  # If 3.9+, use `kwargs = defaults | kwargs`.
    template_kwargs = TemplateKwargs(kwargs)
    return cleaned_template.format_map(template_kwargs)
