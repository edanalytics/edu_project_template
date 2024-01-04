import datetime
import os
import yaml


# Define any custom Python datatype constructors to add to safe_load.
def yaml_to_timedelta(loader, node):
    return datetime.timedelta(seconds=int(node.value))

yaml.SafeLoader.add_constructor('!timedelta', yaml_to_timedelta)


def safe_load_yaml(dir, file, default=None):
    """

    :param dir:
    :param file:
    :param default:
    :return:
    """
    path = os.path.join(dir, file)

    try:
        with open(path) as fp:
            return yaml.safe_load(fp)

    except FileNotFoundError:
        if default is None:
            raise FileNotFoundError(
                f"![ERROR]: YAML file not found at `{path}`."
            )

        print(f"![WARNING]: YAML file not found at `{path}`. Defaulting to {default}.")
        return default
