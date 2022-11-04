from typing import Callable


def filter_dict(
        key_filter: Callable[[str], bool],
        dict_: dict
) -> dict:
    """

    :param key_filter:
    :param dict_:
    :return:
    """
    if key_filter is None:
        return dict_

    return {
        key: val
        for key, val in dict_.items()
        if key_filter(key)
    }


def merge_dicts(
        dict1: dict,
        dict2: dict,
        boolean_or: bool = False
) -> dict:
    """

    :param dict1:
    :param dict2:
    :param boolean_or:
    :return:
    """
    merged = dict1.copy()

    for key, item in dict2.items():
        if boolean_or:
            merged[key] = merged.get(key) or item
        else:
            merged[key] = item

    return merged
