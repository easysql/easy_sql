from typing import Dict


def get_attr(obj: Dict, path: str):
    data_current = obj
    if not path:
        return data_current
    for attr_current in path.split("."):
        assert attr_current != "", f"Neither part of path should be empty: path=`{path}`, current_part=`{attr_current}`"
        if attr_current not in data_current:
            data_current[attr_current] = {}
        data_current = data_current[attr_current]
    return data_current
