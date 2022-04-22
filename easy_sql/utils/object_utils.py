from typing import Dict


def get_attr(obj: Dict, path: str):
    data_current = obj
    for attr_current in path.split('.'):
        if attr_current not in data_current:
            data_current[attr_current] = {}
        data_current = data_current[attr_current]
    return data_current
