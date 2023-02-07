import os
import re
from os import path

from easy_sql.logger import logger


def resolve_file(file_path: str, abs_path: bool = False, prefix: str = "", relative_to: str = "") -> str:
    if file_path.lower().startswith("hdfs://") or file_path.lower().startswith("file://"):
        # do not resolve if it is hdfs or absolute file path
        return file_path
    base_path = os.path.abspath(os.curdir)
    if not path.exists(file_path):
        if path.exists(path.join(base_path, file_path)):
            file_path = path.join(base_path, file_path)
        elif path.exists(path.basename(file_path)):
            file_path = path.basename(file_path)
        elif relative_to and path.isfile(relative_to) and path.exists(path.join(path.dirname(relative_to), file_path)):
            file_path = path.join(path.dirname(relative_to), file_path)
        elif relative_to and path.isdir(relative_to) and path.exists(path.join(relative_to, file_path)):
            path.join(relative_to, file_path)
        else:
            raise Exception(f"file not found: {file_path}")
    if abs_path:
        file_path = path.abspath(file_path)
    if " " in file_path:
        parts = file_path.split("/")
        file_path_no_space = "/".join([re.sub(r" .*$", "", part) for part in parts])
        logger.warn(
            "Remove space inside file path, since spark will raise issue with space in path. "
            "We must ensure there is a soft link to the path with space removed to the end. "
            f'Will resolve file path from "{file_path}" to "{file_path}".'
        )
        file_path = file_path_no_space
    return prefix + file_path


def resolve_files(files_path: str, abs_path: bool = False) -> str:
    return ",".join([resolve_file(f.strip(), abs_path) for f in files_path.split(",") if f.strip()])


def read_sql(sql_file: str):
    with open(resolve_file(sql_file)) as f:
        return f.read()
