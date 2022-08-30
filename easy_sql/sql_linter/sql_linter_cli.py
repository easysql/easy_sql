import re
import sys
import warnings
from typing import List, Optional

import click

from easy_sql.sql_linter.sql_linter import SqlLinter


def split_rules_to_list(rule_description: str) -> Optional[List[str]]:
    if rule_description != "":
        return rule_description.split(",")
    else:
        return None


def parse_backend(sql: str):
    sql_lines = sql.split("\n")
    parsed_backend = None
    for line in sql_lines:
        if re.match(r"^-- \s*backend:.*$", line):
            parsed_backend = line[line.index("backend:") + len("backend:") :].strip()
            break

    if parsed_backend is None:
        parsed_backend = "spark"
    return parsed_backend


def lint_process(
    check_sql_file_path: str,
    exclude: str,
    include: str,
    backend: str,
    easy_sql: bool,
    config_path: Optional[str] = None,
):
    if not check_sql_file_path.endswith(".sql"):
        warnings.warn("file name:" + check_sql_file_path + " must end with .sql")

    with open(check_sql_file_path, "r") as file:
        sql = file.read()
    sql_linter = SqlLinter(sql, exclude_rules=split_rules_to_list(exclude), include_rules=split_rules_to_list(include))
    backend = backend if backend else parse_backend(sql)
    print("using backend:", backend)
    result = sql_linter.lint(backend, easysql=easy_sql, config_path=config_path)
    fixed = sql_linter.fix(backend, easy_sql=easy_sql, config_path=config_path)

    return result, fixed


def write_out_fixed(check_sql_file_path: str, fixed: str, inplace: bool):
    if inplace:
        write_out_file_path = check_sql_file_path
    else:
        write_out_file_path = check_sql_file_path.replace(".sql", ".fixed.sql")
    with open(write_out_file_path, "w") as file:
        file.write(fixed)


@click.group()
def cli():
    """Check or fix violations in SQL."""
    pass


def fix_process(
    path: str,
    exclude: str,
    include: str,
    backend: str,
    inplace: bool,
    easy_sql: bool,
    config_path: Optional[str] = None,
):
    result, fixed = lint_process(path, exclude, include, backend, easy_sql, config_path=config_path)
    write_out_fixed(path, fixed, inplace)


@cli.command(help="""Fix rule violations in sql""")
@click.option("--path", help="sql file path", required=True, type=str)
@click.option("--config-path", help="sql fluff config file path, must be named .sqlfluff", required=False, type=str)
@click.option("--exclude", help="comma separated rule to be excluded", default="", required=False, type=str)
@click.option("--include", help="comma separated rule to be included", default="", required=False, type=str)
@click.option(
    "--backend",
    help=(
        "backend for this file, "
        "if easy sql it will parse from the sql file if not specify, "
        "if normal sql it will default to spark"
    ),
    default=None,
    required=False,
    type=str,
)
@click.option("--inplace", help="fix file inplace", default=False, required=False, type=bool)
@click.option("--easy_sql", help="easy sql or normal sql", default=True, required=False, type=bool)
def fix(path: str, config_path: str, exclude: str, include: str, backend: str, inplace: bool, easy_sql: bool):
    fix_process(path, exclude, include, backend, inplace, easy_sql, config_path=config_path)


@cli.command(help="""Check rule violations in sql""")
@click.option("--path", help="sql file path", required=True, type=str)
@click.option("--config-path", help="sql fluff config file path, must be named .sqlfluff", required=False, type=str)
@click.option("--exclude", help="comma separated rule to be excluded", default="", required=False, type=str)
@click.option("--include", help="comma separated rule to be included", default="", required=False, type=str)
@click.option(
    "--backend",
    help=(
        "backend for this file, "
        "if easy sql it will parse from the sql file if not specify, "
        "if normal sql it will default to spark"
    ),
    default=None,
    required=False,
    type=str,
)
@click.option("--easy_sql", help="easy sql or normal sql", default=True, required=False, type=bool)
def lint(path: str, config_path: str, exclude: str, include: str, backend: str, easy_sql: bool):
    lint_process(path, exclude, include, backend, easy_sql, config_path=config_path)


if __name__ == "__main__":
    cli.main(sys.argv[1:])
