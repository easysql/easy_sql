from easy_sql.sql_linter.sql_linter import SqlLinter
import re
import warnings
import click
from easy_sql.sql_linter.sql_linter_reportor import *


def split_rules_to_list(rule_description: str):
    if rule_description != "":
        return rule_description.split(",")
    else:
        return None


def parse_backend(sql: str):
    sql_lines = sql.split('\n')
    parsed_backend = None
    for line in sql_lines:
        if re.match(r'^-- \s*backend:.*$', line):
            parsed_backend = line[line.index('backend:') + len('backend:'):].strip()
            break

    if parsed_backend is None:
        parsed_backend = "spark"
    return parsed_backend


def lint_process(check_sql_file_path: str, exclude: str, include: str, backend: str, easysql: bool):
    if not check_sql_file_path.endswith(".sql"):
        warnings.warn("file name:" + check_sql_file_path + " must end with .sql")

    with open(check_sql_file_path, 'r') as file:
        sql = file.read()
    sql_linter = SqlLinter(sql, exclude_rules=split_rules_to_list(exclude),
                           include_rules=split_rules_to_list(include))
    backend = backend if backend else parse_backend(sql)
    result = sql_linter.lint(backend, easysql=easysql)
    fixed = sql_linter.fix(backend, easysql=easysql)

    return result, fixed


def write_out_fixed(check_sql_file_path: str, fixed: str, inplace: bool):
    if inplace:
        write_out_file_path = check_sql_file_path
    else:
        write_out_file_path = check_sql_file_path.replace(".sql", ".fixed.sql")
    with open(write_out_file_path, 'w') as file:
        file.write(fixed)


@click.group()
def cli():
    """lint only check violations, fix auto fix the violation"""
    pass


def fix_process(path: str, exclude: str, include: str, backend: str, inplace: bool, easysql: bool):
    result, fixed = lint_process(path, exclude, include, backend, easysql)
    write_out_fixed(path, fixed, inplace)


@cli.command(help='''Fix and write out the info''')
@click.option("--path", help="absolute path", required=True, type=str)
@click.option("--exclude", help="comma separated rule to be exclude", default="", required=False, type=str)
@click.option("--include", help="comma separated rule to be exclude", default="", required=False, type=str)
@click.option("--backend", help="running backend for this query, "
                                "if easy sql it will parse from the sql file if not specify, "
                                "if normal sql default is spark", default=None, required=False, type=str)
@click.option("--inplace", help="fix replace checked file", default=False, required=False, type=bool)
@click.option("--easysql", help="easy sql or normal sql", default=True, required=False, type=bool)
def fix(path: str, exclude: str, include: str, backend: str, inplace: bool, easysql: bool):
    fix_process(path, exclude, include, backend, inplace, easysql)


@cli.command(help='''Check sql quality''')
@click.option("--path", help="absolute path", required=True, type=str)
@click.option("--exclude", help="comma separated rule to be exclude", default="", required=False, type=str)
@click.option("--include", help="comma separated rule to be exclude", default="", required=False, type=str)
@click.option("--backend", help="running backend for this query, "
                                "if easy sql it will parse from the sql file if not specify, "
                                "if normal sql default is spark", default=None, required=False, type=str)
@click.option("--easysql", help="easy sql or normal sql", default=True, required=False, type=bool)
def lint(path: str, exclude: str, include: str, backend:str, easysql: bool):
    lint_process(path, exclude, include, backend, easysql)


#  python easy_sql/sql_linter/sql_linter_cli.py fix --path
#

if __name__ == "__main__":
    cli.main(sys.argv[1:])
