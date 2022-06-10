from easy_sql.sql_linter.sql_linter import SqlLinter
import re
import warnings
from sqlfluff.core import Lexer, Parser
import click
from easy_sql.sql_linter.sql_linter_reportor import *

default_dialect = "sparksql"


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


# TODO: move to easy_sql linter
def lint_for_normal_sql(easy_sql_linter: SqlLinter, sql: str):
    lexer = Lexer(dialect=default_dialect)
    parser = Parser(dialect=default_dialect)
    linter = easy_sql_linter._prepare_linter(default_dialect)
    tokens, _ = lexer.lex(sql)
    parsed = parser.parse(tokens)
    lint_result = linter.lint(parsed)
    fixed_tree, violation = linter.fix(parsed)
    log_list_of_violations(lint_result)
    return lint_result, fixed_tree.raw


# TODO :type of args
def lint_process(check_sql_file_path, include, exclude):
    if not check_sql_file_path.endswith(".sql"):
        warnings.warn("file name:" + check_sql_file_path + " must end with .sql")

    with open(check_sql_file_path, 'r') as file:
        sql = file.read()
    sql_linter = SqlLinter(sql, exclude_rules=split_rules_to_list(exclude),
                           include_rules=split_rules_to_list(include))
    # TODO: do not have this limitation any  >> args
    # TODO: check logic shou;ld include .table.sql/ _table.sql

    if not check_sql_file_path.endswith("table.sql"):
        backend = parse_backend(sql)
        result = sql_linter.lint(backend)
        fixed = sql_linter.fix(backend)
    else:
        print("run for normal sql")
        result, fixed = lint_for_normal_sql(sql_linter, sql)

    return result, fixed


def write_out_fixed(check_sql_file_path: str, fixed: str):
    # TODO: remove origin
    # TODO: --inplace args directly overwrite previous
    origin_with_fixed = fixed

    write_out_file_path = check_sql_file_path.replace(".sql", ".fixed.sql")
    with open(write_out_file_path, 'w') as file:
        file.write(origin_with_fixed)


@click.group()
def cli():
    """lint only check violations, fix auto fix the violation"""  # noqa D403
    pass


# TODO:help

@cli.command(help='''fix and write out the info''')
@click.option("--path", help="", required=True, type=str)
@click.option("--exclude", default="", required=False, type=str)
@click.option("--include", default="", required=False, type=str)
def fix(path: str, exclude: str, include: str):
    result, fixed = lint_process(path, include, exclude)
    write_out_fixed(path, fixed)


@cli.command()
@click.option("--path", required=True, type=str)
@click.option("--exclude", default="", required=False, type=str)
@click.option("--include", default="", required=False, type=str)
def lint(path: str, exclude: str, include: str):
    lint_process(path, include, exclude)


#  python easy_sql/sql_linter/sql_linter_cli.py fix --path
#

if __name__ == "__main__":
    cli.main(sys.argv[1:])
