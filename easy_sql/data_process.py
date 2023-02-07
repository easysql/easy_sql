from __future__ import annotations

import os
import sys
from typing import List, Optional

import click


@click.command(name="data_process")
@click.option("--sql-file", "-f", type=str)
@click.option("--vars", "-v", type=str, required=False)
@click.option("--dry-run", type=str, required=False, help="if dry run, one of [true, 1, false, 0]")
@click.option("--python-path", type=str, required=False)
@click.option("--print-command", "-p", is_flag=True)
def data_process(sql_file: str, vars: str, dry_run: str, python_path: str, print_command: bool):
    EasySqlProcessor(sql_file, vars, dry_run, print_command, python_path=python_path).process()


class EasySqlProcessor:
    def __init__(
        self,
        sql_file: str,
        vars: Optional[str],
        dry_run: Optional[str],
        print_command: bool,
        python_path: Optional[str] = None,
    ) -> None:
        if not sql_file.endswith(".sql"):
            raise Exception(f"sql_file must ends with .sql, found `{sql_file}`")

        try:
            from easy_sql.config.sql_config import EasySqlConfig
        except ModuleNotFoundError:
            assert python_path is not None
            sys.path.insert(0, python_path)
            from easy_sql.config.sql_config import EasySqlConfig

        self.sql_file = sql_file
        self.vars_arg = vars
        self.dry_run_arg = dry_run if dry_run is not None else "0"
        self.dry_run = dry_run in ["true", "1"]
        self.config = EasySqlConfig.from_sql(sql_file)
        self.print_command = print_command

    def process(self, backend_config: Optional[List[str]] = None) -> Optional[str]:
        from easy_sql.cli.backend_processor import BackendProcessor

        backend_processor = BackendProcessor.create_backend_processor(self.config)

        if self.print_command:
            command = backend_processor.shell_command(
                self.vars_arg, self.dry_run_arg, os.path.abspath(__file__), backend_config
            )
            print(command)
            return command
        else:
            backend_processor.run(self.vars_arg, self.dry_run, backend_config)


if __name__ == "__main__":
    data_process()
