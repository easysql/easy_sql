import traceback
from datetime import datetime, timedelta
from typing import List, Optional, Union

from . import SqlProcessorException
from .backend import Backend
from ..logger import logger

__all__ = [
    'ColumnFuncs', 'TableFuncs', 'PartitionFuncs', 'AlertFunc'
]


class ColumnFuncs:

    def __init__(self, backend: Backend):
        self.backend = backend

    def all_cols_without_one_expr(self, table_name: str, *cols_to_exclude: List[str]) -> str:
        return self.all_cols_with_exclusion_expr(table_name, *cols_to_exclude)

    def all_cols_with_exclusion_expr(self, table_name: str, *cols_to_exclude: List[str]) -> str:
        pure_table_name = table_name.split(".")[1] if "." in table_name else table_name
        return ', '.join([f'{pure_table_name}.{col}'
                          for col in self.backend.exec_sql(f'select * from {table_name} limit 0').field_names()
                          if col not in cols_to_exclude])


class TableFuncs:

    def __init__(self, backend: Backend):
        self.backend = backend

    def ensure_no_null_data_in_table(self, step, table_name: str, query: str = None) -> bool:
        fields = self.backend.exec_sql(f'select * from {table_name} limit 0').field_names()
        return self._check_not_null_columns_in_table(step, table_name, fields, query, 'ensure_no_null_data_in_table')

    def check_not_null_column_in_table(self, step, table_name: str, not_null_column: str, query: str = None) -> bool:
        return self._check_not_null_columns_in_table(step, table_name, [not_null_column], query, 'check_not_null_column_in_table')

    def _check_not_null_columns_in_table(self, step, table_name: str, not_null_columns: List[str],
                                         query: str = None, context: str = 'check_not_null_column_in_table') -> bool:
        null_counts = {}
        for field in not_null_columns:
            condition_sql = f'{field} is null'
            sql = f'select count(1) from {table_name} where ' + (condition_sql if query is None else f'({condition_sql}) and ({query})')
            null_count = self.backend.exec_sql(sql).collect()[0][0]
            null_counts[field] = null_count
        if sum(null_counts.values()) != 0:
            null_count_msg = '\n'.join([f'{v} null rows({k})' for k, v in null_counts.items() if v != 0])
            msg = f"{context} {table_name} failed, found: \n{null_count_msg}"
            logger.info(msg)
            step.collect_report(message=msg)
            return False
        else:
            return True


class PartitionFuncs:

    def __init__(self, backend: Backend):
        self.backend = backend

    def is_not_first_partition(self, table_name: str, partition_value: str) -> bool:
        return not self.is_first_partition(table_name, partition_value)

    def is_first_partition(self, table_name: str, partition_value: str) -> bool:
        partition_values = self._get_partition_values(table_name)
        return partition_value == str(partition_values[0]) if len(partition_values) > 0 else False

    def _get_partition_values(self, table_name) -> List[Union[str, int]]:
        raise NotImplementedError()

    def _get_partition_values_as_str(self, table_name) -> List[str]:
        return [str(v) for v in self._get_partition_values(table_name)]

    def partition_exists(self, table_name: str, partition_value: str) -> bool:
        return partition_value in self._get_partition_values_as_str(table_name)

    def ensure_partition_exists(self, step, *args) -> bool:
        from easy_sql.sql_processor import Step
        step: Step = step
        if len(args) < 2:
            raise Exception(f'must contains at least one table and exactly one partition_value when calling ensure_partition_exists, got {args}')
        partition_value = args[-1]
        tables = args[:-1]
        partition_not_exists_tables = []
        for table in tables:
            try:
                if not self.partition_exists(table, partition_value):
                    partition_not_exists_tables.append(table)
            except Exception:
                partition_not_exists_tables.append(table)
        if len(partition_not_exists_tables) > 0:
            message = f'partition {partition_value} not exists: {partition_not_exists_tables}'
            logger.info(message)
            step.collect_report(message=message)
            return False
        return True

    def ensure_dwd_partition_exists(self, step, *args) -> bool:
        from easy_sql.sql_processor import Step
        step: Step = step
        if len(args) < 2:
            raise Exception(f'must contains one table and exactly one partition_value and one or more foreign key columns'
                            f' when calling ensure_dwd_partition_exists, got {args}')
        table_name = args[0]
        partition_value = args[1]
        foreign_key_cols = args[2:]
        check_ok = True
        try:
            if not self.partition_exists(table_name, partition_value):
                first_partition = self.get_first_partition_optional(table_name)
                if not first_partition or partition_value > first_partition:
                    message = f'partition {partition_value} not exists: {table_name}'
                    logger.info(message)
                    step.collect_report(message=message)
                    check_ok = False
        except Exception:
            message = f'partition {partition_value} not exists: {table_name}'
            logger.info(message)
            step.collect_report(message=message)
            check_ok = False

        if len(foreign_key_cols) > 0:
            partition_values = self._get_partition_values(table_name)
            if len(partition_values) > 0:  # this indicates first partition exists
                partition_col, first_partition = self.get_partition_col(table_name), str(partition_values[0])
                partition_value_to_use = partition_value if partition_value > first_partition else first_partition
                partition_value_to_use_expr = f"'{partition_value_to_use}'" if isinstance(partition_values[0], str) else partition_value_to_use
                sql = f"select count(1) as total_count from {table_name} where {partition_col}={partition_value_to_use_expr}"
                total_count = self.backend.exec_sql(sql).collect()[0][0]
                if total_count > 0:
                    fk_col_non_null_expr = ' or '.join([f'{fk_col} is not null' for fk_col in foreign_key_cols])
                    sql = f"select 1 as nonnull_count from {table_name} " \
                          f"where ({fk_col_non_null_expr}) and {partition_col}={partition_value_to_use_expr} limit 1"
                    any_fk_col_non_null_rows = self.backend.exec_sql(sql).collect()
                    if len(any_fk_col_non_null_rows) == 0:
                        message = f'all fk cols are null in partition: table_name={table_name}, partition={partition_value_to_use}'
                        logger.info(message)
                        step.collect_report(message=message)
                        check_ok = False

        return check_ok

    def ensure_partition_or_first_partition_exists(self, step, *args) -> bool:
        from easy_sql.sql_processor import Step
        step: Step = step
        if len(args) < 2:
            raise Exception(f'must contains at least one table and exactly one partition_value when calling ensure_partition_exists, got {args}')
        partition_value = args[-1]
        tables = args[:-1]
        partition_not_exists_tables = []
        for table in tables:
            try:
                if not self.partition_exists(table, partition_value):
                    first_partition = self.get_first_partition_optional(table)
                    if not first_partition or partition_value > first_partition:
                        partition_not_exists_tables.append(table)
            except Exception:
                partition_not_exists_tables.append(table)
        if len(partition_not_exists_tables) > 0:
            message = f'partition {partition_value} not exists: {partition_not_exists_tables}'
            logger.info(message)
            step.collect_report(message=message)
            return False
        return True

    def partition_not_exists(self, table_name: str, partition_value: str) -> bool:
        return not self.partition_exists(table_name, partition_value)

    def previous_partition_exists(self, table_name: str, curr_partition_value_as_dt: str) -> bool:
        partition_values = self._get_partition_values_as_str(table_name)
        partition_date_format = '%Y-%m-%d' if '-' in curr_partition_value_as_dt else '%Y%m%d'
        try:
            curr_partition_value_as_dt = datetime.strptime(curr_partition_value_as_dt, partition_date_format)
        except ValueError:
            raise SqlProcessorException(f'partition value must be date of format `%Y-%m-%d` or `%Y%m%d`, found {curr_partition_value_as_dt}')
        previous_partition_value = (curr_partition_value_as_dt - timedelta(days=1)).strftime(partition_date_format)
        return previous_partition_value in partition_values

    def get_partition_or_first_partition(self, table_name: str, partition_value: str):
        partition_values = self._get_partition_values_as_str(table_name)
        if len(partition_values) == 0:
            return partition_value
        if partition_value in partition_values:
            return partition_value
        return partition_values[0] if partition_value < partition_values[0] else partition_value

    def get_first_partition_optional(self, table_name: str) -> Optional[str]:
        partition_values = self._get_partition_values(table_name)
        return str(partition_values[0]) if len(partition_values) > 0 else None

    def get_first_partition(self, table_name: str) -> str:
        first_partition = self.get_first_partition_optional(table_name)
        if not first_partition:
            raise Exception(f'no partition found for table {table_name}')
        return first_partition

    def get_last_partition(self, table_name: str) -> str:
        partition_values = self._get_partition_values(table_name)
        last_partition = partition_values[-1] if len(partition_values) > 0 else None
        if not last_partition:
            raise Exception(f'no partition found for table {table_name}')
        return str(last_partition)

    def get_partition_cols(self, table_name: str) -> List[str]:
        raise NotImplementedError()

    def get_partition_col(self, table_name: str):
        partition_cols = self.get_partition_cols(table_name=table_name)
        if not partition_cols:
            raise Exception(f'no partition columns found for table {table_name}')
        return partition_cols[0]

    def has_partition_col(self, table_name: str):
        return len(self.get_partition_cols(table_name)) == 0


class Alerter:

    def send_alert(self, msg: str, mentioned_users: str = ''):
        raise NotImplementedError()


class AlertFunc:
    def __init__(self, backend: Backend, alerter: Alerter):
        self.backend = backend
        self.alerter = alerter

    def alert(self, step, context, rule_name: str, pass_condition: str, alert_template: str, mentioned_users: str):
        from .step import Step
        from .context import ProcessorContext
        step: Step = step
        context: ProcessorContext = context
        # Fetch 10 rows at most
        alert_data = self.backend.exec_sql(step.select_sql).limit(10).collect()
        # alert_data = self.spark.sql(step.select_sql).limit(10).collect()

        msg_list = []
        for data in alert_data:
            check_result = data.as_dict()
            context.add_vars(check_result)
            if not step.func_runner.run_func(pass_condition.format(**check_result), context.vars_context):
                msg_list.append(alert_template.format(**check_result))
        if msg_list:
            self.alerter.send_alert("\n".join(msg_list), mentioned_users)

    def alert_exception_handler(self, rule_name: str, mentioned_users: str):
        def exception_handler(e):
            logger.error(f"{traceback.format_exc()}")
            self.alerter.send_alert(f"执行{rule_name}失败: {e}", mentioned_users)
        return exception_handler
