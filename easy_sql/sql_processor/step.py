import re
import uuid
from os import path
from typing import List, Dict, Optional

from ..logger import logger
from .backend import Backend, Table as BackendTable, TableMeta as Table, Partition, SaveMode
from .common import SqlProcessorException
from .context import ProcessorContext
from .funcs import FuncRunner

__all__ = [
    'StepConfig', 'Step', 'StepType', 'ReportCollector', 'StepFactory'
]


class ReportCollector:

    def collect_report(self, step: 'Step', status: str = None, message: str = None):
        raise NotImplemented()


class StepConfig:
    STEP_CONFIG_PATTERN = r'^-- target\s*=\s*(\w+)(.*)$'

    def __init__(self, step_type: str, step_name: str, condition: str, line_no: int):
        self.step_type = step_type
        self.name = step_name
        self.condition = condition
        self.line_no = line_no

    def __str__(self):
        return f'StepConfig(target={self.step_type}.{self.name}, condition={self.condition}, line_no={self.line_no})'

    def __repr__(self):
        return str(self)

    def __eq__(self, other: 'StepConfig'):
        return isinstance(other, StepConfig) and self.step_type == other.step_type \
               and self.name == other.name and self.condition == other.condition and self.line_no == other.line_no

    @staticmethod
    def from_config_line(config_line: str, line_no: int) -> 'StepConfig':
        configs = re.compile(r'^\s*-- ').sub('', config_line).strip()
        configs = configs[configs.index('=') + 1:]
        target_type = configs[:configs.index('.')] if configs.find('.') != -1 else configs
        target_type = target_type[:target_type.index(',')] if target_type.find(',') != -1 else target_type
        if target_type not in StepType.all():
            raise SqlProcessorException(f'unknown step type: type={target_type}, supported_types={StepType.all()}')

        target_name = configs[configs.index('.') + 1:] if configs.find('.') != -1 else None
        target_name = target_name.strip() if target_name is not None and target_name.strip() != '' else None
        target_condition = None

        if target_name is not None:
            condition_match = re.compile(r'^\s*(.*)\s*,\s*if\s*=(.*$)').match(target_name)
        else:
            condition_match = re.compile(r'^\s*(,)\s*if\s*=(.*$)').match(configs[len(target_type):])
        if condition_match:
            target_name = condition_match.group(1) if target_name is not None else None
            target_condition = condition_match.group(2).strip()
            if not re.compile(r'[a-zA-Z0-9_]*\([^()]*\)').match(target_condition):
                raise SqlProcessorException(f'parse step config failed. condition must be like [a-zA-Z0-9_]*\([^()]*\), '
                                            f'but got {target_condition}. config_line={config_line}')

        return StepConfig(step_name=target_name, step_type=target_type, condition=target_condition, line_no=line_no)

    def is_target_name_a_func(self):
        return '(' in self.name if self.name is not None else False

    def has_condition(self):
        return self.condition is not None


class StepType:
    TEMP = 'temp'
    CACHE = 'cache'
    BROADCAST = 'broadcast'
    TEMPLATE = 'template'
    FUNC = 'func'
    LOG = 'log'
    CHECK = 'check'
    VARIABLES = 'variables'
    LIST_VARIABLES = 'list_variables'
    HIVE = 'hive'
    OUTPUT = 'output'
    ACTION = 'action'

    @staticmethod
    def all() -> List[str]:
        return [
            StepType.TEMP, StepType.CACHE, StepType.TEMPLATE, StepType.LOG,
            StepType.CHECK, StepType.VARIABLES, StepType.HIVE, StepType.FUNC,
            StepType.BROADCAST, StepType.OUTPUT, StepType.LIST_VARIABLES, StepType.ACTION
        ]


class Step:
    def __init__(self, id: str, reporter_collector: ReportCollector, func_runner: FuncRunner,
                 target_config: StepConfig = None, select_sql: str = None, debug_var_tmpl_replace: bool = False):
        self.id = id
        self.target_config = target_config
        self.select_sql = select_sql
        self.debug_var_tmpl_replace = debug_var_tmpl_replace
        self.reporter_collector = reporter_collector
        self.func_runner = func_runner

    def __str__(self):
        return str(self.target_config).replace('StepConfig(', 'Step(', 1)

    def __repr__(self):
        return f'[\n    config: {self.target_config},\n    sql: {self.select_sql}\n]'

    def should_run(self, context: ProcessorContext):
        variables: dict = context.vars_context.vars
        if '__skip_all__' in variables and variables['__skip_all__'] in ['TRUE', True, 1, 'True', 'true', '1']:
            return False
        if not self.target_config.has_condition():
            return True
        return self.func_runner.run_func(self.target_config.condition, context.vars_context)

    def read(self, backend: Backend, context: ProcessorContext) -> Optional[BackendTable]:
        if self.target_config.step_type in [StepType.TEMPLATE] or (self.target_config.step_type == StepType.CHECK and self._should_skip_check(context.vars_context.vars)):
            return backend.create_empty_table()
        if self.target_config.is_target_name_a_func():
            if self.select_sql:
                self.preprocess_select_sql(context)
            return backend.create_empty_table()
        self.preprocess_select_sql(context)
        if self.target_config.step_type == StepType.ACTION:
            backend.exec_native_sql(self.select_sql)
            return None
        else:
            return backend.exec_sql(self.select_sql)

    def preprocess_select_sql(self, context):
        self.select_sql = context.replace_templates(self.select_sql)
        self.select_sql = context.replace_variables(self.select_sql)

    def write(self, backend: Backend, df: Optional[BackendTable], context: ProcessorContext, dry_run: bool = False):
        variables: dict = context.vars_context.vars

        if not df:
            return

        if StepType.VARIABLES == self.target_config.step_type:
            if not df.is_empty():
                field_names = df.field_names()
                row = df.first()
                for field_name in field_names:
                    index = field_names.index(field_name)
                    field_value = "null"
                    if row[index] is not None:
                        field_value = str(row[index])
                    context.add_vars({field_name: field_value})

        if StepType.LIST_VARIABLES == self.target_config.step_type:
            field_names = df.field_names()
            rows = df.collect()
            list_vars = {}
            for field_name in field_names:
                index = field_names.index(field_name)
                list_vars[field_name] = [row[index] for row in rows]
            context.add_list_vars(list_vars)

        if StepType.TEMPLATE == self.target_config.step_type:
            context.add_templates({self.target_config.name: self.select_sql})

        elif StepType.TEMP == self.target_config.step_type:
            backend.create_temp_table(df, self.target_config.name)

        elif StepType.CACHE == self.target_config.step_type:
            if '__no_cache__' in variables and variables['__no_cache__'] in ['TRUE', True, 1, 'True', 'true']:
                backend.create_temp_table(df, self.target_config.name)
            else:
                backend.create_cache_table(df, self.target_config.name)

        elif StepType.BROADCAST == self.target_config.step_type:
            backend.broadcast_table(df, self.target_config.name)

        elif StepType.LOG == self.target_config.step_type:
            if '__no_log__' in variables and variables['__no_log__'] in ['TRUE', True, 1, 'True', 'true']:
                return
            self._write_for_log_step(df)

        elif StepType.FUNC == self.target_config.step_type:
            self.func_runner.run_func(self.target_config.name, context.vars_context)

        elif StepType.CHECK == self.target_config.step_type:
            if self._should_skip_check(variables):
                return
            self._write_for_check_step(df, context)

        elif self.target_config.step_type in [StepType.HIVE, StepType.OUTPUT]:
            self._write_for_output_step(backend, df, context, dry_run)

    def _should_skip_check(self, variables):
        return '__no_check__' in variables and variables['__no_check__'] in ['TRUE', True, 1, 'True', 'true']

    def _write_for_output_step(self, backend: Backend, df: BackendTable, context: ProcessorContext, dry_run: bool):
        extra_cols, variables = context.extra_cols, context.vars
        if '.' not in self.target_config.name:
            message = f'table name for hive or output must be a full name, it should be of format DB.TABLE_NAME, got `{self.target_config.name}`'
            self.collect_report(message=message)
            raise SqlProcessorException(message)

        temp_table_name = f'{self.target_config.name.split(".")[1]}_{uuid.uuid4().hex}'
        for col in extra_cols:
            df = df.with_column(col.name, col.value)
        backend.create_temp_table(df, temp_table_name)

        source_table = Table(temp_table_name)
        target_table_name = f'{self.target_config.name}'

        static_partition_name, static_partition_value, create_output_table, save_mode = None, None, False, SaveMode.overwrite
        for name, value in variables.items():
            if '__partition__' in name:
                static_partition_name = name[len('__partition__'):]
                if backend.is_spark_backend:
                    static_partition_value = value
                else:
                    static_partition_value = backend.sql_expr.convert_partition_value(static_partition_name, value)
            if 'save_mode' == name.lower() or '__save_mode__' == name.lower():
                save_mode = SaveMode[value.lower()]
            if name.lower() in ['__create_hive_table__', '__create_output_table__', ]:
                create_output_table = value in [True, 'true', 'TRUE', 'True', 1, '1']

        if static_partition_name is not None:
            if static_partition_value is None or str(static_partition_value).strip() == '':
                raise SqlProcessorException(f'partition value should exist and should not be empty, found {static_partition_value}')
            target_table = Table(target_table_name, partitions=[Partition(field=static_partition_name, value=static_partition_value)])
            self.collect_report(message=f'save with static partition: {static_partition_name}={static_partition_value}')
        else:
            target_table = Table(target_table_name)
            if not dry_run and backend.table_exists(target_table):
                backend.refresh_table_partitions(target_table)
            self.collect_report(message=f'save with dynamic partitions')

        if dry_run:
            if static_partition_name:
                if backend.is_spark_backend:
                    from pyspark.sql.functions import lit
                    column = df.with_column(static_partition_name, lit(static_partition_value))
                else:
                    column = df.with_column(static_partition_name, backend.sql_expr.for_value(static_partition_value))
                backend.create_temp_table(column, temp_table_name + "_output")
            self.collect_report(message=f'will not save data to hive, since we are in dry run mode')
            return

        target_table_exists = backend.table_exists(target_table)
        if not target_table_exists and not create_output_table:
            message = f'target table {target_table.table_name} not exists'
            self.collect_report(message=message)
            raise Exception(message)

        backend.save_table(source_table, target_table, save_mode, create_target_table=create_output_table)

    def _write_for_log_step(self, df: BackendTable):
        log_data = df.limit(20).collect()
        if len(log_data) == 0:
            logger.info(f'log for [{self.target_config.name}]: no data to show')
            self.collect_report(message=f'no data to show')
        elif len(log_data) == 1:
            logger.info(f'log for [{self.target_config.name}]: {str(log_data[0])}')
            self.collect_report(message=f'{str(log_data[0])}')
        else:
            logger.info(f'log for [{self.target_config.name}]: ')
            df.show(20)
            self.collect_report(message=f'{str(log_data[0])}')

    def _write_for_check_step(self, df: BackendTable, context: ProcessorContext):
        if self.target_config.is_target_name_a_func():
            if not self.func_runner.run_func(self.target_config.name, context.vars_context):
                message = f'check failed! check function returned False. ' \
                          f'check={self.target_config.name}, vars={context.vars}'
                self.collect_report(message=message)
                raise SqlProcessorException(message)
            else:
                return

        check_data = df.limit(100).collect()
        if not check_data:
            message = f'Data for check must contains at least one row. Please check your sql. ' \
                      f'check={self.target_config.name}, check_data(limit 100)={check_data}, check_data_count={df.count()}'
            self.collect_report(message=message)
            raise SqlProcessorException(message)
        for check_item in check_data:
            check_data_as_dict = check_item.as_dict()
            if 'actual' not in check_data_as_dict or 'expected' not in check_data_as_dict:
                message = f'Data for check must contains expected and actual data. Please check your sql. ' \
                          f'check={self.target_config.name}, check_data(limit 100)={check_data}'
                self.collect_report(message=message)
                raise SqlProcessorException(message)
            if check_data_as_dict['actual'] != check_data_as_dict['expected']:
                message = f"check [{self.target_config.name}] failed! " \
                          f"actual={check_data_as_dict['actual']}, expected={check_data_as_dict['expected']}, check_data(limit 100)={check_data}"
                logger.error(message)
                self.collect_report(message=message)
                raise SqlProcessorException(message)
        logger.info(f'check [{self.target_config.name}] passed! check_data(limit 100)={check_data}')
        self.collect_report(message=f"check_data(limit 100)={check_data}")

    def collect_report(self, status=None, message=None):
        self.reporter_collector.collect_report(self, status=status, message=message)


class StepFactory:

    def __init__(self, reporter: ReportCollector, func_runner: FuncRunner):
        self.reporter = reporter
        self.func_runner = func_runner

    def create_from_sql(self, sql: str, includes: Dict[str, str] = None) -> List[Step]:
        includes = includes or {}
        resolved_sql = self._resolve_include(sql, includes)
        lines = resolved_sql.split('\n')

        index = 0
        sql_parts = []
        step_list = []
        step = Step(f'step-{len(step_list) + 1}', self.reporter, self.func_runner)
        while index < len(lines):
            line = lines[index].replace(";", '')
            line_stripped = line.strip()
            if re.compile(StepConfig.STEP_CONFIG_PATTERN, flags=re.IGNORECASE).match(line_stripped):
                if len(sql_parts) > 0:
                    step.select_sql = '\n'.join(sql_parts)
                if step.target_config is not None:
                    step_list.append(step)
                step = Step(f'step-{len(step_list) + 1}', self.reporter, self.func_runner)
                sql_parts = []
                target_config = StepConfig.from_config_line(line_stripped, index + 1)
                step.target_config = target_config
                if index == len(lines) - 1:
                    step_list.append(step)
            elif index == len(lines) - 1:
                if '' != line_stripped:
                    sql_parts.append(line)
                if len(sql_parts) > 0:
                    step.select_sql = '\n'.join(sql_parts)
                step_list.append(step)
            elif '' != line_stripped:
                sql_parts.append(line)
            index += 1
        return step_list

    def _resolve_include(self, sql, includes: Dict[str, str]) -> str:
        include_sql_pattern = r'^--\s*include\s*=\s*(.*\.sql)\s*$'
        include_py_pattern = r'^--\s*include\s*=\s*(.*)\.(\w+|\*)$'
        lines = sql.split('\n')
        resoloved_sqls = []
        for index, line in enumerate(lines):
            line = line.replace(";", '')
            line_stripped = line.strip()
            if re.match(include_sql_pattern, line_stripped, flags=re.IGNORECASE):
                matches = re.match(include_sql_pattern, line_stripped, flags=re.IGNORECASE)
                if len(matches.groups()) != 1:
                    raise SqlProcessorException(f'parse include config failed. must provide complete module name and the sql variable name.'
                                                f'bug got config line {line_stripped}')
                file = matches.group(1)
                if file in includes:
                    resoloved_sqls.append(includes[file])
                    continue

                try:
                    import importlib
                    func_mod = importlib.import_module('common.file_reader')
                    read_file_func = getattr(func_mod, 'read_file')
                    resoloved_sqls.append(read_file_func(file))
                except ModuleNotFoundError:
                    logger.info(f'failed to import common.file_reader, will try default file reader')
                    resoloved_sqls.append(SqlSnippetsReader.read_file(file))
            elif re.match(include_py_pattern, line_stripped, flags=re.IGNORECASE):
                matches = re.match(include_py_pattern, line_stripped, flags=re.IGNORECASE)
                if len(matches.groups()) != 2:
                    raise SqlProcessorException(f'parse include config failed. must provide complete module name and the sql variable name.'
                                                f'bug got config line {line_stripped}')
                module = matches.group(1)
                sql_name = matches.group(2)
                import importlib
                snippet_mod = importlib.import_module(module)
                resoloved_sqls.append(getattr(snippet_mod, sql_name))
            else:
                resoloved_sqls.append(line)
        resolved_sql = '\n'.join(resoloved_sqls)
        if self._need_resolve(resolved_sql, include_sql_pattern, include_py_pattern):
            return self._resolve_include(resolved_sql, includes)
        return resolved_sql

    def _need_resolve(self, resolved_sql, include_sql_pattern, include_py_pattern):
        lines = resolved_sql.split('\n')
        return any((self._match_sql_or_py_pattern(include_py_pattern, include_sql_pattern, line.replace(";", '').strip())
                    for line in lines))

    def _match_sql_or_py_pattern(self, include_py_pattern, include_sql_pattern, line):
        return re.match(include_sql_pattern, line, flags=re.IGNORECASE) \
               or re.match(include_py_pattern, line, flags=re.IGNORECASE)


class SqlSnippetsReader:

    @staticmethod
    def read_file(file_name: str, base_path: str = None) -> str:
        possible_paths = [file_name]
        if base_path is not None:
            possible_paths.append(path.join(base_path, file_name))

        for p in possible_paths:
            if not path.exists(p):
                logger.info(f'file not found, tried: {p}')
                continue
            logger.info(f'read file at path: {p}')
            with open(p) as f:
                return f.read()

        raise FileNotFoundError(f'file not found: tried_paths={possible_paths}')
