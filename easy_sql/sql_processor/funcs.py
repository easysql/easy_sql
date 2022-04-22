from typing import Dict, Callable

from .backend import Backend, SparkBackend
from .backend.rdb import RdbBackend
from .common import SqlProcessorException, VarsReplacer

__all__ = [
    'FuncRunner'
]


class FuncRunner:
    _instance = None

    def __init__(self, funcs: Dict[str, Callable] = None):
        self.funcs: Dict[str, Callable] = funcs or {}

    def register_funcs(self, funcs: Dict[str, Callable]):
        self.funcs.update(funcs)

    @staticmethod
    def create(backend: Backend) -> 'FuncRunner':
        import builtins
        builtin_funcs = dict([(func, getattr(builtins, func)) for func in dir(builtins)
                              if not func.startswith('_') and func[0].islower() and callable(getattr(builtins, func))])
        import operator
        operator_funcs = dict([(func, getattr(operator, func)) for func in dir(operator)
                               if not func.startswith('_') and func[0].islower() and callable(getattr(operator, func))])
        operator_funcs.update({'equal': operator.eq})
        all_funcs = {}
        all_funcs.update(builtin_funcs)
        all_funcs.update(operator_funcs)
        all_funcs.update({
            'is_greater_or_equal': lambda a, b: a >= b,
            'equal_ignore_case': lambda a, b: a.lower() == b.lower(),
        })

        if isinstance(backend, (SparkBackend, )):
            all_funcs.update(FuncRunner._get_spark_funcs(backend))
        elif isinstance(backend, (RdbBackend, )):
            all_funcs.update(FuncRunner._get_rdb_funcs(backend))

        FuncRunner._instance = FuncRunner(all_funcs)
        return FuncRunner._instance

    @staticmethod
    def _get_rdb_funcs(backend) -> Dict[str, Callable]:
        from easy_sql.sql_processor.funcs_rdb import PartitionFuncs, ColumnFuncs, TableFuncs
        partition_funcs = PartitionFuncs(backend)
        col_funcs = ColumnFuncs(backend)
        table_funcs = TableFuncs(backend)
        return {
            'partition_exists': partition_funcs.partition_exists,
            'partition_not_exists': partition_funcs.partition_not_exists,
            'is_first_partition': partition_funcs.is_first_partition,
            'is_not_first_partition': partition_funcs.is_not_first_partition,
            'previous_partition_exists': partition_funcs.previous_partition_exists,
            'get_partition_or_first_partition': partition_funcs.get_partition_or_first_partition,
            'ensure_dwd_partition_exists': partition_funcs.ensure_dwd_partition_exists,
            'get_partition_col': partition_funcs.get_partition_col,
            'get_first_partition': partition_funcs.get_first_partition,
            'get_last_partition': partition_funcs.get_last_partition,
            'ensure_partition_exists': partition_funcs.ensure_partition_exists,
            'ensure_partition_or_first_partition_exists': partition_funcs.ensure_partition_or_first_partition_exists,
            'all_cols_without_one_expr': col_funcs.all_cols_without_one_expr,
            'all_cols_with_exclusion_expr': col_funcs.all_cols_with_exclusion_expr,
            'ensure_no_null_data_in_table': table_funcs.ensure_no_null_data_in_table,
            'check_not_null_column_in_table': table_funcs.check_not_null_column_in_table,
        }

    @staticmethod
    def _get_spark_funcs(backend) -> Dict[str, Callable]:
        from easy_sql.sql_processor.funcs_spark import ParallelismFuncs, PartitionFuncs, \
            CacheFuncs, ColumnFuncs, TableFuncs, IOFuncs, ModelFuncs
        spark = backend.spark
        partition_funcs = PartitionFuncs(backend)
        parallelism_funcs = ParallelismFuncs(spark)
        cache_funcs = CacheFuncs(spark)
        col_funcs = ColumnFuncs(backend)
        table_funcs = TableFuncs(backend)
        io_funcs = IOFuncs(spark)
        model_funcs = ModelFuncs(spark)
        return {
            'repartition': parallelism_funcs.repartition,
            'repartition_by_column': parallelism_funcs.repartition_by_column,
            'coalesce': parallelism_funcs.coalesce,
            'set_shuffle_partitions': parallelism_funcs.set_shuffle_partitions,
            'partition_exists': partition_funcs.partition_exists,
            'partition_not_exists': partition_funcs.partition_not_exists,
            'is_first_partition': partition_funcs.is_first_partition,
            'is_not_first_partition': partition_funcs.is_not_first_partition,
            'previous_partition_exists': partition_funcs.previous_partition_exists,
            'get_partition_or_first_partition': partition_funcs.get_partition_or_first_partition,
            'ensure_dwd_partition_exists': partition_funcs.ensure_dwd_partition_exists,
            'get_partition_col': partition_funcs.get_partition_col,
            'get_first_partition': partition_funcs.get_first_partition,
            'get_last_partition': partition_funcs.get_last_partition,
            'ensure_partition_exists': partition_funcs.ensure_partition_exists,
            'ensure_partition_or_first_partition_exists': partition_funcs.ensure_partition_or_first_partition_exists,
            'all_cols_without_one_expr': col_funcs.all_cols_without_one_expr,
            'all_cols_with_exclusion_expr': col_funcs.all_cols_with_exclusion_expr,
            'ensure_no_null_data_in_table': table_funcs.ensure_no_null_data_in_table,
            'check_not_null_column_in_table': table_funcs.check_not_null_column_in_table,
            'unpersist': cache_funcs.unpersist,
            'write_csv': io_funcs.write_csv,
            'rename_csv_output': io_funcs.rename_csv_output,
            'write_json_local': io_funcs.write_json_local,
            'update_json_local': io_funcs.update_json_local,
            'model_predict': model_funcs.model_predict,
        }

    def run_func(self, func_def: str, vars_replacer: VarsReplacer) -> bool:
        func_name = func_def[:func_def.index('(')]
        if func_name not in self.funcs:
            raise SqlProcessorException(f'no function found for {func_def} in sql_processor: {func_def}')
        func = self.funcs[func_name]
        try:
            original_params = func_def[func_def.index('(') + 1: func_def.index(')')].strip()
        except ValueError:
            raise SqlProcessorException('parse params failed for func definition: ' + func_def)
        params = []
        if original_params:
            params = original_params.split(',')
            params = [vars_replacer.replace_variables(p.strip(), False) for p in params]
        return func(*params)

