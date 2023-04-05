from typing import Callable, Dict, List, Optional, Tuple

from .backend import Backend, FlinkBackend, SparkBackend
from .backend.rdb import RdbBackend
from .common import SqlProcessorException, VarsReplacer

__all__ = ["FuncRunner"]


EASYSQL_FUNCS = {
    "is_greater_or_equal": lambda a, b: a >= b,
    "equal_ignore_case": lambda a, b: a.lower() == b.lower(),
}


class FuncRunner:
    _instance = None

    def __init__(self, funcs: Optional[Dict[str, Callable]] = None):
        self.funcs: Dict[str, Callable] = funcs or {}

    def register_funcs(self, funcs: Dict[str, Callable]):
        self.funcs.update(funcs)

    @staticmethod
    def system_funcs() -> Dict[str, Callable]:
        import builtins

        builtin_funcs = {
            func: getattr(builtins, func)
            for func in dir(builtins)
            if not func.startswith("_") and func[0].islower() and callable(getattr(builtins, func))
        }

        import operator

        operator_funcs = {
            func: getattr(operator, func)
            for func in dir(operator)
            if not func.startswith("_") and func[0].islower() and callable(getattr(operator, func))
        }
        operator_funcs.update({"equal": operator.eq})
        all_funcs = {}
        all_funcs.update(builtin_funcs)
        all_funcs.update(operator_funcs)
        return all_funcs

    @staticmethod
    def easysql_funcs() -> Dict[str, Callable]:
        return EASYSQL_FUNCS

    @staticmethod
    def create(backend: Backend) -> "FuncRunner":
        all_funcs = FuncRunner.system_funcs()
        all_funcs.update(FuncRunner.easysql_funcs())

        if isinstance(backend, (SparkBackend,)):
            all_funcs.update(FuncRunner._get_spark_funcs(backend))
        elif isinstance(backend, (RdbBackend,)):
            all_funcs.update(FuncRunner._get_rdb_funcs(backend))
        elif isinstance(backend, (FlinkBackend,)):
            all_funcs.update(FuncRunner._get_flink_funcs(backend))

        FuncRunner._instance = FuncRunner(all_funcs)
        return FuncRunner._instance

    @staticmethod
    def _get_rdb_funcs(backend) -> Dict[str, Callable]:
        from easy_sql.sql_processor.funcs_rdb import (
            AnalyticsFuncs,
            ColumnFuncs,
            IOFuncs,
            ModelFuncs,
            PartitionFuncs,
            TableFuncs,
            TestFuncs,
        )

        partition_funcs = PartitionFuncs(backend)
        col_funcs = ColumnFuncs(backend)
        table_funcs = TableFuncs(backend)
        model_funcs = ModelFuncs(backend)
        io_funcs = IOFuncs()
        ana_funcs = AnalyticsFuncs(backend)
        test_funcs = TestFuncs(backend)
        return {
            "partition_exists": partition_funcs.partition_exists,
            "partition_not_exists": partition_funcs.partition_not_exists,
            "is_first_partition": partition_funcs.is_first_partition,
            "is_not_first_partition": partition_funcs.is_not_first_partition,
            "previous_partition_exists": partition_funcs.previous_partition_exists,
            "get_partition_or_first_partition": partition_funcs.get_partition_or_first_partition,
            "ensure_dwd_partition_exists": partition_funcs.ensure_dwd_partition_exists,
            "ensure_table_partition_exists": partition_funcs.ensure_table_partition_exists,
            "ensure_table_partition_or_first_partition_exists": partition_funcs.ensure_table_partition_or_first_partition_exists,
            "get_partition_col": partition_funcs.get_partition_col,
            "get_first_partition": partition_funcs.get_first_partition,
            "get_last_partition": partition_funcs.get_last_partition,
            "ensure_partition_exists": partition_funcs.ensure_partition_exists,
            "ensure_partition_or_first_partition_exists": partition_funcs.ensure_partition_or_first_partition_exists,
            "get_partition_values_as_joined_str": partition_funcs.get_partition_values_as_joined_str,
            "all_cols_without_one_expr": col_funcs.all_cols_without_one_expr,
            "all_cols_with_exclusion_expr": col_funcs.all_cols_with_exclusion_expr,
            "bq_model_predict_with_local_spark": model_funcs.bq_model_predict_with_local_spark,
            "model_predict_with_local_spark": model_funcs.model_predict_with_local_spark,
            "ensure_no_null_data_in_table": table_funcs.ensure_no_null_data_in_table,
            "check_not_null_column_in_table": table_funcs.check_not_null_column_in_table,
            "all_cols_prefixed_with_exclusion_expr": col_funcs.all_cols_prefixed_with_exclusion_expr,
            "move_file": io_funcs.move_file,
            "data_profiling_report": ana_funcs.data_profiling_report,
            "sleep": test_funcs.sleep,
        }

    @staticmethod
    def _get_flink_funcs(backend) -> Dict[str, Callable]:
        from easy_sql.sql_processor.funcs_flink import (
            AnalyticsFuncs,
            ColumnFuncs,
            ParallelismFuncs,
            StreamingFuncs,
            TableFuncs,
            TestFuncs,
        )

        parallelism_funcs = ParallelismFuncs(backend)
        col_funcs = ColumnFuncs(backend)
        table_funcs = TableFuncs(backend)
        ana_funcs = AnalyticsFuncs(backend)
        streaming_funcs = StreamingFuncs(backend)
        test_funcs = TestFuncs(backend)
        return {
            "all_cols_without_one_expr": col_funcs.all_cols_without_one_expr,
            "all_cols_with_exclusion_expr": col_funcs.all_cols_with_exclusion_expr,
            "all_cols_prefixed_with_exclusion_expr": col_funcs.all_cols_prefixed_with_exclusion_expr,
            "ensure_no_null_data_in_table": table_funcs.ensure_no_null_data_in_table,
            "check_not_null_column_in_table": table_funcs.check_not_null_column_in_table,
            "data_profiling_report": ana_funcs.data_profiling_report,
            "set_parallelism": parallelism_funcs.set_parallelism,
            "execute_streaming_inserts": streaming_funcs.execute_streaming_inserts,
            "sleep": test_funcs.sleep,
            "test_run_etl": test_funcs.test_run_etl,
            "exec_sql_in_source": test_funcs.exec_sql_in_source,
        }

    @staticmethod
    def _get_spark_funcs(backend) -> Dict[str, Callable]:
        from easy_sql.sql_processor.funcs_spark import (
            AnalyticsFuncs,
            CacheFuncs,
            ColumnFuncs,
            IOFuncs,
            LangFuncs,
            ModelFuncs,
            ParallelismFuncs,
            PartitionFuncs,
            TableFuncs,
            TestFuncs,
        )

        spark = backend.spark
        partition_funcs = PartitionFuncs(backend)
        parallelism_funcs = ParallelismFuncs(spark)
        cache_funcs = CacheFuncs(spark)
        col_funcs = ColumnFuncs(backend)
        table_funcs = TableFuncs(backend)
        io_funcs = IOFuncs(spark)
        model_funcs = ModelFuncs(spark)
        ana_funcs = AnalyticsFuncs(backend)
        test_funcs = TestFuncs(backend)
        lang_funcs = LangFuncs(backend)
        return {
            "repartition": parallelism_funcs.repartition,
            "repartition_by_column": parallelism_funcs.repartition_by_column,
            "coalesce": parallelism_funcs.coalesce,
            "set_shuffle_partitions": parallelism_funcs.set_shuffle_partitions,
            "partition_exists": partition_funcs.partition_exists,
            "partition_not_exists": partition_funcs.partition_not_exists,
            "is_first_partition": partition_funcs.is_first_partition,
            "is_not_first_partition": partition_funcs.is_not_first_partition,
            "previous_partition_exists": partition_funcs.previous_partition_exists,
            "get_partition_or_first_partition": partition_funcs.get_partition_or_first_partition,
            "ensure_dwd_partition_exists": partition_funcs.ensure_dwd_partition_exists,
            "ensure_table_partition_exists": partition_funcs.ensure_table_partition_exists,
            "ensure_table_partition_or_first_partition_exists": partition_funcs.ensure_table_partition_or_first_partition_exists,
            "get_partition_col": partition_funcs.get_partition_col,
            "get_first_partition": partition_funcs.get_first_partition,
            "get_last_partition": partition_funcs.get_last_partition,
            "ensure_partition_exists": partition_funcs.ensure_partition_exists,
            "ensure_partition_or_first_partition_exists": partition_funcs.ensure_partition_or_first_partition_exists,
            "get_partition_values_as_joined_str": partition_funcs.get_partition_values_as_joined_str,
            "all_cols_without_one_expr": col_funcs.all_cols_without_one_expr,
            "all_cols_with_exclusion_expr": col_funcs.all_cols_with_exclusion_expr,
            "all_cols_prefixed_with_exclusion_expr": col_funcs.all_cols_prefixed_with_exclusion_expr,
            "ensure_no_null_data_in_table": table_funcs.ensure_no_null_data_in_table,
            "check_not_null_column_in_table": table_funcs.check_not_null_column_in_table,
            "unpersist": cache_funcs.unpersist,
            "write_csv": io_funcs.write_csv,
            "rename_csv_output": io_funcs.rename_csv_output,
            "move_file": io_funcs.move_file,
            "write_json_local": io_funcs.write_json_local,
            "update_json_local": io_funcs.update_json_local,
            "model_predict": model_funcs.model_predict,
            "data_profiling_report": ana_funcs.data_profiling_report,
            "call_java": lang_funcs.call_java,
            "sleep": test_funcs.sleep,
        }

    def run_func(self, func_def: str, vars_replacer: VarsReplacer) -> Optional[str]:
        func_name, func, params = self._parse(func_def, vars_replacer)
        ret_val = func(*params)
        return self._ref_sql(func_name, func, ret_val, *params)

    def _ref_sql(self, func_name, func, func_call_ret: str, *params) -> str:
        if hasattr(func, "ref_sql"):
            assert callable(
                func.ref_sql
            ), f"Ref_sql of {func_name} should be a function to calculate the reference sql. But it is a {func.ref_sql}"
            return func.ref_sql(*params) + "\n;"
        else:
            return func_call_ret

    def _parse(self, func_def: str, vars_replacer: VarsReplacer) -> Tuple[str, Callable, List[str]]:
        func_name = func_def[: func_def.index("(")]
        if func_name not in self.funcs:
            raise SqlProcessorException(f"no function found for {func_def} in sql_processor: {func_def}")
        func = self.funcs[func_name]
        try:
            original_params = func_def[func_def.index("(") + 1 : func_def.index(")")].strip()
        except ValueError:
            raise SqlProcessorException("parse params failed for func definition: " + func_def)
        params = []
        if original_params:
            params = original_params.split(",")
            params = [vars_replacer.replace_variables(p.strip(), False) for p in params]
        return func_name, func, params
