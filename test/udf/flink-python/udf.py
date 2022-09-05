from pyflink.table import DataTypes
from pyflink.table.udf import udf

__all__ = ["test_func"]

@udf(result_type=DataTypes.BIGINT())
def test_func(a: int, b: int) -> int:
    return a + b