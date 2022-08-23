import unittest
from unittest.mock import patch

from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.engine import Inspector, create_mock_engine

from easy_sql.sql_processor.backend.rdb import RdbBackend


class RdbTest(unittest.TestCase):
    def test_get_column_names_should_only_get_from_name(self):
        cols = [{"name": "a"}, {"name": "b"}, {"type": "c"}]
        with patch.object(RdbBackend, "get_columns", return_value=cols):
            rdb = RdbBackend("")
            names = rdb.get_column_names("test")
            self.assertSequenceEqual(names, ["a", "b"])

    def test_get_columns_should_compile_type_by_dialect_when_now_raw(self):
        mock_engine = create_mock_engine("postgresql://", None)
        mock_engine.close = lambda: None
        col = {"name": "id", "type": DOUBLE_PRECISION(10)}
        raw_cols = [col]
        with patch.object(Inspector, "get_columns", return_value=[col.copy() for col in raw_cols]):
            rdb = RdbBackend("")
            rdb.engine = mock_engine

            cols = rdb.get_columns("test")

            self.assertNotEqual(str(col["type"]), "DOUBLE PRECISION")
            self.assertEqual(cols, [{"name": "id", "type": "DOUBLE PRECISION"}])

    def test_get_columns_should_compile_type_by_dialect_when_in_raw(self):
        mock_engine = create_mock_engine("postgresql://", None)
        mock_engine.close = lambda: None
        col = {"name": "id", "type": DOUBLE_PRECISION(10)}
        raw_cols = [col]
        with patch.object(Inspector, "get_columns", return_value=[col.copy() for col in raw_cols]):
            rdb = RdbBackend("")
            rdb.engine = mock_engine

            cols = rdb.get_columns("test", raw=True)

            self.assertEqual(cols, raw_cols)
