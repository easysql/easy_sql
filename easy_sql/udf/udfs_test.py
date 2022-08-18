import unittest

from easy_sql.base_test import LocalSpark
from easy_sql.sql_processor import SqlProcessor


class FunctionsTest(unittest.TestCase):
    def test_remove_all_whitespaces(self):
        spark = LocalSpark.get()
        SqlProcessor(spark, "")

        self.assertEqual("ab", spark.sql("select remove_all_whitespaces('  a    b  ')").collect()[0][0])
        self.assertEqual("ab", spark.sql("select remove_all_whitespaces(' \ta\t    b\t ')").collect()[0][0])
        self.assertEqual("ab", spark.sql("select remove_all_whitespaces(' \na\n    b\n ')").collect()[0][0])
        self.assertEqual("ab", spark.sql("select remove_all_whitespaces(' \fa\f    b\f ')").collect()[0][0])
        self.assertEqual("ab", spark.sql("select remove_all_whitespaces(' \ra\r    b\r ')").collect()[0][0])
        self.assertEqual("ab", spark.sql("select remove_all_whitespaces(' \va\v    b\v ')").collect()[0][0])

        self.assertEqual("ab", spark.sql("select remove_all_whitespaces('  a     b  ')").collect()[0][0])  # \u00A0
        self.assertEqual("ab", spark.sql("select remove_all_whitespaces('  a     b  ')").collect()[0][0])  # \u2007
        self.assertEqual("ab", spark.sql("select remove_all_whitespaces('  a     b  ')").collect()[0][0])  # 202F
        self.assertEqual(None, spark.sql("select remove_all_whitespaces(NULL)").collect()[0][0])
        self.assertEqual("", spark.sql("select remove_all_whitespaces('')").collect()[0][0])
        self.assertEqual("", spark.sql("select remove_all_whitespaces('  ')").collect()[0][0])

    def test_trim_all(self):
        spark = LocalSpark.get()
        SqlProcessor(spark, "")

        self.assertEqual("a    b", spark.sql("select trim_all('  a    b  ')").collect()[0][0])
        self.assertEqual("a    b", spark.sql("select trim_all(' \ta    b\t ')").collect()[0][0])
        self.assertEqual("a    b", spark.sql("select trim_all(' \na    b\n ')").collect()[0][0])
        self.assertEqual("a    b", spark.sql("select trim_all(' \fa    b\f ')").collect()[0][0])
        self.assertEqual("a    b", spark.sql("select trim_all(' \ra    b\r ')").collect()[0][0])
        self.assertEqual("a    b", spark.sql("select trim_all(' \va    b\v ')").collect()[0][0])

        self.assertEqual("a    b", spark.sql("select trim_all('  a    b  ')").collect()[0][0])  # \u00A0
        self.assertEqual("a    b", spark.sql("select trim_all('  a    b  ')").collect()[0][0])  # \u2007
        self.assertEqual("a    b", spark.sql("select trim_all('  a    b  ')").collect()[0][0])  # 202F
        self.assertEqual(None, spark.sql("select trim_all(NULL)").collect()[0][0])
        self.assertEqual("", spark.sql("select trim_all('')").collect()[0][0])
        self.assertEqual("", spark.sql("select trim_all('  ')").collect()[0][0])


if __name__ == "__main__":
    unittest.main()
