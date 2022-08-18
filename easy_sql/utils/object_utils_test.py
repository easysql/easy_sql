import unittest

from easy_sql.utils.object_utils import get_attr


class ObjectUtilsTest(unittest.TestCase):
    def test_get_attr(self):
        self.assertEqual(get_attr({}, "a.b.c"), {})
        self.assertEqual(get_attr({"a": {}}, "a.b.c"), {})
        self.assertEqual(get_attr({"a": {"b": {"c": [1, 2, 3]}}}, "a.b.c"), [1, 2, 3])

        self.assertEqual(get_attr({}, "a"), {})
        self.assertEqual(get_attr({"a": ""}, "a"), "")

        self.assertEqual(get_attr({"a": 1}, ""), {"a": 1})


if __name__ == "__main__":
    unittest.main()
