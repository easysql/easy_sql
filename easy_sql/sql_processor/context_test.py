import unittest

from easy_sql.sql_processor.context import TemplatesContext, VarsContext
from easy_sql.sql_processor.funcs import FuncRunner


class TemplateContextTest(unittest.TestCase):
    def test_should_replace_template(self):
        tc = TemplatesContext(True, {"a": "xx\n#{var}=abc, 123"})
        replaced = tc.replace_templates("??@{a(var=${abc})}??")
        self.assertEquals("??xx\n${abc}=abc, 123??", replaced)

        # does not support var-func in template parameters
        replaced = tc.replace_templates("??@{a(var=${fn(abc)})}??")
        self.assertNotEquals("??xx\n${fn(abc)}=abc, 123??", replaced)

    def test_multi_line_in_template_reference(self):
        tc = TemplatesContext(True, {"a": "xx\n#{var}=abc, #{var1} 123"})
        replaced = tc.replace_templates("??@{a(var=123\n,var1=234)}??")
        self.assertEquals("??xx\n123=abc, 234 123??", replaced)

        replaced = tc.replace_templates("??@{a(var=123,\nvar1=234)}??")
        self.assertEquals("??xx\n123=abc, 234 123??", replaced)

        replaced = tc.replace_templates("??@{a(\n  var\n=123\n,\nvar1=234)}??")
        self.assertEquals("??xx\n123=abc, 234 123??", replaced)


class VarsContextTest(unittest.TestCase):
    def test_should_replace_vars(self):
        vc = VarsContext(vars={"a": "##A##", "aa": "##${a}##"}, debug_log=True)
        self.assertEqual("-##A##, ===####A####===", vc.replace_variables("-${a}, ===${aa}==="))
        # if this is a comment, do not replace
        self.assertEqual("-- -${a}, ===${aa}===", vc.replace_variables("-- -${a}, ===${aa}==="))
        self.assertEqual("-##A##, ==-- =${aa}===", vc.replace_variables("-${a}, ==-- =${aa}==="))
        self.assertEqual("-\\##A##, ===####A####===", vc.replace_variables("-\\${a}, ===${aa}==="))

        vc = VarsContext(vars={"a": "##A##", "aa": "##${a}##", "b": "1"}, debug_log=True)
        vc.init(func_runner=FuncRunner({"f": lambda x: int(x) + 1}))
        self.assertEqual("-6, ===####A####===", vc.replace_variables("-${f(5)}, ===${aa}==="))
        self.assertEqual("-2, ===####A####===", vc.replace_variables("-${f(${b})}, ===${aa}==="))

        # TODO: support for confliction detection
