import unittest

from easy_sql.sql_processor.context import TemplatesContext, VarsContext
from easy_sql.sql_processor.funcs import FuncRunner


class TemplateContextTest(unittest.TestCase):
    def test_should_replace_template(self):
        tc = TemplatesContext(True, {"a": "xx\n#{var}=abc, 123"})
        replaced = tc.replace_templates("??@{a(var=${abc})}??")
        self.assertEqual("??xx\n${abc}=abc, 123??", replaced)

        # does not support var-func in template parameters
        replaced = tc.replace_templates("??@{a(var=${fn(abc)})}??")
        self.assertNotEquals("??xx\n${fn(abc)}=abc, 123??", replaced)

        # if this is a comment, do not replace
        replaced = tc.replace_templates("??@{a(var=${abc})}?? --??@{a(var=${abc})}??")
        self.assertEqual("??xx\n${abc}=abc, 123?? --??@{a(var=${abc})}??", replaced)
        replaced = tc.replace_templates("-- ??@{a(var=${abc})}??")
        self.assertEqual("-- ??@{a(var=${abc})}??", replaced)

    def test_multi_line_in_template_reference(self):
        tc = TemplatesContext(True, {"a": "xx\n#{var}=abc, #{var1} 123"})
        replaced = tc.replace_templates("??@{a(var=123\n,var1=234)}??")
        self.assertEqual("??xx\n123=abc, 234 123??", replaced)

        replaced = tc.replace_templates("??@{a(var=123,\nvar1=234)}??")
        self.assertEqual("??xx\n123=abc, 234 123??", replaced)

        replaced = tc.replace_templates("??@{a(\n  var\n=123\n,\nvar1=234)}??")
        self.assertEqual("??xx\n123=abc, 234 123??", replaced)

    def test_comment_line_in_template_reference(self):
        tc = TemplatesContext(True, {"a": "--xx\n#{var}=abc, #{var1} 123--abc\n--abc"})
        replaced = tc.replace_templates("??@{a(var=123\n,var1=234)}??")
        self.assertEqual("??--xx\n123=abc, 234 123--abc\n--abc\n??", replaced)

        tc = TemplatesContext(True, {"a": "--xx\n#{var}=abc, #{var1} 123--abc\n"})
        replaced = tc.replace_templates("??@{a(var=123\n,var1=234)}??")
        self.assertEqual("??--xx\n123=abc, 234 123--abc\n??", replaced)

        tc = TemplatesContext(True, {"a": "--xx\n#{var}=abc, #{var1} 123--abc"})
        replaced = tc.replace_templates("??@{a(var=123\n,var1=234)}??")
        self.assertEqual("??--xx\n123=abc, 234 123--abc\n??", replaced)

        tc = TemplatesContext(True, {"a": "\n#{var}=abc, #{var1} 123\n"})
        replaced = tc.replace_templates("??@{a(var=123\n,var1=234)}??")
        self.assertEqual("??123=abc, 234 123??", replaced)


class VarsContextTest(unittest.TestCase):
    def test_should_replace_vars(self):
        vc = VarsContext(vars={"a": "##A##", "aa": "##${a}##"}, debug_log=True)
        self.assertEqual("-##A##, ===####A####===", vc.replace_variables("-${a}, ===${aa}==="), "should replace all")
        self.assertEqual(
            "-- -${a}, ===${aa}===", vc.replace_variables("-- -${a}, ===${aa}==="), "do not replace comment"
        )
        self.assertEqual(
            "-##A##, ==-- =${aa}===", vc.replace_variables("-${a}, ==-- =${aa}==="), "do not replace comment"
        )
        self.assertEqual("-\\##A##, ===####A####===", vc.replace_variables("-\\${a}, ===${aa}==="), "ignore escaping")

        vc = VarsContext(vars={"a": "##A##", "b": "##${a}##", "aa": "##${b}##"}, debug_log=True)
        self.assertEqual(
            "-##A##, -####A####, ===######A######===",
            vc.replace_variables("-${a}, -${b}, ===${aa}==="),
            "replace vars recursively",
        )

        vc = VarsContext(vars={"a": "##A##", "aa": "##${a}##", "b": "1"}, debug_log=True)
        vc.init(func_runner=FuncRunner({"f": lambda x: int(x) + 1}))
        self.assertEqual("-6, ===####A####===", vc.replace_variables("-${f(5)}, ===${aa}==="), "func call in vars")
        self.assertEqual(
            "-2, ===####A####===", vc.replace_variables("-${f(${b})}, ===${aa}==="), "vars as args in func call"
        )
        self.assertEqual(
            "-4, ===####A####===",
            vc.replace_variables("-${f(${c:3})}, ===${aa}==="),
            "vars with default value as args in func call",
        )

        vc = VarsContext(vars={"a": "##A##", "b": "##${a}##", "aa": "##${b}##"}, debug_log=True)
        self.assertEqual(
            "-1, -####A####, ===######A######===",
            vc.replace_variables("-${a1:1}, -${b}, ===${aa:b?x}==="),
            "vars with default value",
        )

        # TODO: support for confliction detection
