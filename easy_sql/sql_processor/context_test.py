import unittest

from easy_sql.sql_processor.context import CommentSubstitutor, TemplatesContext


class CommentSubstitutorTest(unittest.TestCase):
    def test_check_if_quote_closed(self):
        # self.assertTrue(CommentSubstitutor.is_quote_closed(''))
        # self.assertTrue(CommentSubstitutor.is_quote_closed('abc'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('""'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"a"'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\'"'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\""'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\\\\\""'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\\\\\""""'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\\\\\"""."'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\\\\\""."."'))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\\\\\""\'\''))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\\\\\""\'.\''))
        self.assertTrue(CommentSubstitutor.is_quote_closed('"\\\\\\"."\'.\''))
        self.assertFalse(CommentSubstitutor.is_quote_closed('"\\\\\\""\''))
        self.assertFalse(CommentSubstitutor.is_quote_closed('"\\\\\\"".\''))
        self.assertFalse(CommentSubstitutor.is_quote_closed('"\\\\\\"""'))
        self.assertFalse(CommentSubstitutor.is_quote_closed('"\\\\""'))
        self.assertTrue(CommentSubstitutor.is_quote_closed("''"))
        self.assertTrue(CommentSubstitutor.is_quote_closed("'a'"))
        self.assertTrue(CommentSubstitutor.is_quote_closed("'\"'"))
        self.assertTrue(CommentSubstitutor.is_quote_closed("'\\''"))
        self.assertTrue(CommentSubstitutor.is_quote_closed("'\\\\\\''"))
        self.assertFalse(CommentSubstitutor.is_quote_closed("'\\\\\\''\""))
        self.assertFalse(CommentSubstitutor.is_quote_closed("'\\\\\\'''"))
        self.assertFalse(CommentSubstitutor.is_quote_closed("'\\\\\\''.'"))
        self.assertFalse(CommentSubstitutor.is_quote_closed("'\\\\''"))

    def test_should_replace_and_recover_comment(self):
        sql_expr = """
select ${a}, ${b} -- ${a} in comment
, ',-- ' as c -- special comment
, ',--  as c -- something' -- special comment 1
, ",--  as c -- something" -- special comment 2 ${a}
, ",-- ${a}",--  as c -- something' -- special comment 3 ${a}
, ",\\"-- ${a}",--  as c -- something' -- special comment 4 ${a}
, ",-- ${a}\\\\",--  as c -- something' -- special comment 5 ${a}
, ",\\"-- ${a}\\\\",--  as c -- something' -- special comment 6 ${a}
, ",'-- ${a}",--  as c -- something' -- special comment 7 ${a}
, ',"-- ${a}',--  as c -- something' -- special comment 8 ${a}
, ',-- ' as c -- special comment
-- ${a} in comment
   ${a} -- some comment
        """
        sub = CommentSubstitutor()
        sql_expr = sub.substitute(sql_expr)
        print("\n".join(sub.recognized_comment))
        print(sql_expr)
        sql_expr = sql_expr.replace("${a}", "aaa")
        sql_expr = sql_expr.replace("${b}", "bbb\nbbb")
        self.assertEqual(
            sub.recover(sql_expr),
            """
select aaa, bbb
bbb -- ${a} in comment
, ',-- ' as c -- special comment
, ',--  as c -- something' -- special comment 1
, ",--  as c -- something" -- special comment 2 ${a}
, ",-- aaa",--  as c -- something' -- special comment 3 ${a}
, ",\\"-- aaa",--  as c -- something' -- special comment 4 ${a}
, ",-- aaa\\\\",--  as c -- something' -- special comment 5 ${a}
, ",\\"-- aaa\\\\",--  as c -- something' -- special comment 6 ${a}
, ",'-- aaa",--  as c -- something' -- special comment 7 ${a}
, ',"-- aaa',--  as c -- something' -- special comment 8 ${a}
, ',-- ' as c -- special comment
-- ${a} in comment
   aaa -- some comment
        """,
        )


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
