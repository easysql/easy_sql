import unittest

from .sql_expr import (
    CommentSubstitutor,
    comment_start,
    is_quote_closed,
    remove_semicolon,
)


class SqlExprTest(unittest.TestCase):
    def test_check_if_quote_closed(self):
        # self.assertTrue(CommentSubstitutor.is_quote_closed(''))
        # self.assertTrue(CommentSubstitutor.is_quote_closed('abc'))
        self.assertTrue(is_quote_closed('""'))
        self.assertTrue(is_quote_closed('"a"'))
        self.assertTrue(is_quote_closed('"\'"'))
        self.assertTrue(is_quote_closed('"\\""'))
        self.assertTrue(is_quote_closed('"\\\\\\""'))
        self.assertTrue(is_quote_closed('"\\\\\\""""'))
        self.assertTrue(is_quote_closed('"\\\\\\"""."'))
        self.assertTrue(is_quote_closed('"\\\\\\""."."'))
        self.assertTrue(is_quote_closed('"\\\\\\""\'\''))
        self.assertTrue(is_quote_closed('"\\\\\\""\'.\''))
        self.assertTrue(is_quote_closed('"\\\\\\"."\'.\''))
        self.assertFalse(is_quote_closed('"\\\\\\""\''))
        self.assertFalse(is_quote_closed('"\\\\\\"".\''))
        self.assertFalse(is_quote_closed('"\\\\\\"""'))
        self.assertFalse(is_quote_closed('"\\\\""'))
        self.assertTrue(is_quote_closed("''"))
        self.assertTrue(is_quote_closed("'a'"))
        self.assertTrue(is_quote_closed("'\"'"))
        self.assertTrue(is_quote_closed("'\\''"))
        self.assertTrue(is_quote_closed("'\\\\\\''"))
        self.assertFalse(is_quote_closed("'\\\\\\''\""))
        self.assertFalse(is_quote_closed("'\\\\\\'''"))
        self.assertFalse(is_quote_closed("'\\\\\\''.'"))
        self.assertFalse(is_quote_closed("'\\\\''"))

    def test_comment_start(self):
        self.assertEquals(comment_start("--abc"), 0)
        self.assertEquals(comment_start("-abc"), -1)
        self.assertEquals(comment_start("---,abc"), 0)
        self.assertEquals(comment_start(" ---,abc"), 1)
        self.assertEquals(comment_start('" -"--,abc'), 4)
        self.assertEquals(comment_start('" ---,abc'), -1)
        self.assertEquals(comment_start("' ---,abc"), -1)
        self.assertEquals(comment_start("' --'-,abc"), -1)
        self.assertEquals(comment_start("' -'--,abc"), 4)
        self.assertEquals(comment_start("' -''--',abc"), -1)
        self.assertEquals(comment_start("' -'--'--',abc"), 4)

    def test_remove_semicolon(self):
        self.assertEquals(remove_semicolon("select 1; select 2"), "select 1 select 2")
        self.assertEquals(remove_semicolon('select "1;" select 2'), 'select "1;" select 2')
        self.assertEquals(remove_semicolon('select --"1;" select 2'), 'select --"1;" select 2')
        self.assertEquals(remove_semicolon('select -"1";"; select 2'), 'select -"1""; select 2')
        self.assertEquals(remove_semicolon('select -"1";"; --select 2'), 'select -"1""; --select 2')
        self.assertEquals(remove_semicolon('\n\nselect -"1";"; --select 2'), '\n\nselect -"1""; --select 2')
        self.assertEquals(remove_semicolon(";;;"), "")
        self.assertEquals(remove_semicolon(";\n;;"), "\n")
        self.assertEquals(remove_semicolon(";  "), "  ")


class CommentSubstitutorTest(unittest.TestCase):
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

    def test_should_replace_and_recover_comment_with_no_leading_space_in_comment(self):
        sql_expr = """
select ${a}, ${b} --${a} in comment
, ',--' as c --special comment
, ',--  as c --something' --special comment 1
, ",--as c -- something" --special comment 2 ${a}
, ",-- ${a}",--  as c --something' --special comment 3 ${a}
, ",\\"-- ${a}",--as c -- something' --special comment 4 ${a}
, ",--${a}\\\\",--  as c -- something' --special comment 5 ${a}
, ",\\"-- ${a}\\\\",--  as c --something' --special comment 6 ${a}
, ",'-- ${a}",--  as c -- something' --special comment 7 ${a}
, ',"-- ${a}',--  as c -- something' --special comment 8 ${a}
, ',-- ' as c --special comment
--${a} in comment
   ${a} --some comment
        """
        sub = CommentSubstitutor()
        sql_expr = sub.substitute(sql_expr)
        print("\n".join(sub.recognized_comment))
        print(sql_expr)
        sql_expr = sql_expr.replace("${a}", "aaa")
        sql_expr = sql_expr.replace("${b}", "bbb\nbbb")
        print(sub.recover(sql_expr))
        self.assertEqual(
            sub.recover(sql_expr),
            """
select aaa, bbb
bbb --${a} in comment
, ',--' as c --special comment
, ',--  as c --something' --special comment 1
, ",--as c -- something" --special comment 2 ${a}
, ",-- aaa",--  as c --something' --special comment 3 ${a}
, ",\\"-- aaa",--as c -- something' --special comment 4 ${a}
, ",--aaa\\\\",--  as c -- something' --special comment 5 ${a}
, ",\\"-- aaa\\\\",--  as c --something' --special comment 6 ${a}
, ",'-- aaa",--  as c -- something' --special comment 7 ${a}
, ',"-- aaa',--  as c -- something' --special comment 8 ${a}
, ',-- ' as c --special comment
--${a} in comment
   aaa --some comment
        """,
        )

    def test_remove_comment(self):
        sql = """--comment
        with--other comment
        a as (--comment)
        )
        '"'--comment
        """
        print(CommentSubstitutor().remove(sql))
        self.assertEqual(
            """
        with
        a as (
        )
        '"'
        """,
            CommentSubstitutor().remove(sql),
        )
