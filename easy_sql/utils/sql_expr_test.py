import unittest

from .sql_expr import CommentSubstitutor


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
