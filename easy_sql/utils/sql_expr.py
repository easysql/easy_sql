from __future__ import annotations

import re
from typing import List, Optional


class CommentSubstitutor:
    COMMENT_IDENTIFIABLE_NAME = "__COMMENT_SUBSTITUTED__"

    def __init__(self, comment_identifiable_name: Optional[str] = None):
        self.comment_identifiable_name: str = (
            comment_identifiable_name
            if comment_identifiable_name is not None
            else CommentSubstitutor.COMMENT_IDENTIFIABLE_NAME
        )
        self.recognized_comment = []

    def remove(self, sql_expr: str) -> str:
        replaced = self.substitute(sql_expr, True)
        return replaced

    def substitute(self, sql_expr: str, replace_with_empty_str: bool = False) -> str:
        if self.comment_identifiable_name in sql_expr:
            raise Exception(
                "Cannot handle sql expression with comment identifiable"
                f" name({self.comment_identifiable_name}) inside: {sql_expr}"
            )
        lines = []
        recognized_comment = []

        # TODO: support of comment pattern: /* ... */
        for line in sql_expr.split("\n"):
            if line.startswith("--"):
                lines.append(self._get_replacement(recognized_comment, replace_with_empty_str))
                recognized_comment.append(line)
                continue

            current_index = 0
            while True:
                m = re.match(r".*?([^-]--).*", line[current_index:])
                if m:
                    comment_start = m.start(1) + 1
                    left_of_comment = line[: current_index + comment_start]
                    if CommentSubstitutor.is_quote_closed(left_of_comment):
                        lines.append(
                            left_of_comment + self._get_replacement(recognized_comment, replace_with_empty_str)
                        )
                        recognized_comment.append(line[current_index + comment_start :])
                        break
                    else:
                        current_index += comment_start
                else:
                    lines.append(line)
                    break

        self.recognized_comment = recognized_comment
        return "\n".join(lines)

    def _get_replacement(self, recognized_comment: List[str], replace_with_empty_str: bool) -> str:
        return (
            self.comment_identifiable_name + str(len(recognized_comment)) + "__" if not replace_with_empty_str else ""
        )

    @staticmethod
    def is_quote_closed(sql_expr: str) -> bool:
        sql_expr = sql_expr.replace("\\\\", "")

        def find_char_without_escape(text: str, char: str) -> int:
            start = 0
            while True:
                idx = text.find(char, start)
                if idx != -1:
                    if idx > 0 and text[idx - 1] == "\\":
                        start = idx + 1
                    else:
                        return idx
                else:
                    return -1

        def _is_quote_closed(quote_index: int, quote_char: str) -> bool:
            start_index = quote_index + 1
            char_without_escape_idx = find_char_without_escape(sql_expr[start_index:], quote_char)
            if char_without_escape_idx == -1:
                return False
            else:
                return CommentSubstitutor.is_quote_closed(sql_expr[start_index + char_without_escape_idx + 1 :])

        # a valid sql_expr should not starts with \' or \", so we don't need to check \\ when find quote start point
        single_quote_index = sql_expr.find("'")
        double_quote_index = sql_expr.find('"')
        if single_quote_index != -1 and double_quote_index == -1:
            return _is_quote_closed(single_quote_index, "'")
        elif single_quote_index != -1 and double_quote_index != -1:
            if single_quote_index < double_quote_index:
                return _is_quote_closed(single_quote_index, "'")
            else:
                return _is_quote_closed(double_quote_index, '"')
        elif single_quote_index == -1 and double_quote_index == -1:
            return True
        elif single_quote_index == -1 and double_quote_index != -1:
            return _is_quote_closed(double_quote_index, '"')
        else:
            raise Exception("should not happen")

    def recover(self, substituted_sql_expr: str) -> str:
        lines = []
        comment_index = 0
        for line in substituted_sql_expr.split("\n"):
            identifiable_name = f"{self.comment_identifiable_name}{comment_index}__"
            identifiable_name_count = line.count(identifiable_name)
            if identifiable_name_count > 1:
                raise Exception(f"found multiple comment identifiable name {identifiable_name} in line: {line}")
            if identifiable_name_count == 1 and not line.endswith(identifiable_name):
                raise Exception(
                    f"found comment identifiable name {identifiable_name}, but is not at end in line: {line}"
                )
            if identifiable_name_count == 1:
                lines.append(line.replace(identifiable_name, self.recognized_comment[comment_index]))
                comment_index += 1
            else:
                lines.append(line)
        return "\n".join(lines)
