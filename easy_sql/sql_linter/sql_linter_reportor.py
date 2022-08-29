from __future__ import annotations

import codecs
import logging
import sys
from typing import TYPE_CHECKING, List, Union

import colorlog

if TYPE_CHECKING:
    from sqlfluff.core import SQLBaseError


class LintReporter:
    def __init__(self):
        self.sql_linter_log = LintReporter._create_logger(logging.DEBUG)

    def _get_extra_default_dict(self):
        return {"pos_info": "", "description": "", "warn": "", "pass": ""}

    @staticmethod
    def _create_logger(log_level: Union[int, str]):
        logger = logging.getLogger("linter_logger")
        logger.setLevel(log_level)
        info_formater = colorlog.ColoredFormatter(
            fmt="%(white)s%(message)s%(red)s%(warn)s %(green)s%(pass)s %(blue)s%(pos_info)s %(white)s%(description)s "
        )
        python_version = sys.version_info
        if python_version.major == 3 and python_version.minor == 6:
            sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())  # type: ignore
        elif hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(info_formater)
        for existing_handler in logger.handlers:
            logger.removeHandler(existing_handler)
        logger.addHandler(handler)
        return logger

    def report_violation(self, violation: SQLBaseError, step_start_line=0):
        pos_info = "L: {} | P: {}: | {}  :".format(
            violation.line_no + step_start_line, violation.line_pos, violation.rule_code()
        )
        extra_dict = self._get_extra_default_dict()
        extra_dict["pos_info"] = pos_info
        extra_dict["description"] = violation.desc()
        self.sql_linter_log.info("", extra=extra_dict)

    def report_list_of_violations(self, lint_result: List[SQLBaseError], step_start_line=0):
        if len(lint_result) > 0:
            self.report_warning("Fail")
            for violation in lint_result:
                self.report_violation(violation, step_start_line)
        else:
            self.report_pass("Pass")

    def report_message(self, message):
        self.sql_linter_log.info(message, extra=self._get_extra_default_dict())

    def report_warning(self, warning: str):
        extra_dict = self._get_extra_default_dict()
        extra_dict["warn"] = warning
        self.sql_linter_log.warning("", extra=extra_dict)

    def report_pass(self, pass_info: str):
        extra_dict = self._get_extra_default_dict()
        extra_dict["pass"] = pass_info
        self.sql_linter_log.warning("", extra=extra_dict)
