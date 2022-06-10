import logging
from typing import List

import colorlog
import logging

from sqlfluff.core import SQLBaseError

LOG_LEVEL = logging.DEBUG


class SqlLinterReporter:
    def __init__(self):
        self.logger = self.create_logger()

    @staticmethod
    def get_extra_default_dict():
        return {"pos_info": "",
                "description": "", "warn": ""}

    @staticmethod
    def create_logger():
        logger = logging.getLogger('linter_logger')
        logger.setLevel(LOG_LEVEL)
        info_formater = colorlog.ColoredFormatter(
            fmt=(
                "%(white)s%(message)s"
                "%(red)s%(warn)s "
                "%(blue)s%(pos_info)s "
                "%(white)s%(description)s "
            )
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(info_formater)
        logger.addHandler(console_handler)
        return logger

    def log_out_violation(self, violation: SQLBaseError):
        pos_info = "L: {} | P: {}: | {}  :".format(violation.line_pos,violation.line_pos,violation.rule_code())
        extra_dict = self.get_extra_default_dict()
        extra_dict["pos_info"] = pos_info
        extra_dict["description"] = violation.desc()
        self.logger.debug("", extra=extra_dict)

    def log_out_list_of_violations(self, lint_result: List[SQLBaseError]):
        if len(lint_result)>0:
            self.log_out_warning("Fail")
            for violation in lint_result:
                self.log_out_violation(violation)
        else:
            self.log_out_warning("Pass")

    def log_out_message(self, message):
        self.logger.info(message, extra=self.get_extra_default_dict())

    def log_out_message_with_conclude(self, message: str, conclude: str):
        extra_dict = self.get_extra_default_dict()
        extra_dict["warn"] = conclude
        self.logger.info(message, extra=extra_dict)

    def log_out_warning(self, conclude):
        extra_dict = self.get_extra_default_dict()
        extra_dict["warn"] = conclude
        self.logger.warning("", extra=extra_dict)


if __name__ == '__main__':
    report_logger = SqlLinterReporter()
    report_logger.log_out_message_with_conclude("=====file:test.sql start lint===== ", "pass")
    report_logger.log_out_message("=====file:test.sql  end  lint===== ")
    report_logger.log_out_warning("have unparsable element")
