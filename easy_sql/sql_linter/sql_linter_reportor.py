from typing import List
import codecs
import colorlog
import logging
import sys
from sqlfluff.core import SQLBaseError

LOG_LEVEL = logging.DEBUG


def _get_extra_default_dict():
    return {"pos_info": "",
            "description": "", "warn": "", "pass": ""}


def _create_logger():
    logger = logging.getLogger('linter_logger')
    logger.setLevel(LOG_LEVEL)
    info_formater = colorlog.ColoredFormatter(
        fmt=(
            "%(white)s%(message)s"
            "%(red)s%(warn)s "
            "%(green)s%(pass)s "
            "%(blue)s%(pos_info)s "
            "%(white)s%(description)s "
        )
    )
    python_version = sys.version_info
    if python_version.major == 3 and python_version.minor == 6:
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    elif hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(info_formater)
    for existing_handler in logger.handlers:
        logger.removeHandler(existing_handler)
    logger.addHandler(handler)
    return logger


def log_violation(violation: SQLBaseError, step_start_line=0):
    pos_info = "L: {} | P: {}: | {}  :".format(violation.line_no + step_start_line,
                                               violation.line_pos, violation.rule_code())
    extra_dict = _get_extra_default_dict()
    extra_dict["pos_info"] = pos_info
    extra_dict["description"] = violation.desc()
    sql_linter_log.info("", extra=extra_dict)


def log_list_of_violations(lint_result: List[SQLBaseError], step_start_line=0):
    if len(lint_result) > 0:
        log_warning("Fail")
        for violation in lint_result:
            log_violation(violation, step_start_line)
    else:
        log_pass("Pass")


def log_message(message):
    sql_linter_log.info(message, extra=_get_extra_default_dict())


def log_warning(warning: str):
    extra_dict = _get_extra_default_dict()
    extra_dict["warn"] = warning
    sql_linter_log.warning("", extra=extra_dict)


def log_pass(pass_info: str):
    extra_dict = _get_extra_default_dict()
    extra_dict["pass"] = pass_info
    sql_linter_log.warning("", extra=extra_dict)


sql_linter_log = _create_logger()
