from typing import Callable, Dict, Any, Union, List


def remove_all_whitespaces(value: str):
    return "".join(value.split()) if value is not None else None


def trim_all(value: str):
    return value.strip() if value is not None else None


def all_udfs(cls: Any):
    return dict([(attr, getattr(cls, attr)) for attr in dir(cls) if callable(getattr(cls, attr)) and not attr.startswith('_') and attr != 'all'])


def get_udfs(type: str) -> Dict[str, Callable[[], Union[str, List[str]]]]:
    if type == 'pg':
        return PgUdfs.all()
    elif type == 'ch':
        return ChUdfs.all()
    else:
        return {}


class PgUdfs:
    @staticmethod
    def all() -> Dict[str, Callable[[], str]]:
        return all_udfs(PgUdfs)

    @staticmethod
    def trim_all():
        return '''
create or replace function trim_all(value text) returns text
    as $$ select regexp_replace(regexp_replace($1, E'^[\\\\a\\\\b\\\\e\\\\f\\\\n\\\\r\\\\t\\\\v\\\\0 ]+', ''), E'[\\\\a\\\\b\\\\e\\\\f\\\\n\\\\r\\\\t\\\\v\\\\0 ]+$', '') $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT
'''

    @staticmethod
    def split():
        return '''
create or replace function split(value text, sep text) returns text[]
    as $$ select string_to_array($1, $2) $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT
'''

    @staticmethod
    def from_unixtime():
        return '''
create or replace function from_unixtime(value float) returns timestamp
    as $$ select to_timestamp($1) $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT
'''

    @staticmethod
    def date_format():
        return '''
create or replace function date_format(value timestamp, format text) returns text
    as $$ select to_char($1, $2) $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT
'''

    @staticmethod
    def get_json_object():
        return '''
create or replace function get_json_object(value text, path text) returns text
    as $$ select $1::json#>(string_to_array($2, '.'))[2:] $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT
'''

    @staticmethod
    def sha1():
        return '''
CREATE EXTENSION IF NOT EXISTS pgcrypto with schema public;
create or replace function sha1(value text) returns text
    as $$ select encode(public.digest($1::bytea, cast('sha1' as text)), 'hex') $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT
'''.split(';')


class ChUdfs:

    @staticmethod
    def all() -> Dict[str, Callable[[], str]]:
        return all_udfs(ChUdfs)

