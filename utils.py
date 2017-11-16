"""Set of helper utils, for example converters, validators, parsers etc."""

import logging

from datamaps import key_map, type_map

logger = logging.getLogger(__name__)


class QueryParseError(Exception):
    """
    Can be raised in case of errors related to query parsing.
    """
    pass


def convert(data):
    """
    Function to convert string-based params from CLI.
    Iterates through the type map to determine type of conversion.

    :param data: parsed data from CLI
    :return: dict with converted values
    """
    converted = {}
    for k, v in data.items():
        if k in type_map:
            try:
                mapped = {k: type_map[k](data[k])}
                converted.update(mapped)
            except ValueError:
                logger.error(
                    'Failed to recognize values: [{}] value '
                    'must be `{}`, not `{}`'.format(
                        k, type_map[k].__name__, type(k).__name__))
                exit(1)
    return converted


def validate_keys(key):
    """
    Function that validates key for existing in expected list.
    Raises `QueryParseError` if key not in expected list.

    :param key: key to check
    """
    if key not in key_map:
        raise QueryParseError


def parse_query(q_args, delimiter=':'):
    """
    Function that parses arguments and prepares query.
    Query is used for searching in MongoDB.

    :param q_args: list of arguments to parse.
    :param delimiter: delimiter to split arguments.
    :return: parsed query
    """
    query = {}
    for _arg in q_args:
        try:
            _arg = _arg.split(delimiter)
            validate_keys(_arg[0])
            query[_arg[0]] = _arg[1]
        except IndexError:
            logging.error(
                'Parsing failed, must use key{}value format'.format(delimiter))
            exit(1)
        except QueryParseError:
            logger.error('Failed: unknown parameter [{}]'.format(_arg[0]))
            exit(1)

    return query
