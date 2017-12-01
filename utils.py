"""Set of helper utils, for example converters, validators, parsers etc."""

import decimal
import logging
import re
from datetime import datetime

import julian

from datamaps import key_map, type_map, year_map, month_map, day_map

logger = logging.getLogger(__name__)

NUM_PATTERN = re.compile(r"\(\d+\)")
NAME_PATTERN = re.compile(r"\s\w+")
DIG_PATTERN = re.compile(r'\d+')


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


def match_in_dat(query, target):
    """
    Check if given query is matched in target object data.

    :param query: query to match.
    :param target: object to match query.
    :return: bool (True if matched, else False)
    """
    intersection = set(query.items()).intersection(set(target.items()))
    intersection = dict(intersection)
    return intersection == query


def _filter_desig_line(line):
    """
    Filters given line, splits by delimiter and removes empty values.

    :param line: line to filter.
    :return: filtered list of values.
    """
    filtered = filter(None, [x.strip() for x in line.split('  ')])
    return filtered


def _extract_by_pattern(pattern, line):
    """
    Simply matches pattern and search by regex in given line.

    :param pattern: regex pattern to match.
    :param line: line for matching.
    :return: matched values.
    """
    try:
        res = ''.join(re.findall(pattern, line)).strip()

    except AttributeError:
        res = ''
    return res


def _extract_epoch(value):
    """
    Extracts date object from string value.
    Thet it is converted into Julian Date.

    :param value: value that hold epoch data.
    :return: float with parsed epoch.
    """
    try:
        year = year_map[value[0]] + value[1:3]
        month = value[3]
        if month_map.get(value[3]):
            month = month_map[value[3]]
        day = value[4::]
        if day_map.get(value[4::]):
            day = day_map[value[4::]]
        epoch = '{}/{}/{}'.format(year, month, day)
        epoch = julian.to_jd(datetime.strptime(epoch, '%Y/%m/%d'))
    except IndexError:
        epoch = value

    return epoch


def _extract_desig(line):
    """
    Extracts packed designation values via different criteria.

    :param line: line with packed designations.
    :return: unpacked designation.
    """

    desig = None
    if line.isdigit():
        return line
    elif line.startswith(('PL', 'T')) and len(line) == 7:
        desig = '{}-{} {}'.format(line[0], line[1], line[2::])

    elif len(line) == 7:
            year = year_map[line[0]] + line[1:3]
            half_month = line[3] + line[6:]
            cycle = line[4:6]
            cycle_s = day_map.get(cycle[0])

            cycle_e = day_map.get(cycle[1])
            if not cycle_s:
                cycle_s = cycle[0]
            if not cycle_e:
                cycle_e = cycle[1]
            cycle = int(cycle_s + cycle_e)

            if cycle == 0:
                cycle = ''
            desig = '{} {}{}'.format(year, half_month, cycle)

    elif len(line) == 5:
        num = day_map.get(line[0])
        if not num:
            num = line[0]
        desig = num + line[1::]

    return desig


def _extract_arc_data(line):
    """
    Extracts Arc distance\Arc years data from given line.

    :param line: line for extraction.
    :return: extracted arc data.
    """
    if 'days' in line:
        key = 'Arc_length'
        value = re.search(DIG_PATTERN, line).group()
    else:
        key = 'Arc_years'
        value = line
    return key, value


def peri(a, e):
    """
    Simple calculation of perihelion distance.

    :param a: Semi major axis value.
    :param e: Orbital eccentricity
    :return: Perihelion distance float.
    """
    return float(decimal.Decimal(a) * decimal.Decimal((1 - e)))


def aph(a, e):
    """
    Simple calculation of aphelion distance.

    :param a: Semi major axis value.
    :param e: Orbital eccentricity
    :return: Aphelion distance float.
    """
    return float(decimal.Decimal(a) * decimal.Decimal((1 + e)))


def _parse_dat_file(line):
    """
    Parses line from DAT file and matches required keys.
    It attempts to be close to JSON data from MPC in case there is no ability to use it.

    :param line: line to match.
    :return: parsed dict
    """
    arc_key, arc_value = _extract_arc_data('{}{}{}'.format(line[127:131],
                                                           line[131:132],
                                                           line[132:136]))
    parsed = {
        'Principal_desig': _extract_desig(line[0:7].strip()),
        'H': line[8:13].strip(),
        'G': line[14:19].strip(),
        'Epoch': _extract_epoch(line[20:25].strip()),
        'M': line[26:35].strip(),
        'Peri': line[36:46].strip(),
        'Node': line[47:57].strip(),
        'i': line[58:68].strip(),
        'e': line[69:79].strip(),
        'n': line[80:91].strip(),
        'a': line[92:103].strip(),
        'U': line[104:106].strip(),
        'Ref': line[107:116].strip(),
        'Num_obs': line[117:122].strip(),
        'Num_opps': line[123:126].strip(),
        arc_key: arc_value,
        'rms': line[137:141].strip(),
        'Perturbers': line[142:145].strip(),
        'Perturbers_2': line[146:149].strip(),
        'Computer': line[150:160].strip(),
        'Hex_flags': line[161:165].strip(),
        'Name': _extract_by_pattern(NAME_PATTERN, line[166:194].strip()),
        'Number': _extract_by_pattern(NUM_PATTERN, line[166:194].strip()),
        'Last_obs': '{}-{}-{}'.format(line[194:198], line[198:200], line[200:202]),
        'Tp': line[203:216].strip(),
        'Other_desigs': list(_filter_desig_line(line[217::]))
    }
    parsed = {k: v for k, v in parsed.items() if v}
    parsed = convert(parsed)
    decimal.getcontext().prec = 8
    if parsed.get('a') and parsed.get('e'):
        parsed['Perihelion_dist'] = peri(parsed['a'], parsed['e'])
        parsed['Aphelion_dist'] = aph(parsed['a'], parsed['e'])
    return parsed


def search_in_dat(filename, query, first=False):
    """
    Performs search for data in DAT file by given query.

    :param filename: name of the DAT file to open.
    :param query: query to match.
    :param first: if True, only the first occurrence will be returned.
    :return: dict with matched items.
    """
    result = []
    with open(filename, 'rb') as dat:
        for _ in range(43):
            next(dat)
        for line in dat:
            line = str(line, 'utf-8')

            parsed = _parse_dat_file(line)
            if 'Other_desigs' in parsed:
                parsed['Other_desigs'] = tuple(parsed['Other_desigs'])
            if match_in_dat(query, parsed):
                result.append(parsed)
                if first:
                    break
            else:
                pass
    return result
