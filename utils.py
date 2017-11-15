"""Set of helper utils, for example converters, validators, parsers etc."""

# map with the required type for each field in query
type_map = {
    "Orbit_type": str,
    "a": float,
    "Semilatus_rectum": float,
    "Name": str,
    "Hex_flags": str,
    "Last_obs": str,
    "Synodic_period": float,
    "Epoch": float,
    "Tp": float,
    "e": float,
    "Computer": str,
    "M": float,
    "Aphelion_dist": float,
    "G": float,
    "Peri": float,
    "Arc_years": str,
    "U": str,
    "Number": str,
    "H": float,
    "Other_desigs": list,
    "rms": float,
    "Perturbers": str,
    "Perihelion_dist": float,
    "Num_opps": int,
    "Ref": str,
    "Principal_desig": str,
    "Orbital_period": float,
    "Num_obs": int,
    "Perturbers_2": str,
    "n": float,
    "Node": float,
    "i": float
}


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
            mapped = {k: type_map[k](data[k])}
            converted.update(mapped)
    return converted


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
        _arg = _arg.split(delimiter)
        query[_arg[0]] = _arg[1]
    return query
