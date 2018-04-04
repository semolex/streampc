import configparser

config = configparser.ConfigParser()


def get():
    """
    Reads configuration file items and return them a dict.

    :return: dict with parsed config items.
    """
    config.read('config.ini')

    conf = {'db_name': config['mongo']['DefaultDatabaseName'],
            'col_name': config['mongo']['DefaultCollection'],
            'dat_file': config['mpcorb']['DatFileName'],
            'mpcorb_url': config['mpcorb']['Url'],
            'mpcorb_file': config['mpcorb']['JsonFileName'],
            'mongo_host': config['mongo']['host'],
            'mongo_port': int(config['mongo']['port'])}
    return conf
