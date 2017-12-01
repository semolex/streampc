import os

_url = 'http://minorplanetcenter.net/Extended_Files/mpcorb_extended.json.gz'


def get():
    return {
        'db_name': os.getenv('MPC_DB_NAME', 'mpcorb'),
        'col_name': os.getenv('MPC_COLLECTION', 'mpc_data'),
        'dat_file': os.getenv('MPCORB_DAT', 'mpcorb_extended.dat'),
        'mpcorb_url': os.getenv('MPCORB_URL', _url),
        'mpcorb_file': os.getenv('MPCORB_FILE', 'mpcorb_extended.json.gz'),
        'mongo_host': os.getenv('MONGODB_HOST', 'localhost'),
        'mongo_port': int(os.getenv('MONGODB_PORT', 27017))
    }
