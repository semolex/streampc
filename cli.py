"""Set of command line utils to manipulate and search MPCORB file data"""

import gzip
import logging
import os
import pprint
import urllib.request

import argh
import pymongo
from bson import json_util

import config
from utils import convert, parse_query

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger()


conf = config.get()

client = pymongo.MongoClient(host=conf['mongo_host'],
                             port=conf['mongo_port'],
                             connect=False)


@argh.arg('-q', nargs='+')
@argh.arg('-db', default=conf['db_name'])
@argh.arg('-col', default=conf['col_name'])
@argh.arg('--pretty')
def find(pretty=False, **kwargs):
    """
    Function that searches for an objects inside DB.
    Using parsed query, prepared from command line params.
    Query is parsed and values are converted into proper types.
    CLI args:
        -p: determines that values will be recognized as query params.
            Uses following param convention: {key}:{value} for proper use.
        -db: DB name for search
        -col: collection name for search

    Example:
        `$ python cli.py find -q Aphelion_dist:3.1948214 Num_opps:38 --pretty`
        `$ python cli.py find -q Num_opps:38 -db backup_mpc -col mpc_data_2`

    :param pretty: if True, result will be "pretty-formatted" and ordered.
    :param kwargs: keyword arguments from CLI.
    :return: search result
    """
    db_name = kwargs.get('db')
    col = kwargs.get('col')
    query = parse_query(kwargs['q'])
    query = convert(query)
    result = client[db_name][col].find(query)
    if not result.count():
        logger.info('Objects not found. Try more precise query.')

    result = {'data': [rec for rec in result],
              'count': result.count()}
    if pretty:
        return pprint.pformat(result)
    else:
        return result


@argh.arg('-url', default=conf['mpcorb_url'])
@argh.arg('-path', default=os.curdir)
@argh.arg('-name', default=conf['mpcorb_file'])
@argh.arg('--extract')
def get(extract=False, **kwargs):
    """
    Function for downloading MPC data files.
    Has ability to decompress expected .gz files.
    Used values from `config` as defaults.
    CLI args:
        -url: URI for file download
        -path: path to folder for download
        -name: name for the downloaded file
    Downloaded file will be saved under `{path}/{filename}`
    Example:
        `$ python cli.py get --extract`
        `$ python cli.py get -path /tmp -name mytar.gz --extract`
        `$ python cli.py get -url http://myfile/data/data.json -path /tmp`

    :param extract: if passed, downloaded .gz will be decompressed.
    :param kwargs: keyword arguments from CLI
    """
    filename = kwargs.get('name')
    path = kwargs.get('path')
    url = kwargs.get('url')
    path = '{}/{}'.format(path, filename)
    logger.info('Started download from MPC')
    req, _ = urllib.request.urlretrieve(url, path)
    logging.info('Downloaded [{}], size: {}mb'.
                 format(req, os.path.getsize(path) >> 20))
    if extract:
        path = path.replace('.gz', '')
        if not path.endswith('.json'):
            path = '{}.json'.format(path)
        target = open(path, 'wb')
        logging.info('Decompressing file...')
        with gzip.open(req) as gz:
            target.write(gz.read())
            target.close()
            logging.info('Extracted file [{}], size: {}mb'.
                         format(path, os.path.getsize(path) >> 20))


@argh.arg('-db', default=conf['db_name'])
@argh.arg('-col', default=conf['col_name'])
@argh.arg('-path')
def update(**kwargs):
    """
    Function for updating database from JSON file.
    Drops existing collection records and replace it with new records.
    Used values from `config` by default.
    CLI args:
        -db: name of the database for data population
        -col name of the collection for data population
        -path: path to the data file
    Example:
        `$ python cli.py update -path mpcorb_extended.json`
        `$ python cli.py update -path mpcorb.json -col mpc_backup -db mpc2`

    :param kwargs: keyword arguments from CLI.
    """
    db_name = kwargs.get('db')
    col = kwargs.get('col')
    json_file = kwargs.get('path')
    logging.warning('Old collection will be dropped before update (if exists)')
    collection = client[db_name][col]
    collection.drop()
    logging.info('Inserting MPCORB data records into database, wait...')
    with open(json_file) as f:
        data = json_util.loads(f.read())
    for record in data:
        collection.insert_one(record)
    logging.info('Inserted {} records into [{}.{}]'.format(collection.count(),
                                                           db_name, col))


if __name__ == '__main__':
    parser = argh.ArghParser()
    parser.add_commands([find, get, update])
    parser.dispatch()
