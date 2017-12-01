"""Set of command line utils to manipulate and search MPCORB file data"""

import gzip
import logging
import os
import pprint
import urllib.request
from json.decoder import JSONDecodeError

import argh
import pymongo
from bson import json_util

import config
from utils import convert, parse_query, search_in_dat

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger('streampc')

conf = config.get()

client = pymongo.MongoClient(host=conf['mongo_host'],
                             port=conf['mongo_port'],
                             connect=False)


@argh.arg('-q', nargs='+',
          help='Query arguments to search, example: [-q Aphelion_dist:3.1948214 Num_opps:38].')
@argh.arg('-db', default=conf['db_name'],
          help='Name of the DB, where MPCORB data is stored.')
@argh.arg('-col', default=conf['col_name'],
          help='Name of the collection in DB, where MPCORB is stored.')
@argh.arg('-dat', default=conf['dat_file'],
          help='Name of DAT file to perform search.')
@argh.arg('--pretty', help='Result will be formatted if argument passed.')
@argh.arg('--file', help='Indicates that search must be performed in file')
@argh.arg('--first', help='Indicates that search must be performed in file')
def find(pretty=False, file=False, first=False, **kwargs):
    """
    Function that searches for an objects inside DB.
    Using parsed query, prepared from command line params.
    Query is parsed and values are converted into proper types.
    CLI args:
        -q: determines that values will be recognized as query params.
            Uses following param convention: {key}:{value} for proper use.
        -db: DB name for search
        -col: collection name for search
        -dat: path to the DAT file
        --file: indicates that search must be performed in DAT file.
        --first: Indicates that only first occurance should be returned.
        --pretty: Indicates that result should be "pretty-formatted".

    Example:
        `$ python cli.py find -q Aphelion_dist:3.1948214 Num_opps:38 --pretty`
        `$ python cli.py find -q Num_opps:38 -db backup_mpc -col mpc_data_2`
        `$ python cli.py find -q Principal_desig:'2015 RV82' --file --pretty`
        `$ python cli.py find -q Name:Ceres --file --pretty --first`

    :param pretty: if True, result will be "pretty-formatted".
    :param file: if True, that search will be performed in file instead of DB.
    :param first: if True, only first occurrence from file will be returned.
    :param kwargs: keyword arguments from CLI.
    :return: search result
    """
    db_name = kwargs.get('db')
    col = kwargs.get('col')
    dat = kwargs.get('dat')
    if not kwargs.get('q'):
        logger.error(
            'Query cannot be empty and should be passed via [-q] parameter.')
        exit(1)
    query = parse_query(kwargs['q'])
    query = convert(query)
    if file:
        logger.info('Performing search in file [{}]'.format(dat))
        result = search_in_dat(dat, query, first)
    else:
        result = client[db_name][col].find(query)
        result = [rec for rec in result]
    if not len(result):
        logger.info('Objects not found. Try more precise query.')

    result = {'data': result,
              'count': len(result)}
    if pretty:
        return pprint.pformat(result)

    return result


@argh.arg('-url', default=conf['mpcorb_url'],
          help='URL for downloading MPCORB file')
@argh.arg('-path', default=os.curdir,
          help='Path, where file will be downloaded.')
@argh.arg('-name', default=conf['mpcorb_file'],
          help='Custom name for file that will be downloaded.')
@argh.arg('--extract', help='Extract downloaded file if it is archive.')
def get(extract=False, **kwargs):
    """
    Function for downloading MPC data files.
    Has ability to decompress expected .gz files.
    Used values from `config` as defaults.
    CLI args:
        -url: URI for file download
        -path: path to folder for download
        -name: name for the downloaded file
        -extracts: indicates need to extract data from archive.
    Downloaded file will be saved under `{path}/{filename}`
    Example:
        `$ python cli.py get --extract`
        `$ python cli.py get -path /tmp -name mytar.gz --extract`
        `$ python cli.py get -url http://myfile/data/data.json -path /tmp`

    :param extract: if passed, downloaded .gz will be decompressed.
    :param kwargs: keyword arguments from CLI
    """
    filename = kwargs.get('name')
    if not filename.endswith(('.gz', '.zip')) and extract:
        logger.warning(
            'Cannot determine type of compression, extraction can fail.')
    path = kwargs.get('path')
    if not os.path.exists(path):
        logger.error(
            "Failed to start download: no such directory: [{}]".format(path))
        exit(1)
    url = kwargs.get('url')
    if not os.access(path, os.W_OK):
        logger.error(
            'Insufficient permission to write into folder [{}]'.format(path))
        exit(1)
    path = '{}/{}'.format(path, filename)
    logger.info('Downloading from {}'.format(url))
    req, _ = urllib.request.urlretrieve(url, path)
    logger.info('Downloaded [{}], size: {}mb'.
                format(req, os.path.getsize(path) >> 20))
    if extract:
        path = path.replace('.gz', '')
        if not path.endswith('.json'):
            path = '{}.json'.format(path)
        target = open(path, 'wb')
        logger.info('Decompressing file...')
        with gzip.open(req) as gz:
            target.write(gz.read())
            target.close()
            logger.info('Extracted file [{}], size: {}mb'.
                        format(path, os.path.getsize(path) >> 20))


@argh.arg('-db', default=conf['db_name'], help='Name of the DB for updating.')
@argh.arg('-col', default=conf['col_name'],
          help='Name of the collection in DB for inserting.')
@argh.arg('-path', help='Path to the file that will be used for DB update.')
@argh.arg('--update', help='Indicates that updating existing records must be '
                           'performed instead of re-inserting full DB.')
def insert(update=False, **kwargs):
    """
    Function for inserting data to database from JSON file.
    Drops existing collection records and replace it with new records.
    Used values from `config` by default.
    CLI args:
        -db: name of the database for data population
        -col name of the collection for data population
        -path: path to the data file
    Example:
        `$ python cli.py insert -path mpcorb_extended.json`
        `$ python cli.py insert -path mpcorb.json -col mpc_backup -db mpc2`
        `$ python cli.py insert -path mpcorb.json -col mpc_backup -db mpc2 --update`

    :param kwargs: keyword arguments from CLI.
    :param update: indicates that existing records will be updated.
    """
    db_name = kwargs.get('db')
    col = kwargs.get('col')
    json_file = kwargs.get('path')

    if not json_file:
        logger.error('No JSON file is provided.')
        exit(1)
    if not os.path.exists(json_file):
        logger.error('File [{}] not exists.'.format(json_file))
        exit(1)

    with open(json_file) as f:
        try:
            logger.info('Reading JSON file...')
            data = json_util.loads(f.read())
            logger.info('DB [{}] will be used.'.format(db_name))
            if col in client[db_name].collection_names() and not update:
                logger.warning(
                    'Existing collection [{}] will be dropped before update.'.format(
                        col))
            collection = client[db_name][col]
            if not data:
                raise JSONDecodeError('', '', 1)
            if update:
                logger.info(
                    'Updating MPCORB data records into database, wait...')
                for record in data:
                    collection.update_one(
                        {'Principal_desig': record['Principal_desig']},
                        {'$set': record}, upsert=False)
                count = len(data)
            else:
                collection.drop()
                logger.info(
                    'Inserting MPCORB data records into database, wait...')
                for record in data:
                    collection.insert_one(record)
                count = collection.count()
            logger.info(
                'Processed {} records into [{}.{}]'.format(count,
                                                           db_name, col))
        except (JSONDecodeError, UnicodeDecodeError):
            logger.error(
                'Failed to read JSON file: data is empty or corrupted.')


if __name__ == '__main__':
    parser = argh.ArghParser()
    parser.add_commands([find, get, insert])
    parser.dispatch()

