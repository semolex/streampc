"""Uses as set of base functionality for MPC data"""

import logging

import pymongo

import config

logger = logging.getLogger(__name__)


class Engine:
    """
    Base class for manipulating MPC data.
    Performs searching for objects and can instantiate DB object for usage.
    """

    def find(self, query, db_name, collection):
        """
        Method for searching in MongoDB by mongo-compatible query.

        :param query: query for searching.
        :param db_name: name of the database to use.
        :param collection: name of the collection to use.
        :return: list of found records
        """
        result = self.db(db_name, collection).find(query)
        if not result.count():
            logger.info('Objects not found. Try more precise query.')

        return {'data': [rec for rec in result],
                'count': result.count()}

    def db(self, db_name, collection):
        conf = config.get()
        """
        Method that return database `collection` object for usage.

        :param db_name: name of the database to use.
        :param collection: name of the collection to use.
        :return: `pymongo.collection.Collection` object
        """
        client = pymongo.MongoClient(host=conf['mongo_host'],
                                     port=conf['mongo_port'],
                                     connect=False)
        return client[db_name][collection]
