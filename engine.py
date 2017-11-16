"""Uses as set of base functionality for MPC data"""

import pymongo

from config import DEFAULT_DBNAME, DEFAULT_COLLECTION


class Engine:
    """
    Base class for manipulating MPC data.
    Performs searching for objects and can instantiate DB object for usage.
    """
    def find(self, query, db_name=DEFAULT_DBNAME,
             collection=DEFAULT_COLLECTION):
        """
        Method for searching in MongoDB by mongo-compatible query.

        :param query: query for searching.
        :param db_name: name of the database to use.
        :param collection: name of the collection to use.
        :return: list of found records
        """
        result = self.db(db_name, collection).find(query)
        return [rec for rec in result]

    def db(self, name, collection):
        """
        Method that return database `collection` object for usage.

        :param name: name of the database to use.
        :param collection: name of the collection to use.
        :return: `pymongo.collection.Collection` object
        """
        client = pymongo.MongoClient(connect=False)
        return client[name][collection]








