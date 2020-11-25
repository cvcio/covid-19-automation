#!/usr/bin/env python

"""
Import Libraries
"""

import sys
import logging
import argparse
import time
import pymongo

from pymongo import MongoClient

from conf.constants import ALLOWED_SOURCES

from strategies.jhu import JHUStrategy
from strategies.wom import WOMStrategy
from strategies.imedd import IMEDDStrategy

"""
Logging
"""

log_format = "%(asctime)s - [COVID19-AUTOMATION] [%(levelname)s] %(message)s"
formatter = logging.Formatter(log_format)

"""
Methods
"""


def downloadDocument(doc, output):
    logging.debug("Downloading document {} to {}".format(doc, output))
    pass


def formatData(data):
    logging.debug("Formating data")
    pass


def migrate(data):
    logging.debug("Migrating data")
    pass


def geo_loc(doc):
    lat = float(doc.pop("lat", 0.0))
    long = float(doc.pop("long", 0.0))
    if lat != 0.0 and long != 0.0:
        doc["loc"] = {"type": "Point", "coordinates": [long, lat]}


def clean_docs(df):
    docs = []
    for doc in df:
        geo_loc(doc)
        docs.append(doc)
    return docs


def getStrategy(source, *options):
    logging.debug("Choosing source strategy {}".format(source.upper()))

    # switch case dictionary
    switch = {
        "jhu": JHUStrategy,
        "worldometer": WOMStrategy,
        "imedd": IMEDDStrategy,
        "who": None,
        "eody": None,
        "sch": None,
    }

    # get case
    strategy = switch.get(source, lambda: "Invalid source")

    if strategy == None:
        logging.warning("Sorry, {} strategy not implemented yet".format(source.upper()))
        return None

    try:  # try to extract data
        s = strategy(source, options)
        return s.get()
    except Warning as w:  # warn and pass on warning
        logging.warning(str(w))
        return None


def get_mongodb_client(uri):
    if not uri:
        logging.warning("MongoDB URI is missing, can't connect")
        return None
    return MongoClient(uri)


def create_indexes(client, db, collection):
    coll = client.get_database(db).get_collection(collection)
    coll.create_index("date")
    if collection == "global":
        coll.create_index([("uid", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], sparse=True)
        coll.create_index([("iso3", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], sparse=True)
        coll.create_index([("country", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], sparse=True)
    else:
        coll.create_index([("uid", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], sparse=True)
        coll.create_index([("region", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], sparse=True)
        coll.create_index([("state", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], sparse=True)

"""
Initialize Covid-19 Automation Script
"""


def cli():
    start = time.time()

    parser = argparse.ArgumentParser()
    
    # cli accepted arguments
    parser.add_argument(
        "--log-level", dest="loglevel", help="Set logging level", default="INFO"
    )
    parser.add_argument(
        "--output", dest="output", help="Set output path", default="tmp/"
    )
    parser.add_argument(
        "--tmp",
        dest="tmp",
        help="Set temporary files path",
        default="tmp/",
    )
    parser.add_argument(
        "--source",
        dest="source",
        help="Set source (all|jhu|who|worldometer|imedd)",
        default="all",
    )
    parser.add_argument(
        "--clone",
        dest="clone",
        help="Clone dependencies",
        type=bool,
        default=False,
    )
    
    parser.add_argument(
        "--drop",
        dest="drop",
        help="Drop database",
        type=bool,
        default=False,
    )
    parser.add_argument(
        "--mongo",
        dest="mongo",
        help="MongoDB client URI",
        default="mongodb://localhost:27017/",
    )
    parser.add_argument(
        "--db",
        dest="db",
        help="Database name",
        default="covid19",
    )

    # parse cli arguments
    args = parser.parse_args()
    
    # set logging level
    logging.basicConfig(
        level=args.loglevel.upper(), format=log_format, datefmt="%Y-%m-%d %T%z"
    )
    # create the mongodb client
    mongo_client = get_mongodb_client("{}{}?retryWrites=true&w=majority".format(args.mongo, args.db))
    args.mongo_client = mongo_client
    
    # check if source strategy exists or is default
    if args.source not in ALLOWED_SOURCES and not args.source == "all":
        raise Exception('Sorry, source "{}" not allowed'.format(args.source))
    
    sources = ALLOWED_SOURCES if args.source == "all" else [args.source]
    strategies = []
    for source in sources:
        strategy = getStrategy(source, vars(args))
        strategies.append(strategy)
    logging.debug(
        "All documents have been generated in {}s".format(
            round(time.time() - start, 2),
        )
    )
    # save data on mongodb
    for strategy in strategies:
        if strategy is not None:
            strategy.migrate()
            if strategy.name == "imedd":
                strategy.enrich_global()
    
    # create mongodb indexes 
    if args.drop:
        create_indexes(mongo_client, args.db, "global")
        create_indexes(mongo_client, args.db, "greece")


if __name__ == "__main__":
    try:
        cli()
    except Exception as e:  # hold on exception
        logging.error(str(e))
    # exit on CTRL-D
    except KeyboardInterrupt:
        sys.exit("Exiting Covid-19 Automation Script")
