import os
import sys
import argparse

import pymongo

import tubes

def parseargs(argv):
    parser = argparse.ArgumentParser(prog='csvtosql',
             description="Convert a CSV file to an SQL table")
    parser.add_argument('-c', '--csv', required=True,
            help="path to input CSV file")
    parser.add_argument('-s', '--server', required=True,
            help="Mongodb server dns/ip")
    parser.add_argument('-p', '--port', required=True, type=int,
            help="Mongodb server port number")
    parser.add_argument('-d', '--db', required=True,
            help="Mongodb database name")
    parser.add_argument('-n', '--collection', required=True,
            help="Name of the destination collection")
    args = parser.parse_args(argv)
    return args

if __name__ == "__main__":
    args = parseargs(sys.argv[1:])
    mclient = pymongo.MongoClient(args.server, args.port)
    mdb = mclient[args.db]

    d = tubes.csvTube(args.csv)
    d.dumpMongo(mdb, args.collection)
