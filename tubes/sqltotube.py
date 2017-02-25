import os
import sys
import argparse

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker

import tubes

def parseargs(argv):
    parser = argparse.ArgumentParser(prog='csvtosql',
             description="Convert a CSV file to an SQL table")
    #parser.add_argument('-c', '--csv', required=True,
    #        help="path to input CSV file")
    parser.add_argument('-d', '--db', required=True,
            help="SQLAlchemy specifier for destination database")
    parser.add_argument('-n', '--tablename', required=True,
            help="Name of the destination table")
    args = parser.parse_args(argv)
    return args

if __name__ == "__main__":
    args = parseargs(sys.argv[1:])

    Base = declarative_base()
    engine = sqla.create_engine(args.db)
    meta = sqla.MetaData()
    con = engine.connect()

    tbl = sqla.Table(args.tablename, meta, autoload=True, autoload_with=engine)
    d = tubes.sqlaTube(tbl, con)

    print d.recordlist()

    con.close()
