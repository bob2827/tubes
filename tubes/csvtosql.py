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
    parser.add_argument('-c', '--csv', required=True,
            help="path to input CSV file")
    parser.add_argument('-d', '--db', required=True,
            help="SQLAlchemy specifier for destination database")
    parser.add_argument('-n', '--tablename', required=True,
            help="Name of the destination table")
    args = parser.parse_args(argv)
    return args

if __name__ == "__main__":
    args = parseargs(sys.argv[1:])
    d = tubes.csvTube(args.csv)
    Base = declarative_base()
    tbl = d.sqlaTable(Base.metadata, args.tablename)

    engine = sqla.create_engine(args.db)
    Base.metadata.create_all(engine)
    con = engine.connect()
    con.execute(tbl.insert(), d.recordlist())
    con.close()
