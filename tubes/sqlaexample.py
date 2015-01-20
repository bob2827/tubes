import tubes
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker

d = tubes.csvTube('somecsvfile.csv')
Base = declarative_base()
tbl = d.sqlaTable(Base.metadata, 'tblname')

engine = sqla.create_engine("sqlite:///foo.db")
Base.metadata.create_all(engine)
con = engine.connect()
con.execute(tbl.insert(), d.recordlist())
