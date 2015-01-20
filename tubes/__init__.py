import sqlalchemy as sqla
import shlex
import json
import csv

valid_types = ["str", "int", "real", "time", "blob"]

def lex_schema(schema):
    s = shlex.shlex(schema)
    fields = {'names': [], 'types': []}
    while True:
        fieldtype = s.get_token()
        name = s.get_token()
        comma = s.get_token()
        if fieldtype == '' and comma == '' and name == '':
            return fields

        fields['names'].append(name)
        if fieldtype.lower() in valid_types:
            fields['types'].append(fieldtype.lower())
        else:
            raise Exception("Invalid Schema")

def fieldTypeToSqla(ft, name):
    if ft == 'int':
        return sqla.Column(name, sqla.Integer)
    elif ft == 'real':
        return sqla.Column(name, sqla.Float)
    elif ft == 'str':
        return sqla.Column(name, sqla.Text)
    elif ft == 'time':
        return sqla.Column(name, sqla.DateTime)
    elif ft == 'blob':
        return sqla.Column(name, sqla.types.BLOB)
    else:
        return None

"""
 * data is in 'list-of-lists' format which each minor list as a record and each
   element therof corresponding with the element sharing its position in the
   schema list
   i.e. [[el1, el2, el3], [el1, el2, el3]]
 * valid schema_formats are:
    * tubes - {'names': ["n1", "n2", ..., "nm"], "types": ["t1", "t2", ...,"tm"]}
    * signature - "t1 n1, t2 n2, ... tm nm"
"""
class tubesData(object):
    def __init__(self, schema, schema_format="signature", data=None):
        self.interpretSchema(schema, schema_format)
        self.data=data

    def interpretSchema(self, schema, schema_format):
        if schema_format == "signature":
            scheme = lex_schema(schema)
            self.names = scheme['names']
            self.types = scheme['types']
        elif schema_format == "tubes":
            self.names = schema['names']
            self.types = schema['types']
        else:
            raise Exception("Invalid schema format %s" % schema_format)

    def records(self):
        for r in self.data:
            yield dict(zip(self.names, r))

    def recordlist(self):
        return [r for r in self.records()]

    def dumpd(self):
        return {'schema': {'names': self.names,
                           'types': self.types},
                'records': self.data}

    def dumps(self):
        return json.dumps(self.dumpd())

    def sqlaTable(self, sqlaMetadata, tableName):
        members = [sqla.Column('id', sqla.Integer, primary_key=True)]
        for name, ft in zip(self.names, self.types):
            members.append(fieldTypeToSqla(ft, name))
        tabledef = sqla.Table(tableName, sqlaMetadata, *members)
        return tabledef

class csvTube(tubesData):
    def __init__(self, path):
        with open(path, 'r') as fd:
            reader = csv.reader(fd)
            self.names = None
            data = []
            for r in reader:
                if not self.names:
                    self.names = r
                    self.types = len(self.names)*['str']
                else:
                    data.append(r)
            self.data = data

#data is in 'list-of-dicts' format with each dict possesing keys for each
#element of the schema
class tubesDicts(tubesData):
    def __init__(self, schema, schema_format, dicts):
        self.interpretSchema(schema, schema_format)

        l = []
        for d in dicts:
            e = []
            for n in self.names:
                e.append(d[n])
            l.append(e)
        self.data = l

class tubesDirect(tubesData):
    def __init__(self, names, types, data):
        self.names = names
        self.types = types
        self.data = data

def loadPacket(pkt):
    return tubesDirect(pkt['schema']['names'],
                       pkt['schema']['types'],
                       pkt['records'])
