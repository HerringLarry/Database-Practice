from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData
import psycopg2
import sys
import csv

def create(db,fname):
    meta = MetaData(db)
    with open(fname, 'rb') as f_one:
        reader = csv.DictReader(f_one, delimiter = ',')
        header = reader.next()
        table = return_table(header,meta)
        with db.connect() as conn:
            table.create()
            for row in reader:
                table.insert().values(**row).execute()

def return_table(header,meta):
    print header
    table = Table('csv', meta,
           *(Column(name, String) for name in header.keys() if name))
    return table

def main():
    fname = "aids.csv"
    try:
        conn_string = "postgres://wnewman:pg123@localhost/aids"
        db = create_engine(conn_string)
        print "success"
    except:
        print "failure"
    
    create(db,fname)
if __name__ == "__main__":
    main()
