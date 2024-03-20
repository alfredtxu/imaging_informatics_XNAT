import hashlib
import numpy as np
import pandas as pd
import os
import argparse
from pathlib import Path
import argparse
from pathlib import Path

import xnat
# from connect_to_db import *
from dbconnection.connect_to_db import *

# Password : Parkgasse18

def create_hashtable():
    conn = None

    try:
        conn = connect()

        # create a cursor
        cur = conn.cursor()

        # create table with same headers as csv file
        cur.execute("""CREATE TABLE IF NOT EXISTS 
            xnat_hashoriginalmap(
            name  varchar(255),
            patientId  varchar(255),
            dateofbirth  varchar(255),
            patientIdentifier text, 
            dateofbirthIdentifier text, UNIQUE (patientId, dateofbirth));
           """)
        cur.execute(""" INSERT INTO xnat_hashoriginalmap (patientId, name, dateofbirth) SELECT upper(xi.dcmpatientid), 
        xi.dcmpatientname, xi.dcmpatientbirthdate from xnat_imagesessiondata xi WHERE
        xi.dcmpatientid is not null and  xi.dcmpatientname is not null and xi.dcmpatientbirthdate is not null 
        GROUP BY  xi.dcmpatientid, xi.dcmpatientname, xi.dcmpatientbirthdate ON CONFLICT DO NOTHING; """)

        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    print('Hash table created successfully!!')


def print_hash_of_mrn_dob(mrn_dob):
    password = 'Parkgasse18'
    for i in mrn_dob:
        m = hashlib.shake_256()     # recreate object otherwise wrong hash gets generated
        i = password + i
        m.update(i)
        h = m.hexdigest(15)
        print(f" Hash :{h}")


def add_hashcolumns_imagesessiondata():
    conn = None
    cur = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(""" ALTER TABLE public.xnat_imagesessiondata ADD COLUMN IF NOT EXISTS
        dateofbirthidentifier varchar(255); """)
        cur.execute(""" ALTER TABLE public.xnat_imagesessiondata ADD COLUMN IF NOT EXISTS 
        patientidentifier varchar(255); """)
        conn.commit()

    except Exception as e:
        print(e)
        conn.rollback()

    cur.close()
    conn.close()


def update_xnat_imagesessiondata():
    conn = None
    cur = None
    try:
        conn = connect()
        cur = conn.cursor()
        query = """ UPDATE public.xnat_imagesessiondata
                               SET patientidentifier = subq1.patientidentifier, 
                               dateofbirthidentifier = subq1.dateofbirthidentifier
                               FROM (SELECT patientid, dateofbirth, patientidentifier, dateofbirthidentifier
                               FROM public.xnat_hashoriginalmap) AS subq1
                               WHERE public.xnat_imagesessiondata.dcmpatientid = subq1.patientid AND 
                               TO_CHAR(public.xnat_imagesessiondata.dcmpatientbirthdate, 'YYYY-MM-DD') = subq1.dateofbirth AND
                               public.xnat_imagesessiondata.dcmpatientid IS NOT NULL AND
                               public.xnat_imagesessiondata.dcmpatientbirthdate IS NOT NULL;"""
        cur.execute(query)
        updated_rows = cur.rowcount
        if updated_rows >= 1:
            conn.commit()

    except Exception as e:
        print(e)
        conn.rollback()
    cur.close()
    conn.close()


def hash():
    conn = None
    cur = None
    password = 'Parkgasse18'
    try:
        conn = connect()
        cur = conn.cursor()
        query = "Select * from xnat_hashoriginalmap;"
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=["name", "patientid", "dateofbirth", "patientidentifier",
                                                   "dateofbirthidentifier"])
        updatequery = """ UPDATE xnat_hashoriginalmap
                               SET patientidentifier = %s, 
                               dateofbirthidentifier = %s
                               WHERE name = %s AND 
                               patientid = %s AND 
                               dateofbirth = %s"""

        for index, row in df.iterrows():
            mmrn = hashlib.shake_256()
            mrn = password + row['patientid']
            mmrn.update(mrn.encode('utf-8'))
            h_mrn = mmrn.hexdigest(15)
            dob = password+row['dateofbirth']
            mdob = hashlib.shake_256()
            mdob.update(dob.encode('utf-8'))
            h_dob = mdob.hexdigest(15)
            cur.execute(updatequery, (h_mrn, h_dob, row['name'], row['patientid'], row['dateofbirth']))
            updated_rows = cur.rowcount
            if updated_rows >= 1:
                conn.commit()

    except Exception as e:
        print(e)
        conn.rollback()
    cur.close()
    conn.close()


def create_trigger():
    conn = None
    cur = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""
        CREATE OR REPLACE FUNCTION copy_pid_dob_tohash() RETURNS TRIGGER AS
        $BODY$
        BEGIN
        insert into xnat_hashoriginalmap (patientId, name, dateofbirth) select upper(xi.dcmpatientid),
        xi.dcmpatientname, xi.dcmpatientbirthdate from xnat_imagesessiondata xi
        where  xi.dcmpatientid is not null and  xi.dcmpatientname is not null and xi.dcmpatientbirthdate is not null 
        group by  xi.dcmpatientid, xi.dcmpatientname, xi.dcmpatientbirthdate on conflict do nothing ;
        
                   RETURN new;
        END;
        $BODY$
        language plpgsql;""")
        cur.execute("""CREATE TRIGGER trig_copy_pid_dob_tohash
         AFTER INSERT ON xnat_imagesessiondata
         FOR EACH ROW
         EXECUTE PROCEDURE copy_pid_dob_tohash();""")

    except Exception as e:
        print(e)
        conn.rollback()

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='hash sensitive informative')
    parser.add_argument('--hashmappingtable', type=bool, default=True,
                        help='create hashes for patientid and birthdate in xnat_hashoriginalmap: default False')
    parser.add_argument('--hashimagesessiondata', type=bool, default=True,
                        help='replace patientid and birthdate in xnat_imagesessiondata with hashes: default False')
    parser.add_argument('--hashofmrndob', nargs=3, help="Please pass True/False, mrn and dob (YYYY-MM-DD)"
                                                        "Input should look like True 12345678 1920-10-21")

    args = parser.parse_args()
    create_hashtable()
    create_trigger()
    add_hashcolumns_imagesessiondata()
    if args.hashmappingtable:
        hash()
    if args.hashimagesessiondata:
        update_xnat_imagesessiondata()
    if args.hashofmrndob[0]:
        print_hash_of_mrn_dob(args.hashofmrndob[1:])
