#!/usr/bin/env python
import argparse
from pathlib import Path

import xnat
# from connect_to_db import *
from dbconnection.connect_to_db import *

def drop_table(table_name):
    conn = None

    try:
        conn = connect()

        # create a cursor
        cur = conn.cursor()

        # create table with same headers as csv file
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        conn.commit()
        cur.close()
        print('Table dropped successfully!!')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def copy_data(table_name, sep=','):
    conn = None

    try:
        conn = connect()

        # create a cursor
        cur = conn.cursor()

        # In Static dimensions tables we check if any data exists, delete the whole data and re-insert
        cur.execute(f"SELECT count(*) FROM {table_name}")
        rows_found = cur.rowcount

        if rows_found:
            # execute the UPDATE  statement
            cur.execute(f"DELETE FROM {table_name}")
            # get the number of updated rows
            rows_deleted = cur.rowcount
            print(f"Rows deleted {rows_deleted}")
            conn.commit()

        # # Copy data from file
        with open(f'/mnt/wwn-0x5000c500cc87eb78/xnatpy/data/{table_name}.csv', 'r') as f:
            # Notice that we don't need the `csv` module.
            next(f)  # Skip the header row.
            cur.copy_from(f, table_name, sep=sep, null='')

        conn.commit()
        cur.close()
        print(f'Data inserted to {table_name} successfully!!')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def create_table():
    conn = None

    try:
        conn = connect()

        # create a cursor
        cur = conn.cursor()

        # create table with same headers as csv file
        cur.execute("""CREATE TABLE IF NOT EXISTS 
        onc_beacondata(
        index bigint NULL,
        mrn text NULL,
        scandate text NULL,
        treatmentstartdate text NULL,
        months_scan_to_treatmentstartdate double precision NULL,
        days_scan_to_treatmentstartdate double precision NULL,
        treatmentepisode text NULL,
        treatmentplan text NULL, 
        consultant text NULL,
        medication text NULL,
        chemodrug text NULL,
        medicationstartdate text NULL,
        medicationenddate text NULL, 
        statustreatmentepisode text NULL,
        statustreatmentplan text NULL,
        statuscycle text NULL,
        order_id text NULL,
        opt_id text NULL) ; 
        """)

        cur.execute("""CREATE
        TABLE IF NOT EXISTS 
        onc_bloodtest
        (
        index bigint NULL,
        mrn text NULL,
        scandate text NULL,
        orderdate text NULL,
        months_scan_to_group_and_screenorder double precision NULL,
        days_scan_to_group_and_screenorder double precision NULL,
        procedure_text text NULL,
        procedurecategory text NULL,
        componentname text NULL,
        labresult text NULL,
        labresultstatus text NULL
        );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS onc_carecast_episodedata(
        index bigint,
        mrn text,
        scandate date NULL,
        months_scan_to_episodestart double precision NULL,
        days_scan_to_episodestart double precision NULL,
        episode_start_date timestamp without time zone NULL,
        episode_end_date timestamp without time zone NULL,
        scanwithinepisode text NULL,
        hadchemotherapy text NULL,
        admissiontype text NULL,
        primarydiagnosis text NULL,
        tumoursite text NULL,
        primaryprocedure text NULL,
        consultant text NULL,
        subsplit text NULL,
        reportingunit text NULL,
        division text NULL,
        episode_id text NULL,
        hospitalproviderspellnumber text NULL);""")

        cur.execute("""CREATE TABLE IF NOT EXISTS onc_chemocaredata
        (
          index bigint NULL,
          mrn text NULL,
          scandate text NULL,
          months_scan_to_treatmentstartdate double precision NULL,
          days_scan_to_treatmentstartdate double precision NULL,
          treatmentplan text NULL,
          consultantcode text NULL,
          consultant text NULL,
          treatmentplanstartdate text NULL,
          medication text NULL,
          diagnosiscode text NULL,
          diagnosis text NULL,
          tumoursite text NULL
        );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS onc_epic_episodedata
        (
          index bigint NULL,
          mrn text NULL,
          scandate text NULL,
          months_scan_to_episodestart double precision NULL,
          days_scan_to_episodestart double precision NULL,
          episode_start_date text NULL,
          episode_end_date text NULL,
          scanwithinepisode text NULL,
          hadchemotherapy text NULL,
          admissiontype text NULL,
          primarydiagnosis text NULL,
          tumoursite text NULL,
          primaryprocedure text NULL,
          consultant text NULL,
          specialty text NULL,
          reportingunit text NULL,
          division text NULL,
          episode_id text NULL,
          hospitalproviderspellnumber text NULL
        );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS onc_patientlist
        (
          index bigint NULL,
          mrn text NULL,
          scandate text NULL,
          patientinourdatabase text NULL,
          date_of_death text NULL,
          months_scan_to_death double precision NULL,
          days_scan_to_death double precision NULL
        );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS onc_problemlist
        (
          index bigint NULL,
          mrn text NULL,
          scandate text NULL,
          noteddate text NULL,
          months_scan_to_problemlist_noteddate double precision NULL,
          days_scan_to_problemlist_noteddate double precision NULL,
          diagnosis text NULL,
          tumoursite text NULL,
          icd_code text NULL,
          problemstatus text NULL,
          problem_list_id integer NULL
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS onc_surgery_post_epic
        (
          index bigint NULL,
          mrn text NULL,
          scandate text NULL,
          operationdate text NULL,
          months_scan_to_surgery double precision NULL,
          days_scan_to_surgery double precision NULL,
          case_id text NULL,
          primaryprocedure text NULL,
          theatresite text NULL,
          theatre text NULL,
          operatingroom text NULL,
          schedulestatus text NULL,
          operationstatus text NULL,
          casestatus text NULL,
          operationpriority text NULL, 
          anaesthesiatype text NULL,
          "ASAScore" text NULL,
          service text NULL,
          primarysurgeon text NULL,
          primaryanaesthetist text NULL
        );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS onc_surgery_pre_epic
        (
          index bigint NULL,
          mrn text NULL,
          scandate text NULL,
          operationdate text NULL,
          months_scan_to_surgery double precision NULL, 
          days_scan_to_surgery double precision NULL,
          keyrequest text NULL,
          procedurename text NULL,
          theatresite text NULL,
          theatre text NULL,
          operatingroom text NULL,
          operationpriority text NULL,
          anaesthesiatype text NULL,
          "ASAScore" text NULL,
          service text NULL,
          primarysurgeon text NULL,
          anaesthetistname text NULL
        );""")

        conn.commit()
        cur.close()
        print('Tables created successfully!!')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    create_table()
    list_of_tables = [
    'onc_beacondata', 'onc_bloodtest', 'onc_carecast_episodedata',
                      'onc_chemocaredata',
                     'onc_epic_episodedata', 'onc_patientlist', 'onc_problemlist', 'onc_surgery_post_epic',
                     'onc_surgery_pre_epic']
    #
    # for table_name in list_of_tables:
    #     drop_table("onc_beacondata")
    #
    for table_name in list_of_tables:
        if table_name == 'onc_surgery_pre_epic':
            copy_data(table_name, sep='|')
        else:
            copy_data(table_name, sep=',')
