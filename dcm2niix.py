import argparse
from pathlib import Path
import pandas as pd
import xnat
import os
import shutil
import logging
import numpy as np
import re
# from connect_to_db import *
from dbconnection.connect_to_db import *
import json
import time
import docker
import sys

# XNAT's dcm2niix command
# docker run  -v '/mnt/wwn-0x5000c500cc87eb78/IvyGAP/test':/input -v '/mnt/wwn-0x5000c500cc87eb78/IvyGAP/test':/output   xnat_dcm2niix:v1 dcm2niix -z y -o /output /input

# Chris Foulon's dcm2niix command
# docker run  -v '/mnt/wwn-0x5000c500cc87eb78/IvyGAP/test':/input -v '/mnt/wwn-0x5000c500cc87eb78/IvyGAP/output':/output xnat_chrisdcm2niix:v1  python ./data_identification/scripts/dicom_conversion.py -re none -v info  -p /input -o /output -do "-z o -f dsabhjfdgahdff" -nc 1


def truncate_table():
    conn = None
    cur = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""Truncate table xnat_dcm2niix;""")
        conn.commit()
        cur.close()
        print('dcm2niix table truncated successfully!!')
        conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            print('Database connection closed.')


def create_dcm2niix_table():
    conn = None
    cur = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS 
                  xnat_dcm2niix(
                  subjectlabel  varchar(255),
                  experimentid  varchar(255),
                  subjectid  varchar(255),
                  sessionlabel varchar(255),
                  scanid varchar(255),
                  scantype varchar(255),
                  resourcename varchar(255),
                  uri text,
                  nifticreated varchar(10),
                  remarks text,
                  UNIQUE(subjectlabel, experimentid, subjectid, sessionlabel, scanid, scantype,
                  resourcename, uri));
                 """)

        insert_query = "insert into xnat_dcm2niix (subjectlabel, experimentid, subjectid, sessionlabel, scanid, scantype," \
                       "resourcename, uri) select public.xnat_subjectdata.label, public.xnat_subjectassessordata.id," \
                      " public.xnat_subjectassessordata.subject_id, public.xnat_experimentdata.label," \
                      " public.xnat_imagescandata.id," \
                      " public.xnat_imagescandata.type, public.xnat_abstractresource.label, public.xnat_resource.uri " \
                      "from public.xnat_imagesessiondata, public.xnat_experimentdata, public.xnat_subjectassessordata, " \
                      "public.xnat_subjectdata, public.xnat_imagescandata, public.xnat_mrscandata, " \
                      "public.xnat_abstractresource, public.xnat_resourcecatalog, public.xnat_resource," \
                      " (select public.xnat_subjectdata.label as subject_label, " \
                      "public.xnat_subjectassessordata.id as experiment_id, " \
                      "public.xnat_subjectassessordata.subject_id, " \
                      "public.xnat_experimentdata.label as experiment_label, public.xnat_imagescandata.id as scan_id, " \
                      "public.xnat_imagescandata.type," \
                      " count(public.xnat_imagescandata.id) as number_of_dcmnii " \
                      "from public.xnat_imagesessiondata, public.xnat_experimentdata, " \
                      "public.xnat_subjectassessordata, public.xnat_subjectdata, " \
                      "public.xnat_imagescandata, public.xnat_mrscandata, " \
                      "public.xnat_abstractresource, public.xnat_resourcecatalog, " \
                      "public.xnat_resource " \
                      "where public.xnat_imagesessiondata.id = public.xnat_experimentdata.id and " \
                      "public.xnat_subjectassessordata.id = public.xnat_experimentdata.id and " \
                      "public.xnat_subjectdata.id = public.xnat_subjectassessordata.subject_id and " \
                      "public.xnat_imagesessiondata.id = public.xnat_imagescandata.image_session_id and " \
                      "public.xnat_mrscandata.xnat_imagescandata_id = public.xnat_imagescandata.xnat_imagescandata_id " \
                      "and public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id = public.xnat_imagescandata.xnat_imagescandata_id and " \
                      "public.xnat_resourcecatalog.xnat_abstractresource_id = public.xnat_abstractresource.xnat_abstractresource_id " \
                      "and public.xnat_resource.xnat_abstractresource_id = public.xnat_abstractresource.xnat_abstractresource_id " \
                      "and lower(public.xnat_imagescandata.type) not like '%scout%' " \
                      "group by public.xnat_subjectdata.label, public.xnat_subjectassessordata.id, public.xnat_subjectassessordata.subject_id," \
                      " public.xnat_experimentdata.label,public.xnat_imagescandata.id, public.xnat_imagescandata.type" \
                      " having count(public.xnat_imagescandata.id) < 2 ) T2 " \
                      "where public.xnat_subjectdata.label = T2.subject_label " \
                      "and public.xnat_subjectassessordata.id = T2.experiment_id " \
                      "and public.xnat_subjectassessordata.subject_id = T2.subject_id " \
                      "and public.xnat_imagesessiondata.id = public.xnat_experimentdata.id " \
                      "and public.xnat_imagescandata.id = T2.scan_id " \
                      "and public.xnat_experimentdata.label = T2.experiment_label " \
                      "and public.xnat_imagesessiondata.id = public.xnat_imagescandata.image_session_id " \
                      "and public.xnat_mrscandata.xnat_imagescandata_id = public.xnat_imagescandata.xnat_imagescandata_id " \
                      "and public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id = public.xnat_imagescandata.xnat_imagescandata_id " \
                      "and public.xnat_resourcecatalog.xnat_abstractresource_id = public.xnat_abstractresource.xnat_abstractresource_id " \
                      "and public.xnat_resource.xnat_abstractresource_id = public.xnat_abstractresource.xnat_abstractresource_id " \
                      "and public.xnat_abstractresource.label = 'DICOM' " \
                      "order by public.xnat_subjectassessordata.subject_id ON CONFLICT DO NOTHING;"

        cur.execute(insert_query)
        conn.commit()
        cur.close()
        print('dcm2niix table created successfully!!')

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.rollback()
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            print('Database connection closed.')


def dcm2niix(input_vol, output_vol, filename=None):
    image = 'xnat_chrisdcm2niix:v1'
    try:
        client = docker.from_env()

        if filename is not None:
            # cmd = f'dcm2niix -f {filename} -z y -o /output /input'
            cmd = f'python ./data_identification/scripts/dicom_conversion.py -re none ' \
                   f'-p /input -o /output -do "-z o -f {filename}" -nc 1'
        else:
            # cmd = f'dcm2niix -z y -o /output /input'
            cmd = f'python ./data_identification/scripts/dicom_conversion.py -re none -v info  ' \
                   f'-p /input -o /output -do "-z o" -nc 1'

        status = client.containers.run(image, cmd, volumes=[f'{input_vol}:/input', f'{output_vol}:/output'])
        status = status.decode("utf-8").upper()
        if 'Status exit code' not in status.decode("utf-8").upper():
            return True
        else:
            return False
    except Exception as e:
        print(f"{e} in {input_vol}")
        return False


def upload_files(connection, subject, experiment, experiment_type, scan, scan_label,
                     scan_description, resource, data, project='UCLH'):

        if experiment_type not in ['CT', 'MR']:
            print(f"[ERROR] experiment type {experiment_type} not supported use 'MR' or 'CT'")
            return

        xnat_project = connection.projects[project]
        # If subject does not exist create subject
        if subject in xnat_project.subjects:
            xnat_subject = xnat_project.subjects[subject]
        else:
            xnat_subject = connection.classes.SubjectData(parent=xnat_project, label=subject)

        # if experiment does not create new experiment
        if experiment in xnat_subject.experiments:
            xnat_experiment = xnat_subject.experiments[experiment]
        else:
            if experiment_type == 'CT':
                xnat_experiment = connection.classes.CtSessionData(parent=xnat_subject, label=experiment)
            elif experiment_type == 'MR':
                xnat_experiment = connection.classes.MrSessionData(parent=xnat_subject, label=experiment)
            else:
                print(f"[ERROR] experiment type {experiment_type} not supported use 'MR' or 'CT'")
                return

        # if scan does not exist create new Scan
        if str(scan) in xnat_experiment.scans:
            xnat_scan = xnat_experiment.scans[str(scan)]
        else:

            if experiment_type == 'CT':
                xnat_scan = connection.classes.CtScanData(parent=xnat_experiment, id=scan, type=scan_label,
                                                          series_description=scan_description, quality='usable',
                                                          label=scan)
            elif experiment_type == 'MR':
                xnat_scan = connection.classes.MrScanData(parent=xnat_experiment, id=scan, type=scan_label,
                                                          series_description=scan_description, quality='usable',
                                                          label=scan)
            else:
                print(f"[ERROR] scan type {experiment_type} not supported use 'MR' or 'CT'")
                return

        # If resource exists create new resource
        if resource in xnat_scan.resources:
            xnat_resource = xnat_scan.resources[resource]
        else:
            xnat_resource = connection.classes.ResourceCatalog(parent=xnat_scan, label=resource, format=resource)

        for file_ in data:
            if file_.split('/')[-1] not in xnat_resource.files.keys():
                file_ = Path(file_)
                if file_.exists():
                    xnat_resource.upload(str(file_), file_.name)
                else:
                    print(f'[WARNING] Could not find file: {file_}')


def main():
    conn = None
    cur = None
    xnathost = 'http://localhost'
    project = 'UCLH'

    # create the dcm2niix table
    create_dcm2niix_table()

    try:
        connection = xnat.connect(xnathost, user='admin', password='admin', default_timeout=300)

        conn = connect()
        cur = conn.cursor()
        fetch_query = "select subjectlabel, experimentid, subjectid, sessionlabel, scanid, scantype," \
                       "resourcename, uri, patientidentifier from xnat_dcm2niix, xnat_imagesessiondata where" \
                      " (xnat_dcm2niix.nifticreated is NULL or xnat_dcm2niix.nifticreated = 'N') " \
                      "and xnat_dcm2niix.experimentid = xnat_imagesessiondata.id;"

        cur.execute(fetch_query)
        df = pd.DataFrame(cur.fetchall(), columns=["subject_label", "experiment_id", "subject_id", "session_label",
                                                   "scan_id", "scan_type", "resource_name", "uri", "patientidentifier"],
                          dtype=object)
        static_mnt_path = '/home/samia/Documents/xnatuser/XNAT-DATA/xnat-data'
        updatequery = """ UPDATE xnat_dcm2niix
                                       SET nifticreated = %s
                                       WHERE subjectlabel = %s AND 
                                       experimentid = %s AND 
                                       subjectid = %s AND
                                       sessionlabel = %s AND
                                       scanid = %s AND
                                       scantype = %s AND
                                       resourcename = %s AND
                                       uri = %s
                                       """
        for index, row in df.iterrows():
            try:
                input_vol = os.path.join(static_mnt_path, '/'.join(row['uri'].split('/')[3:-1]))
                output_vol = os.path.join('/tmp', '/'.join(row['uri'].split('/')[3:-2]) + '/NIFTI')
                status = dcm2niix(input_vol, output_vol,
                                  filename=row['patientidentifier'] if row['patientidentifier'] is not None else None)
                if status:
                    files = [os.path.join(output_vol, x) for x in os.listdir(output_vol)] # re.sub('[^A-Za-z0-9-_.]+', '', x)
                    upload_files(connection, row['subject_label'], row['session_label'], 'MR', row['scan_id'],
                                 row['scan_type'], row['scan_type'], 'NIFTI', files, project='UCLH')
                    cur.execute(updatequery,  ('Y', row['subject_label'], row['experiment_id'], row['subject_id'],
                                               row['session_label'], row['scan_id'], row['scan_type'],
                                               row['resource_name'], row['uri']))
                else:
                    cur.execute(updatequery, ('N', row['subject_label'], row['experiment_id'], row['subject_id'],
                                              row['session_label'], row['scan_id'], row['scan_type'],
                                              row['resource_name'], row['uri']))
                conn.commit()

                # shutil.rmtree(output_vol)

            except Exception as e:
                print(f"{e} in {row['uri']}")
                conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:

        print("Error: %s" % error)
        conn.rollback()
        cur.close()
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
