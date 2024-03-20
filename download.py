import argparse
from pathlib import Path
import pandas as pd
import xnat
import os
import shutil
import logging
import numpy as np
import re
import json
import time
import hashlib
from distutils.dir_util import copy_tree
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)
from dbconnection.connect_to_db import *


def download_xnat_session_type(connection, projectid, session, outputdir):
    output_path = os.path.join(outputdir, connection.projects[projectid].experiments[session].label)
    connection.projects[projectid].experiments[session].download(output_path)


def download_on_mrn(conn, cur, outputdir, hmrn=None, hdob=None, resource='NIFTI', project='UCLH',
                    scan_type_list=None, original_mrn=None, add_session_labels=False):

    server_path = '/home/samia/Documents/xnatuser/XNAT-DATA/xnat-data/archive'
    try:
        fetch_query = "SELECT public.xnat_subjectdata.label as subject_label," \
                        " public.xnat_subjectassessordata.subject_id," \
                        " xnat_subjectassessordata.id as experiment_id," \
                        " xnat_experimentdata.label as session_label," \
                        " public.xnat_imagescandata.id as scan_id," \
                        " public.xnat_imagescandata.type as scan_type," \
                        " public.xnat_imagescandata.series_description " \
                        "FROM public.xnat_mrscandata, public.xnat_imagescandata," \
                        " public.xnat_imagesessiondata, public.xnat_subjectassessordata," \
                        " public.xnat_subjectdata, public.xnat_experimentdata " \
                        "WHERE public.xnat_mrscandata.xnat_imagescandata_id = public.xnat_imagescandata.xnat_imagescandata_id " \
                        "AND public.xnat_imagescandata.image_session_id=public.xnat_imagesessiondata.ID " \
                        "AND public.xnat_imagescandata.image_session_id = public.xnat_subjectassessordata.id " \
                        "AND public.xnat_subjectdata.id = public.xnat_subjectassessordata.subject_id " \
                        "AND xnat_imagesessiondata.id = xnat_experimentdata.id " \
                        " AND ((public.xnat_imagescandata.type NOT SIMILAR TO " \
                        "'(%SCOUT%|%OTHER%|%scout%|%other%|%Scout%|%Other)%') " \
                        "AND (public.xnat_imagescandata.series_description " \
                        "NOT SIMILAR to '(%localizer%|%Localizer%|%LOCALIZER|SCOUT|Scout|scout)%')) "

        if hmrn is not None and hdob is not None:
            fetch_query += f" AND xnat_imagesessiondata.patientidentifier = '{hmrn}'" \
                             f" AND  xnat_imagesessiondata.dateofbirthidentifier = '{hdob}' "
        elif hmrn is not None:
            fetch_query += f" AND xnat_imagesessiondata.patientidentifier = '{hmrn}'"
        else:
            print('No MRN cannot download!!')
            return

        cur.execute(fetch_query)
        df = pd.DataFrame(cur.fetchall(), columns=["subject_label", "subject_id", "experiment_id", "session_label",
                                                   "scan_id", "scan_type", "series_description"], dtype=object)

        if scan_type_list is not None:
            df = df[df['scan_type'].str.contains('|'.join(scan_type_list), regex=True)]

        for index, row in df.iterrows():
            src = f"{server_path}/{project}/arc001/{row['session_label']}/SCANS/{row['scan_id']}/{resource}/"

            if original_mrn is not None:
                dst = f"{os.path.join(outputdir, original_mrn)}"
            else:
                dst = f"{os.path.join(outputdir, hmrn)}"

            if add_session_labels:
                dst = f"{os.path.join(dst, row['session_label'])}/{row['scan_type'].replace(',', '').replace(' ', '')}/"
            else:
                dst = f"{dst}/{row['scan_type'].replace(',', '').replace(' ', '')}/"

            if os.path.exists(src):
                os.makedirs(dst, exist_ok=True)
                if os.path.exists(dst):
                    copy_tree(src, dst)

            # delete unwanted files from destination folder like .xml
            if resource == 'NIFTI':
                files = os.listdir(dst)
                files = [os.remove(os.path.join(dst, f)) for f in files if not f.endswith(('nii', 'nii.gz'))]

            elif resource == 'DICOM':
                files = os.listdir(dst)
                files = [os.remove(os.path.join(dst, f)) for f in files if not f.endswith(('.dcm', 'dicom'))]

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        conn.close()


def clean_mrn(mrn):
    mrn = str(mrn)
    mrn = mrn.upper().replace(' ', '')
    if len(mrn) < 8 and mrn[0].isdigit():
        mrn = mrn.zfill(8)

    return mrn


def hash(df):
    password = 'Parkgasse18'

    for index, row in df.iterrows():
        try:
            if 'patientid' in row:
                mrn = clean_mrn(row['patientid'])
                mrn = password + mrn
                m = hashlib.shake_256()
                m.update(mrn.encode('utf-8'))
                h_mrn = m.hexdigest(15)
                df.loc[index, 'hmrn'] = h_mrn
            if 'dateofbirth' in row:
                dob = password + str(row['dateofbirth'])
                m = hashlib.shake_256()
                m.update(dob.encode('utf-8'))
                h_dob = m.hexdigest(15)
                df.loc[index, 'hdob'] = h_dob

        except Exception as e:
            print(e)
            df.loc[index, 'hmrn'] = None
            if 'dateofbirth' in df:
                df.loc[index, 'hdob'] = None

    return df


def main():
    parser = argparse.ArgumentParser(description='Download niftis/dcms from XNAT')
    parser.add_argument('--csv', type=str, help="a csv containing mrns, dobs  having column names: patientid,"
                        "dateofbirth in format YYYY-MM-DD",
                        default='/mnt/wwn-0x5000c500cc87eb78/xnatpy/testdownload.csv')
    parser.add_argument('--anonymise', default=False, type=bool,
                        help="Do you want your downloaded folders to have anonymised folder structure? default: True")
    # enhancement add this to the input csv/ accept from args multiple strings as a list
    parser.add_argument('--scantypes', type=list, default=None,
                        help="pass a list of modalities you want to download like ['T1', 'T2'],"
                        "default : None")
    parser.add_argument('--download_folders_with_session', type=bool, default=False,
                        help="Do you want add session ids to downloaded folders? default: False")
    parser.add_argument('--o', type=str, help="output directory",
                        default='/mnt/wwn-0x5000c500cc87eb78/IvyGAP/downloadtest')

    args = parser.parse_args()
    df = pd.read_csv(args.csv)
    keys = df.keys().tolist()

    # connect to db
    conn = None
    cur = None
    try:
        conn = connect()
        cur = conn.cursor()
    except Exception as e:
        print(f"Connection error: {e}")
        conn.rollback()
        cur.close()
        conn.close()

    if 'patientid' in keys and 'dateofbirth' in keys:
        hashed_mrns_dobs = hash(df)
        for index, row in hashed_mrns_dobs.iterrows():
            try:
                download_on_mrn(conn, cur, outputdir=args.o,
                                hmrn=row['hmrn'] if row['hmrn'] is not None else None,
                                hdob=row['hdob'] if row['hdob'] is not None else None,
                                project='UCLH',
                                scan_type_list=args.scantypes,
                                original_mrn=str(row['patientid']) if not args.anonymise else None,
                                add_session_labels=args.download_folders_with_session)
            except Exception as e:
                print(e)

    elif 'patientid' in keys:
        hashed_mrns_dobs = hash(df)
        for index, row in hashed_mrns_dobs.iterrows():
            try:
                download_on_mrn(conn, cur, outputdir=args.o,
                                hmrn=row['hmrn'] if row['hmrn'] is not None else row['patientid'],
                                project='UCLH', scan_type_list=args.scantypes,
                                original_mrn=str(row['patientid']) if not args.anonymise else None,
                                add_session_labels=args.download_folders_with_session)
            except Exception as e:
                print(e)

    else:
        print('Please check csv. Cannot find keys or csv is blank!')

    cur.close()
    conn.close()
    print('Connection Closed!')


if __name__ == '__main__':
    main()