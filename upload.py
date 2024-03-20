# Author: Samia Mohinta
#!/usr/bin/env python

# Copyright 2011-2015 Biomedical Imaging Group Rotterdam, Departments of
# Medical Informatics and Radiology, Erasmus MC, Rotterdam, The Netherlands
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from pathlib import Path
import sys
path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, path)

import argparse
import pandas as pd
import xnat
import os
import logging
import numpy as np
import re
from dbconnection.connect_to_db import *
import json
import hashlib
import time


logging.basicConfig(filename='/mnt/wwn-0x5000c500cc87eb78/XNAT_MIGRATION_1_8_3/logs/upload_nifti.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s', filemode='w+')
logger = logging.getLogger(__name__)

pd.options.mode.chained_assignment = None


def copy_mrscandata(mrscan_data, xnat_scan):
    """
               Copy over all xnat_mrscandata per experiment's scan
    """
    scan_list = ['coil', 'fieldstrength', 'marker', 'stabilization', 'parameters_voxelres_units',
                 'parameters_voxelres_x', 'parameters_voxelres_y', 'parameters_voxelres_z', 'parameters_orientation',
                 'parameters_fov_x', 'parameters_fov_y', 'parameters_matrix_x', 'parameters_matrix_y',
                 'parameters_partitions', 'parameters_tr', 'parameters_te', 'parameters_ti',
                 'parameters_flip', 'parameters_sequence', 'parameters_origin', 'parameters_imagetype',
                 'parameters_scansequence', 'parameters_seqvariant', 'parameters_scanoptions', 'parameters_acqtype',
                 'parameters_coil', 'parameters_dtiacqcount', 'parameters_pixelbandwidth',
                 'parameters_diffusion_bvalues', 'parameters_diffusion_directionality',
                 'parameters_diffusion_orientations', 'parameters_diffusion_anisotropytype', 'dcmvalidation',
                 'dcmvalidation_status']
    try:
        mrscan_data_s = mrscan_data[scan_list].drop_duplicates().replace(
            {np.nan: None})
    
        for key in scan_list:
            try:
                value = mrscan_data_s[key].values[0]
    
                if value is not None:
                    if not isinstance(mrscan_data_s[key].values[0], str) \
                            and np.array_equal(mrscan_data_s[key], mrscan_data_s[key].astype(int)):
                        value = int(mrscan_data_s[key].values[0])
    
                    xnat_scan.set(key, value)
            except Exception as e:
                print(f"mr scan data update: {e}")
    except Exception as e:
        print(f"mr scan data update: {e}")
        

def copy_experimentdata(xnat_experimentdata, xnat_experiment):
    """
           Copy over all xnat_experimentdata per experiment
    """
    experimentdata_list = ['date', 'time', 'duration', 'delay', 'note',
                 'acquisition_site', 'visit_id', 'version', 'original', 'protocol']

    try:
        xnat_experimentdata_s = xnat_experimentdata[experimentdata_list].drop_duplicates().replace(
            {np.nan: None})
    
        for key in experimentdata_list:
            try:
                value = xnat_experimentdata_s[key].values[0]
                if value is not None:
                    xnat_experiment.set(key, value)
            except Exception as e:
                print(f"Experiment data update: {e}")

    except Exception as e:
        print(f"Experiment data update: {e}")


def copy_imagescandata(xnat_imagescandata, xnat_scan):
    """
        Copy over all xnat_imagescandata per experiment's scan
        """
    scan_list = ['note', 'condition', 'documentation', 'scanner', 'scanner_manufacturer', 'scanner_model', 'modality',
                 'frames', 'operator', 'validation_xnat_validationdata_id', 'starttime', 'uid']

    try:
        xnat_imagescandata[['frames']] = xnat_imagescandata[['frames']].astype(np.int32)

        xnat_imagescandata_s = xnat_imagescandata[scan_list].drop_duplicates().replace(
            {np.nan: None})

        for key in scan_list:
            value = xnat_imagescandata_s[key].values[0]
            if value is not None:
                xnat_scan.set(key, value)

    except Exception as e:
        print(f"Image scan update: {e}")


def copy_imagesessiondata(xnat_imagesessiondata, xnat_experiment):
    """
    Copy over all xnat_imagesessiondata for an experiment
    """
    # we dont need 'id', 'prearchivepath', 'imagesessiondata_info' hence removed from list

    imagesession_list = ['scanner', 'scanner_manufacturer', 'scanner_model', 'operator',
                         'dcmaccessionnumber', 'dcmpatientid', 'dcmpatientname', 'dcmpatientbirthdate', 'session_type',
                         'modality', 'uid']

    try:
        xnat_imagesessiondata_s = xnat_imagesessiondata[imagesession_list].drop_duplicates().replace(
            {np.nan: None})
    
        for key in imagesession_list:
            # uppercase and remove space from dcmpatientid
            if key == 'dcmpatientid':
                value = str(xnat_imagesessiondata_s[key].values[0]).upper().replace(' ', '')
                if len(value) < 8 and not value[0].isalpha():
                    value = value.zfill(8)
            else:
                value = xnat_imagesessiondata_s[key].values[0]

            if value is not None:
                xnat_experiment.set(key, value)

    except Exception as e:
        print(f"Image session update: {e}")
        
        
def copy_demographics(xnat_demographicdata, xnat_subject):
    """
    Copy over all demographics for a subject
    """
    demographic_list = [
        'age', 'birth_weight', 'dob', 'education', 'educationdesc', 'employment',
        'ethnicity', 'gender', 'gestational_age', 'handedness', 'height', 'post_menstrual_age',
        'race', 'race2', 'race3', 'race4', 'race5', 'race6', 'ses', 'weight', 'yob']

    try:
        xnat_demographicdata_s = xnat_demographicdata[demographic_list].drop_duplicates().replace(
            {np.nan: None})

        demographics_data = {}

        for demographic in demographic_list:
            try:
                value = xnat_demographicdata_s[demographic].values[0]

                if demographic == 'gender':
                    value = str(xnat_demographicdata_s[demographic].values[0][0]).upper()

                if demographic == 'yob' and xnat_demographicdata_s['dob'].values[0] is not None:
                    value = int(xnat_demographicdata_s['dob'].values[0][:4])

                if value is not None:
                    demographics_data[demographic] = value
            except Exception as e:
                print(f"Demographics update {e}")
        xnat_subject.demographics.mset(demographics_data)
        
    except Exception as e:
        print(f"Demographics update {e}")


def data_exists(args: object, patientids=None) -> object:
    conn = None
    cur = None

    try:
        conn = connect()
        cur = conn.cursor()

        q_select = "select  public.xnat_subjectdata.label as subject_id, " \
                "public.xnat_experimentdata.label as session_label," \
                " public.xnat_imagescandata.id as scan_id," \
                " public.xnat_abstractresource.label as resource_name," \
                " public.xnat_imagesessiondata.dcmpatientid as patientid" \
                " from public.xnat_imagesessiondata, public.xnat_experimentdata," \
                " public.xnat_subjectassessordata, public.xnat_subjectdata," \
                " public.xnat_imagescandata, public.xnat_mrscandata," \
                " public.xnat_abstractresource, public.xnat_resourcecatalog," \
                " public.xnat_resource where public.xnat_imagesessiondata.id = public.xnat_experimentdata.id" \
                " and public.xnat_subjectassessordata.id = public.xnat_experimentdata.id" \
                " and public.xnat_subjectdata.id = public.xnat_subjectassessordata.subject_id " \
                "and public.xnat_imagesessiondata.id = public.xnat_imagescandata.image_session_id" \
                " and public.xnat_mrscandata.xnat_imagescandata_id = public.xnat_imagescandata.xnat_imagescandata_id" \
                " and public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id = public.xnat_imagescandata.xnat_imagescandata_id" \
                " and public.xnat_resourcecatalog.xnat_abstractresource_id = public.xnat_abstractresource.xnat_abstractresource_id" \
                " and public.xnat_resource.xnat_abstractresource_id = public.xnat_abstractresource.xnat_abstractresource_id " \

        if patientids is not None:
            q_where = f"and public.xnat_imagesessiondata.dcmpatientid in {patientids} "
            q_select += q_where

        q_grp_by = "group by" \
                " public.xnat_subjectdata.label," \
                " public.xnat_experimentdata.label, public.xnat_imagescandata.id," \
                " public.xnat_abstractresource.label," \
                " public.xnat_imagesessiondata.dcmpatientid;"

        query = q_select + q_grp_by
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=["subject_id", "session_label", "scan_id", "resource_name",
                                                   "patientid"], dtype=object)
        return df
    except (Exception, psycopg2.DatabaseError) as error:

        print("Error: %s" % error)
        conn.rollback()
        cur.close()


def upload_files(connection, project, subject, experiment, experiment_type, scan, scan_label,
                 scan_description, resource, data, dict_of_df, args):

    if experiment_type not in ['CT', 'MR']:
        print(f"[ERROR] experiment type {experiment_type} not supported use 'MR' or 'CT'")
        return
    testdata = dict_of_df['xnat_imagesessiondata'][dict_of_df['xnat_imagesessiondata']['subject_id'] == subject]
    if testdata['dcmpatientid'].str.upper().str.contains('TEST').any():
        print(f" TEST data not uploaded")
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
                                                      series_description=scan_description, quality='usable', label=scan)
        elif experiment_type == 'MR':
            xnat_scan = connection.classes.MrScanData(parent=xnat_experiment, id=scan, type=scan_label,
                                                      series_description=scan_description, quality='usable', label=scan)
        else:
            print(f"[ERROR] scan type {experiment_type} not supported use 'MR' or 'CT'")
            return

    # If resource exists create new resource
    if resource in xnat_scan.resources:
        xnat_resource = xnat_scan.resources[resource]
    else:
        xnat_resource = connection.classes.ResourceCatalog(parent=xnat_scan, label=resource, format=resource)

    if int(args.ofiles_odata_both) == 0:    # just uploads scans
        for file_ in data:
            if file_.split('/')[-1] not in xnat_resource.files.keys():
                file_ = Path(file_)
                if file_.exists():
                    xnat_resource.upload(str(file_), file_.name)
                else:
                    print(f'[WARNING] Could not find file: {file_}')
                    
    elif int(args.ofiles_odata_both) == 1:  # just updates db
        # copy demographics
        # if not len(xnat_subject.demographics.data):
        subject_demographics_data = dict_of_df['xnat_demographicdata']
        subject_demographics_data = subject_demographics_data[subject_demographics_data['subject_id'] == subject]

        if len(subject_demographics_data):
            copy_demographics(subject_demographics_data, xnat_subject)

        # if not len(xnat_experiment.data):
        # copy imagesessiondata
        experiment_imagesession_data = dict_of_df['xnat_imagesessiondata']
        experiment_imagesession_data = experiment_imagesession_data[
            (experiment_imagesession_data['subject_id'] == subject)
            & (experiment_imagesession_data['session_label']
               == experiment)]
        if len(experiment_imagesession_data):
            copy_imagesessiondata(experiment_imagesession_data, xnat_experiment)

        # copy experimentdata
        experiment_data = dict_of_df['xnat_experimentdata']
        experiment_data = experiment_data[(experiment_data['subject_id'] == subject)
                                          & (experiment_data['label'] == experiment)]
        if len(experiment_data):
            copy_experimentdata(experiment_data, xnat_experiment)

        # copy imagescandata
        imagescan_data = dict_of_df['xnat_imagescandata']
        imagescan_data = imagescan_data[(imagescan_data['session_label'] == experiment)
                                        & (imagescan_data['id'].astype(str) == str(scan))]
        if len(imagescan_data):
            copy_imagescandata(imagescan_data, xnat_scan)

        # copy mrscandata
        mrscan_data = dict_of_df['xnat_mrscandata']
        mrscan_data = mrscan_data[(mrscan_data['session_label'] == experiment)
                                  & (mrscan_data['scan_id'].astype(str) == str(scan))]
        if len(mrscan_data):
            copy_mrscandata(mrscan_data, xnat_scan)

    elif int(args.ofiles_odata_both) == 2:  # uploads scans and updates db
        for file_ in data:
            if file_.split('/')[-1] not in xnat_resource.files.keys():
                file_ = Path(file_)
                if file_.exists():
                    xnat_resource.upload(str(file_), file_.name)
                else:
                    print(f'[WARNING] Could not find file: {file_}')
        
        # copy demographics
        subject_demographics_data = dict_of_df['xnat_demographicdata']
        subject_demographics_data = subject_demographics_data[subject_demographics_data['subject_id'] == subject]
        if len(subject_demographics_data):
            copy_demographics(subject_demographics_data, xnat_subject)

        # copy imagesessiondata
        experiment_imagesession_data = dict_of_df['xnat_imagesessiondata']
        experiment_imagesession_data = experiment_imagesession_data[
            (experiment_imagesession_data['subject_id'] == subject)
            & (experiment_imagesession_data['session_label']
               == experiment)]
        if len(experiment_imagesession_data):
            copy_imagesessiondata(experiment_imagesession_data, xnat_experiment)

        # copy experimentdata
        experiment_data = dict_of_df['xnat_experimentdata']
        experiment_data = experiment_data[(experiment_data['subject_id'] == subject)
                                          & (experiment_data['label'] == experiment)]
        if len(experiment_data):
            copy_experimentdata(experiment_data, xnat_experiment)

        # copy imagescandata
        imagescan_data = dict_of_df['xnat_imagescandata']
        imagescan_data = imagescan_data[(imagescan_data['session_label'] == experiment)
                                        & (imagescan_data['id'].astype(str) == str(scan))]
        if len(imagescan_data):
            copy_imagescandata(imagescan_data, xnat_scan)

        # copy mrscandata
        mrscan_data = dict_of_df['xnat_mrscandata']
        mrscan_data = mrscan_data[(mrscan_data['session_label'] == experiment)
                                  & (mrscan_data['scan_id'].astype(str) == str(scan))]
        if len(mrscan_data):
            copy_mrscandata(mrscan_data, xnat_scan)
        
        
def create_upload_csv(path, list_of_tablenames):
    xnathost = 'http://localhost'
    project = 'UCLH'

    connection = xnat.connect(xnathost, user='admin', password='admin', default_timeout=60)
    dict_of_df = dict()
    for t in list_of_tablenames:
        dict_of_df['_'.join(t.split('_')[0:2])] = pd.read_csv(os.path.join(path, t), sep='|', encoding='latin-1',
                                                              low_memory=False)

    return dict_of_df, connection, project


def hash_mrn(patientid):
    password = 'Parkgasse18'
    mmrn = hashlib.shake_256()
    mrn = password + patientid
    mmrn.update(mrn.encode('utf-8'))
    h_mrn = mmrn.hexdigest(15)
    return h_mrn


def check_hashcolumns_exists(column_name):
    conn = None
    cur = None
    try:
        conn = connect()
        cur = conn.cursor()
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name='xnat_imagesessiondata'" \
                f" and column_name='{column_name}';"

        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=["column_name"], dtype=object)
        if len(df):
            return True
        else:
            return False
    except Exception as e:
        print(e)
        cur.close()
        conn.close()
    cur.close()
    conn.close()


def loop_and_upload(dict_of_df, connection, project, args):
    xnat_resourcepaths = dict_of_df['xnat_resourcepaths']
    unique_subjects_scans = xnat_resourcepaths[['session_label', 'scan_id', 'resource_name', 'patientid']].drop_duplicates()
    ex_df =None
    if args.resume:
        ex_df = data_exists(args, patientids=tuple(set(unique_subjects_scans['patientid'].to_list())))
        unique_subjects_scans = unique_subjects_scans.applymap(str)
        if ex_df is not None:
            ex_df = ex_df.applymap(str)
            # check if hashpatientid exists in xnat_imagesessiondata table
            if check_hashcolumns_exists('hashpatientid') or ex_df['patientid'].str.len().all() == 30:
                for index, row in unique_subjects_scans.iterrows():
                    hash_mrn(row['patientid'])
            # to drop exact duplicates
            ex_df_temp = ex_df[['session_label', 'scan_id', 'resource_name', 'patientid']]
            unique_subjects_scans = pd.concat([unique_subjects_scans, ex_df_temp]).drop_duplicates(keep=False)

    for index, row in unique_subjects_scans.iterrows():
        # time taken to create subjects and upload scans
        # startTime = time.time()
        try:
            xnat_resourcepaths_s = xnat_resourcepaths[
                (xnat_resourcepaths['patientid'] == row['patientid']) &
                (xnat_resourcepaths['subject_id'].str.upper() != 'TEST')
                & (xnat_resourcepaths['scan_id'].astype(str) == row['scan_id'])
                & (xnat_resourcepaths['resource_name'] == row['resource_name'])
                & (xnat_resourcepaths['session_label'] == row['session_label'])
                ]
            subject = xnat_resourcepaths_s.iloc[0]['subject_id']
            if ex_df is not None:
                if row['patientid'] in ex_df:
                    subject = ex_df[ex_df['patientid'] == row['patientid']].iloc[0]['subject_id']

            experiment = xnat_resourcepaths_s.iloc[0]['session_label']
            experiment_type = xnat_resourcepaths_s.iloc[0]['modality']
            scan_description = xnat_resourcepaths_s.iloc[0]['series_desc']
            scan_label = xnat_resourcepaths_s.iloc[0]['scan_type']
            scan = xnat_resourcepaths_s.iloc[0]['scan_id']
            resource_name = xnat_resourcepaths_s.iloc[0]['resource_name']

            files = xnat_resourcepaths_s[['mnt_filepath', 'filename']].agg(''.join, axis=1).to_list()

            print(f"processing: {subject}, {experiment}, {scan}, {scan_description}, {len(files)}")
            upload_files(connection, project, subject, experiment, experiment_type, scan, scan_label, scan_description,
                         resource_name, files, dict_of_df, args)

            # executionTime = (time.time() - startTime)
            # print('Execution time in seconds to replace one subjects scans with db updates: ' + str(executionTime))

        except Exception as e:
            print(f"Error: {e}")
            logger.error(e)

    connection.disconnect()


def main():
    parser = argparse.ArgumentParser(description='Upload niftis/dcms to XNAT')
    parser.add_argument('--datetimestamp', type=str, default='12092021_164814',
                        help="timestamp to find which csv to upload")
    parser.add_argument('--ofiles_odata_both', default=2, type=int,
                        help="Upload only dcms/niis first"
                        "0: Only files uploaded"
                        "1: Only db updates"
                        "2: Both files and db uploaded/updated")
    parser.add_argument('--resume', type=bool, default=True,
                        help=" resumes upload from non-uploaded subject-experiment-scan")

    args = parser.parse_args()
    path = '/mnt/wwn-0x5000c500cc87eb78/xnat_test/DATA2/for_xnat_upload'
    list_of_tablenames = [
                          f'xnat_demographicdata_{args.datetimestamp}.csv',
                          f'xnat_experimentdata_{args.datetimestamp}.csv',
                          f'xnat_imagescandata_{args.datetimestamp}.csv',
                          f'xnat_imagesessiondata_{args.datetimestamp}.csv',
                          f'xnat_mrscandata_{args.datetimestamp}.csv',
                          f'xnat_resourcepaths_{args.datetimestamp}.csv']

    result, connection, project = create_upload_csv(path, list_of_tablenames)

    loop_and_upload(result, connection, project, args)
    if int(args.ofiles_odata_both) == 0:
        args.ofiles_odata_both = 1
        loop_and_upload(result, connection, project, args)


if __name__ == '__main__':
    main()


