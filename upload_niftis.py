import sys
import os
import csv
import subprocess
import fileinput
import datetime
import time
import string
import server_utils as svr

#from dax import XnatUtils




REMOTE = "/data/archive/experiments"
REMOTE_END= "?extract=true&overwrite=delete&format=NIFTI&content=NIFTI_RAW"""
REMOTE_END_BVEC= "?overwrite=delete&format=BVEC&content=BVEC"""
REMOTE_END_BVAL= "?overwrite=delete&format=BVAL&content=BVAL"""
project = 'DATA'

def compare_nifti_imported():
    #list of nifti resource
    command = "SELECT public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id FROM public.xnat_abstractresource,public.xnat_imagescandata WHERE public.xnat_abstractresource.label = 'NIFTI' AND public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id =public.xnat_imagescandata.xnat_imagescandata_id "
    nifti_resources = svr.connect_to_db(command, True)
    
    #list of scans:
    command = "SELECT public.xnat_imagescandata.xnat_imagescandata_id FROM public.xnat_imagescandata,public.xnat_subjectassessordata,public.xnat_subjectdata WHERE  public.xnat_imagescandata.type NOT SIMILAR TO '%OTHER%'   AND public.xnat_imagescandata.type NOT SIMILAR TO '%SPINE%'    AND public.xnat_imagescandata.type NOT SIMILAR TO '%SCOUT%'  AND public.xnat_subjectassessordata.id =  public.xnat_imagescandata.image_session_id AND public.xnat_subjectassessordata.subject_id = public.xnat_subjectdata.id AND public.xnat_subjectdata.project = 'DATA'   "
    scans_of_interest = svr.connect_to_db(command, True)
    
    #missing_nitfi_list = list(set(scans_of_interest) - set(nifti_resources)) # set.difference ???   set(nifti_resources) not & set(scans_of_interest)
    s = set(nifti_resources)
    missing_nifti_list = [str(x) for x in scans_of_interest if x not in s]
    
    
    
    print len(missing_nifti_list), ' scans to upload'
    #print nifti_resources 
    print missing_nifti_list
    return  missing_nifti_list

def nifti_resource_exist( imagescan_id ):
    command = "SELECT COUNT(*) FROM public.xnat_abstractresource,public.xnat_imagescandata WHERE public.xnat_abstractresource.label = 'NIFTI' AND public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id =public.xnat_imagescandata.xnat_imagescandata_id AND public.xnat_imagescandata.id = '{}'".format(imagescan_id)

    #command = "SELECT COUNT(*) FROM public.xnat_abstractresource,public.xnat_imagescandata WHERE public.xnat_abstractresource.label = 'NIFTI' AND CAST(public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id AS character varying(255)) =public.xnat_imagescandata.id AND public.xnat_imagescandata.id = '{}'".format(imagescan_id)
    results = svr.connect_to_db(command, True)
    for row in results:
        count = str(row[0])
        if count == '0':
            #print "Scan not uplaoded....", count, " ",imagescan_id
            return False
        else:
            #print 'Scan uplaoded...', count, " ", imagescan_id
            return True

def main(argv):  
  
    """Upload NIFTIS that have been converted but not imported into XNAT due to REST errors     """
    svr.configure_server()   
    archive = svr.xnat_archive_path
    zip_path = svr.seven_zip_path
    jsession = ''
    if len(sys.argv) >1:
        if 'jsession' in str(sys.argv[1]):
            jsession=str(sys.argv[1]).replace('jsession=','')
            
            print 'Using jsession:', jsession
    else:
        jsession = svr.get_jsession()
        print 'Creating jsession:', jsession
    
    #missing nitis is a set, not list
    missing_niis = compare_nifti_imported()    
    s = frozenset(missing_niis)
    #print  missing_niis #s
        
        
    command = "SELECT public.xnat_mrscandata.xnat_imagescandata_id, public.xnat_imagescandata.image_session_id, public.xnat_imagescandata.id, public.xnat_imagesessiondata.uid, public.xnat_imagescandata.type,public.xnat_subjectassessordata.subject_id,public.xnat_imagesessiondata_meta_data.insert_date FROM public.xnat_mrscandata, public.xnat_imagescandata, public.xnat_imagesessiondata, public.xnat_imagesessiondata_meta_data, public.xnat_subjectassessordata, public.xnat_subjectdata WHERE  public.xnat_imagescandata.type NOT SIMILAR TO '%OTHER%'   AND public.xnat_imagescandata.type NOT SIMILAR TO '%SPINE%'    AND public.xnat_imagescandata.type NOT SIMILAR TO '%SCOUT%' AND  public.xnat_mrscandata.xnat_imagescandata_id=public.xnat_imagescandata.xnat_imagescandata_id AND public.xnat_imagescandata.image_session_id=public.xnat_imagesessiondata.ID AND public.xnat_subjectassessordata.id =  public.xnat_imagescandata.image_session_id AND public.xnat_imagesessiondata_meta_data.meta_data_id =  public.xnat_imagesessiondata.imagesessiondata_info AND public.xnat_subjectassessordata.subject_id = public.xnat_subjectdata.id AND public.xnat_subjectdata.project = '{}';".format(project)
    
    print command
    results = svr.connect_to_db(command, True)

    fe = svr.make_file('','upload_niftis', 'log')      
    number_scans = len(results)
    num = 1
    testn = 0
           
    for row in results:
        try:
            image_scandata_id = row[0]
            SESSION_ID = row[1]
            SCAN_ID = str(row[2])           
            SESSION_LABEL = row[3].replace(".","")
            type = row[4].replace(".","")
            subjectID = row[5].replace(" ","")
          
            
            
            testn = testn + 1
            if testn > 10:
                num = num + 10
                testn = 1
                print "test" + str(testn) + "  num:" + str(num) + " out of " + str(number_scans)    + ' image_Scan_id:' +str(image_scandata_id)
            
            #already checks in call
            #if not nifti_resource_exist( image_scandata_id ):
            
           # print str(s[10000]) + ' ' + str(image_scandata_id)
            #thing = [str(image_scandata_id),]
            #if str(thing) in s:           
            #if str(image_scandata_id) in missing_niis:
            OUT_FOLDER = archive + "DATA\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\NIFTI\\"
            print OUT_FOLDER
            if os.path.isdir(OUT_FOLDER):
                nifti_files = os.listdir(OUT_FOLDER)
                for nifti_file in nifti_files:
                    if 'nii.gz' in nifti_file:
                        try:
                            #zipped??
                            REMOTE = "\"/data/archive/experiments/{}/scans/{}/resources/NIFTI/files/{}{}\"".format(SESSION_ID,SCAN_ID,nifti_file , REMOTE_END)
                            local = " -local {}{}".format(OUT_FOLDER, nifti_file)
                            upload_file  = svr.xnat_rest + "  -host "+ svr.xnat_host +" -u import -p tropmi -m PUT  -remote " + REMOTE + local
                            #upload_file = svr.xnat_rest(jsession, 'PUT',  REMOTE + local)  
                            print upload_file 
                            fe.write(upload_file)
                            subprocess.call(upload_file,shell=False)
                            imported = True
                        except Exception as detail:
                            err = "error uploading: " + SCAN_ID+ " " +SESSION_ID+ " " +SESSION_LABEL + "\n"
                            print err + type + str(detail) 
                            fe.write(err + type + str(detail) )
                            fe.write( "\n")
            else:
                err = "dcm2nii: " + SCAN_ID+ " " +SESSION_ID+ " " +SESSION_LABEL  + " "
                print err + type 
                fe.write(err + type  )
                fe.write( "\n") 
            #makes quicker searches
            #missing_niis.discard(image_scandata_id)
            
        except Exception as detail:
            err = "error  "
            print err +  str(detail) 
            fe.write(err +  str(detail) )
            fe.write( "\n")
            
    print len(missing_niis), 'missign nii scans left'
    fe.close()
    
if __name__ == "__main__":
    main(sys.argv[1:])

    
                 
             
