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
    missing_nifti_list = [x for x in scans_of_interest if x not in s]
    print missing_nifti_list
    return  missing_nifti_list

def nifti_resource_exist( imagescan_id ):
    command = "SELECT COUNT(*) FROM public.xnat_abstractresource,public.xnat_imagescandata WHERE public.xnat_abstractresource.label = 'NIFTI' AND public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id =public.xnat_imagescandata.xnat_imagescandata_id AND public.xnat_imagescandata.id = '{}'".format(imagescan_id)

    #command = "SELECT COUNT(*) FROM public.xnat_abstractresource,public.xnat_imagescandata WHERE public.xnat_abstractresource.label = 'NIFTI' AND CAST(public.xnat_abstractresource.xnat_imagescandata_xnat_imagescandata_id AS character varying(255)) =public.xnat_imagescandata.id AND public.xnat_imagescandata.id = '{}'".format(imagescan_id)
    results = svr.connect_to_db(command, True)
    for row in results:
        count = str(row[0])
        if count == 0:
            print "Scan not uplaoded...."
            return False
        else:
            return True

def main(argv):  
  
    
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

    missing_niis = compare_nifti_imported()    
        
        
    command = "SELECT public.xnat_mrscandata.xnat_imagescandata_id, public.xnat_imagescandata.image_session_id, public.xnat_imagescandata.id, public.xnat_imagesessiondata.uid, public.xnat_imagescandata.type,public.xnat_subjectassessordata.subject_id,public.xnat_imagesessiondata_meta_data.insert_date, public.xnat_subjectdata.project  FROM public.xnat_mrscandata, public.xnat_imagescandata, public.xnat_imagesessiondata, public.xnat_imagesessiondata_meta_data, public.xnat_subjectassessordata, public.xnat_subjectdata WHERE public.xnat_mrscandata.xnat_imagescandata_id=public.xnat_imagescandata.xnat_imagescandata_id AND public.xnat_imagescandata.image_session_id=public.xnat_imagesessiondata.ID AND public.xnat_subjectassessordata.id =  public.xnat_imagescandata.image_session_id AND public.xnat_imagesessiondata_meta_data.meta_data_id =  public.xnat_imagesessiondata.imagesessiondata_info AND public.xnat_subjectassessordata.subject_id = public.xnat_subjectdata.id AND public.xnat_subjectdata.project = '{}';".format(project)
    
    print command
    results = svr.connect_to_db(command, True)

    fe = svr.make_file('','multisort_errors', 'log')      
    num_of_scans = len(results)
    num = 0       
    for row in results:
        try:
            num = num +1
            SCAN_ID = str(row[2])
            SESSION_ID = row[1]
            image_scandata_id = row[0]
            SESSION_LABEL = row[3].replace(".","")
            type = row[4].replace(".","")
            subjectID = row[5].replace(" ","")
            proj = row[7].replace(" ","")
           
            
            if svr.is_scan_of_interest(type, SESSION_ID, SCAN_ID):
                OUT_FOLDER = archive + "DATA\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\NIFTI\\"
                IN_FOLDER = archive + "DATA\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\"
                ZIP_NAME = OUT_FOLDER + "nifti.zip"
                CATALOG_NAME = os.path.join(OUT_FOLDER,"catalog.xml")
                NIFTI_CATALOG_NAME = os.path.join(OUT_FOLDER,"NIFTI_catalog.xml")          
                
                print OUT_FOLDER
                #print CATALOG_NAME 
            
                        
                if os.path.isdir( OUT_FOLDER):
                    print "-------------------------------------------"
                    print "----------" + OUT_FOLDER + "--------------"
                    print "---------" + str(num) + " out of "+str(num_of_scans)+   "-------------------"
                    for root, dirs, files in os.walk(OUT_FOLDER):
                        PAD = True    
                        imported=False
                        # st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S.%f')
                        for file in files:
                           
                            if file.endswith("_1.nii"):
                                FILE_BASE  = OUT_FOLDER + string.replace(file, '_1.nii' , '')
                                PAD = False
                                if file.startswith("x"):
                                    print file
                                else:
                                    if os.path.exists(OUT_FOLDER + "x" + file ):
                                        #two copies of data!!!!!! one with x is corrcet, as has avergae removed
                                        FILE_BASE  = OUT_FOLDER + "x"+ string.replace(file, '_1.nii' , '') 
                                    
                            if file.endswith("_01.nii"):
                                FILE_BASE  = OUT_FOLDER + string.replace(file, '_01.nii' , '') 
                                if file.startswith("x"):
                                    print file
                                else:
                                    if os.path.exists(OUT_FOLDER + "x" + file ):
                                        #two copies of data!!!!!! one with x is corrcet as it has an "avergaed" file removed. See dcm2niix website for explanation
                                        FILE_BASE  = OUT_FOLDER + "x"+ string.replace(file, '_01.nii' , '') 
                               
                            if file.endswith(".nii"):
                            
                                print zip_path + " a -tgzip  " + OUT_FOLDER+file +".gz " + OUT_FOLDER+file
                                suc = subprocess.check_call(zip_path + " a -tgzip  " + OUT_FOLDER+file +".gz " + OUT_FOLDER+file)
                                #if suc==0:
                                #    os.remove(OUT_FOLDER+file)
                                if os.path.exists(CATALOG_NAME):
                                    for line in fileinput.input(CATALOG_NAME, inplace=True):
                                        print line.replace(file, file + ".gz")
                                if os.path.exists(NIFTI_CATALOG_NAME):
                                    for line in fileinput.input(NIFTI_CATALOG_NAME, inplace=True):
                                        print line.replace(file, file + ".gz")
                                    
                        #check to see if uploaded: ARGH MAYBE SHOULD GET LIST BEFOREHAND instea dof many small calls
                        if not imported:
                            if not nifti_resource_exist(image_scandata_id): 
                                nifti_files = os.listdir(OUT_FOLDER)
                                for nifti_file in nifti_files:
                                    if 'nii.gz' in nifti_file:
                                        try:
                                            #zipped??
                                            REMOTE = "\"/data/archive/experiments/{}/scans/{}/resources/NIFTI/files/{}{}\"".format(SESSION_ID,SCAN_ID,nifti_file , REMOTE_END)
                                            local = " -local {}\{}".format(OUT_FOLDER, nifti_file)
                                            upload_file  = svr.xnat_rest + "  -host "+ svr.xnat_host +" -user_session " + jsession  + " -m PUT  -remote " + REMOTE + local
                                            #upload_file = svr.xnat_rest(jsession, 'PUT',  REMOTE + local)  
                                            print upload_file 
                                            subprocess.call(upload_file,shell=False)
                                            imported = True
                                        except Exception as detail:
                                            err = "error uploading: " + SCAN_ID+ " " +SESSION_ID+ " " +SESSION_LABEL + "\n"
                                            print err + type + str(detail) 
                                            fe.write(err + type + str(detail) )
                                            fe.write( "\n")
                                       
                        
                        
                        
                        for file in files:                
                            if file.endswith(".bvec"):
                                print "----------------------------------"
                                print "BVEC found " + SESSION_ID + " " + SCAN_ID
                                
                                command =  "\"/data/archive/experiments/" + SESSION_ID+"/scans/" +SCAN_ID +"/resources/BVEC/files/" + file +REMOTE_END_BVEC+ "\" -local "+ OUT_FOLDER+file
                                subproc = svr.xnat_rest_call(jsession, 'PUT', command)
                                try:
                                    subprocess.call(subproc )                                                          
                                except: 
                                    print "oh well"
                            
                            if file.endswith(".bval"):  
                                print "------------------------------------------------------"
                                print "BVAL found " + SESSION_ID + " " + SCAN_ID +"   pad is:" + str(PAD) 
                                print "-----------------------------------------------------"
                                                                        
                                command = "\"/data/archive/experiments/" + SESSION_ID+"/scans/" +SCAN_ID +"/resources/BVAL/files/" + file +REMOTE_END_BVAL+ "\"  -local "+ OUT_FOLDER+file                                
                                subproc = svr.xnat_rest_call(jsession, 'PUT', command)                                  
                                print subproc
                                try:
                                    subprocess.call(subproc )            
                                except: 
                                    print "oh well"
                               
                                with open(OUT_FOLDER+file, "r") as filestream:
                                    #get bvals, and determine if bvals are in rows or columns
                                    #lines = sum(1 for _ in filestream)
                                    #print "lines:" +str(lines)
                                    #bvalues = []
                                    #if lines > 1:
                                    #    for line in filestream:
                                    #        bvalues[line] = str(line)
                                    #else:
                                    for line in filestream:
                                        bvalues = line.split(" ")
                                    print "number of bvalues:" + str(len(bvalues))
                                    # GE Signa Genesis correction    
                                    if len(bvalues)==2 and bvalues[1]==bvalues[0]:
                                        bvalues[1] = "0"
                                        
                                    COUNTER=1  
                                  
                                    # rewrite filenames in catalog.xml
                                    print " file base:" +FILE_BASE
                                    for bval in bvalues:
                                        print "-----------"
                                        print "COUNTER and bval:" + str(COUNTER).zfill(2) + " " +bval 
                                        print FILE_BASE + "_" +  str(COUNTER).zfill(2) + ".nii.gz"
                                        if len(bval.strip())>0:
                                            
                                            if os.path.exists(FILE_BASE + "_" + str(COUNTER).zfill(2) + "_" + bval.strip().zfill(4) +".nii.gz"):
                                               os.remove(FILE_BASE + "_" + str(COUNTER).zfill(2) + "_" + bval.strip().zfill(4) +".nii.gz")
                                            if os.path.exists(FILE_BASE + "_" + str(COUNTER) + "_" + bval.strip().zfill(4) +".nii.gz"):
                                               os.remove(FILE_BASE + "_" + str(COUNTER) + "_" + bval.strip().zfill(4) +".nii.gz")
                                            if PAD:
                                                file_name = FILE_BASE + "_" + str(COUNTER).zfill(2)
                                                os.rename(file_name + ".nii.gz" , file_name + "_" + bval.strip().zfill(4) +".nii.gz")
                                            else:
                                                file_name = FILE_BASE + "_" + str(COUNTER)
                                                os.rename(file_name + ".nii.gz" , FILE_BASE + "_" + str(COUNTER).zfill(2) + "_" + bval.strip().zfill(4) +".nii.gz") 
                                     
                                            if os.path.exists(CATALOG_NAME):
                                                for line in fileinput.input(CATALOG_NAME, inplace=True):
                                                    print file_name + ".nii.gz"
                                                    print line.replace(file_name + ".nii.gz",file_name + "_" + bval.strip().zfill(4) +".nii.gz")
                                            if os.path.exists(NIFTI_CATALOG_NAME):
                                                for line in fileinput.input(NIFTI_CATALOG_NAME, inplace=True):
                                                    print file_name + ".nii.gz"
                                                    print line.replace(file_name + ".nii.gz",file_name + "_" + bval.strip().zfill(4) +".nii.gz")
                                            COUNTER += 1
                                        
                                # if bvals, then must stack images. Use pynifti.
                                        
                                        
                        command = "/data/services/refresh/catalog?resource=/archive/experiments/"+SESSION_ID+"/scans/"+SCAN_ID+"/resources/NIFTI"      
                        refresh_catalog = svr.xnat_rest_call(jsession, POST, command)                              
               
                        print refresh_catalog
                        subprocess.call(refresh_catalog)
                
                if os.path.exists(ZIP_NAME):
                    os.remove(ZIP_NAME)
        except Exception as detail:
            print "Error:", detail
            fe.write( str(detail) , '\n' )
            fe.close()
            pass    
            
if __name__ == "__main__":
    main(sys.argv[1:])

    
                 
             
