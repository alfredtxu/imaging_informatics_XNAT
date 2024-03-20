import package_loader


import os
import csv
import subprocess
import fileinput
import datetime
import time
import string
#from dax import XnatUtils

drive = "d"
csv_file = drive + ":\\logs\\scanlist.csv"
#zip_path =drive + ":\\SOFTWARE\\7-Zip\\7z.exe"
zip_path =drive + ":\\XNAT\\7-Zip\\7z.exe"

REMOTE = "/data/archive/experiments"
REMOTE_END= "?extract=true&overwrite=delete&format=NIFTI&content=NIFTI_RAW"""
REST_CLIENT= drive + ":\\SOFTWARE\\xnat_tools\\XNATRestClient.exe"
XNAT_HOST="http://localhost:8080/brcii"
XNAT_PUT = REST_CLIENT + "  -host "+XNAT_HOST +" -u import -p tropmi -m PUT -remote "  
XNAT_POST = REST_CLIENT + "  -host "+XNAT_HOST +" -u import -p tropmi -m POST -remote "  


REMOTE_END_BVEC= "?overwrite=delete&format=BVEC&content=BVEC"""
REMOTE_END_BVAL= "?overwrite=delete&format=BVAL&content=BVAL"""
ARCHIVE =  "d:\\XNAT_DATA\\archive\\DATA\\arc001\\"

DCMNIIX_PATH=drive + ":\\SOFTWARE\\mricrogl\\dcm2niix.exe"
DCMNII_PATH=drive + ":\\SOFTWARE\\mricron\\dcm2nii.exe"
INI_FILE=drive + ":\\SOFTWARE\\mricron\\dcm2nii.ini"


#host = "http://localhost:8080/brcii"
#user = "brcii"
#pwd = "brcii"
#xnat = XnatUtils.get_interface(host, user, pwd)

#xnat.disconnect()
  
def search_archive(filter="null", action="null"):
    global xnat
    with open(csv_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            SCAN_ID = row['id']
            SESSION_ID = row['session']
            SESSION_LABEL = row['labels']
            OUT_FOLDER = drive +":\\XNAT_DATA\\archive\\DATA\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\NIFTI\\"
            IN_FOLDER = drive +":\\XNAT_DATA\\archive\\DATA\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\"
            ZIP_NAME = OUT_FOLDER + "nifti.zip"
            CATALOG_NAME = os.path.join(OUT_FOLDER,"catalog.xml")
            NIFTI_CATALOG_NAME = os.path.join(OUT_FOLDER,"NIFTI_catalog.xml")
          
            
            print OUT_FOLDER
            print CATALOG_NAME 
            
            if SCAN_ID != "0" and SCAN_ID != "99":
                #gzip_nii(OUT_FOLDER)??
        
                
                if os.path.isdir( OUT_FOLDER):
                    print "-------------------------------------------"
                    print "----------" + OUT_FOLDER + "--------------"
                    print "-------------------------------------------"
                    for root, dirs, files in os.walk(OUT_FOLDER):
                        PAD = True    
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
                                    
                      
                        for file in files:                
                            if file.endswith(".bvec"):
                                print "----------------------------------"
                                print "BVEC found " + SESSION_ID + " " + SCAN_ID
                                print XNAT_PUT +  "\"/data/archive/experiments/" + SESSION_ID+"/scans/" +SCAN_ID +"/resources/BVEC/files/" + file +REMOTE_END_BVEC+ "\" -local "+ OUT_FOLDER+file
                                try:
                                    subprocess.call(XNAT_PUT +  "\"/data/archive/experiments/" + SESSION_ID+"/scans/" +SCAN_ID +"/resources/BVEC/files/" + file + REMOTE_END_BVEC+ "\"  -local "+ OUT_FOLDER+file )                                                          
                                except: 
                                    print "oh well"
                            
                            if file.endswith(".bval"):  
                                print "------------------------------------------------------"
                                print "BVAL found " + SESSION_ID + " " + SCAN_ID +"   pad is:" + str(PAD) 
                                print "-----------------------------------------------------"
                                print XNAT_PUT +  "\"/data/archive/experiments/" + SESSION_ID+"/scans/" +SCAN_ID +"/resources/BVAL/files/" + file +REMOTE_END_BVAL+ "\"  -local "+ OUT_FOLDER+file
                                try:
                                    subprocess.call(XNAT_PUT +  "\"/data/archive/experiments/" + SESSION_ID+"/scans/" +SCAN_ID +"/resources/BVAL/files/" + file + REMOTE_END_BVAL+"\"  -local "+ OUT_FOLDER+file )            
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
                                        
                                        
                           
                       
                        refresh_catalog = XNAT_POST + "/data/services/refresh/catalog?resource=/archive/experiments/"+SESSION_ID+"/scans/"+SCAN_ID+"/resources/NIFTI"        
                        print refresh_catalog
                        subprocess.call(refresh_catalog)
                
                if os.path.exists(ZIP_NAME):
                    os.remove(ZIP_NAME)
                    
search_archive()

    
                 
             
