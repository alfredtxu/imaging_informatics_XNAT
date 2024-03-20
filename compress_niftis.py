import os
import csv
import subprocess
import fileinput
import datetime
import time
import string
#from dax import XnatUtils

drive = "e"
csv_file = drive + ":\\logs\\scanlist_full2.csv"
zip_path =drive + ":\\SOFTWARE\\7-Zip\\7z.exe"
#zip_path =drive + ":\\XNAT\\7-Zip\\7z.exe"

REMOTE = "/data/archive/experiments"
REMOTE_END= "?extract=true&overwrite=delete&format=NIFTI&content=NIFTI_RAW"""
REST_CLIENT= drive + ":\\SOFTWARE\\xnat_tools\\XNATRestClient.exe"
XNAT_HOST="http://localhost:8080/brcii"
XNAT_PUT = REST_CLIENT + "  -host "+XNAT_HOST +" -u import -p tropmi -m PUT -remote "  
XNAT_POST = REST_CLIENT + "  -host "+XNAT_HOST +" -user_session AD26156A2BE2C344D687A2490C0C18DA -m POST -remote "  


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
            SCAN_ID = row['ID']
            SESSION_ID = row['session']
            SESSION_LABEL = row['labels']
            OUT_FOLDER = drive +":\\XNAT_DATA\\archive\\DATA\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\NIFTI\\"
            CATALOG_NAME = os.path.join(OUT_FOLDER,"catalog.xml")
            NIFTI_CATALOG_NAME = os.path.join(OUT_FOLDER,"NIFTI_catalog.xml")
          
            
            print OUT_FOLDER
            print CATALOG_NAME 
            
            if SCAN_ID != "0" and SCAN_ID != "99":
                #gzip_nii(OUT_FOLDER)??
        
                
                if os.path.isdir( OUT_FOLDER):
                    print "-------------------------------------------"
                    print "----------" + OUT_FOLDER + "--------------"
                    
                    for root, dirs, files in os.walk(OUT_FOLDER):
                        # st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%dT%H:%M:%S.%f')
                        for file in files:
 
                               
                            if file.endswith(".nii"):
                            
                                print zip_path + " a -tgzip  " + OUT_FOLDER+file +".gz " + OUT_FOLDER+file
                                suc = subprocess.check_call(zip_path + " a -tgzip  " + OUT_FOLDER+file +".gz " + OUT_FOLDER+file)
                                if suc==0:
                                    os.remove(OUT_FOLDER+file)
                                    if os.path.exists(CATALOG_NAME):
                                        for line in fileinput.input(CATALOG_NAME, inplace=True):
                                            print line.replace(file+"\"", file + ".gz\"")
                                    if os.path.exists(NIFTI_CATALOG_NAME):
                                        for line in fileinput.input(NIFTI_CATALOG_NAME, inplace=True):
                                            print line.replace(file, file + ".gz")
                                        
                      
                       
                            
                           
                        refresh_catalog = XNAT_POST + "/data/services/refresh/catalog?resource=/archive/experiments/"+SESSION_ID+"/scans/"+SCAN_ID+"/resources/NIFTI"        
                        print refresh_catalog
                        subprocess.call(refresh_catalog)
                

search_archive()

    
                 
             
