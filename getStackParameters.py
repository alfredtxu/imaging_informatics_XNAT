import sys
import pydicom
import os
import csv
import subprocess
import fileinput
import datetime
import time
import shutil
import psycopg2
#from dax import XnatUtils 

#import _server_utils
#configure_server()
#csv_file = log_folder + 'scans_missing_voxsizes.csv'
#sql_file = log_folder + 'voxel_sizes.sql'

drive = "E"
project = "DATA"
threshold = 10;

#subject_counter=0
#IN_FOLDER=args[0]
#PROJECT = args[1]
#PREARCHIVE_PATH

    
  
def main(argv):   

    server_utils.configure_server()   
    archive = server_utils.xnat_archive_path
    
        

    
    fov_rows = 0;
    fov_columns = 0;
    fs = server_utils.make_file('stack_params','stack', 'sql') 
    fe = server_utils.make_file('stack_params','stack_errors', 'log')  

    # SELECT  public.xnat_experimentdata.ID, public.xnat_experimentdata.label, public.xnat_subjectassessordata.subject_id FROM public.xnat_experimentdata, public.xnat_subjectassessordata WHERE public.xnat_subjectassessordata.id = public.xnat_experimentdata.ID;
    
    command = "SELECT public.xnat_mrscandata.xnat_imagescandata_id, public.xnat_imagescandata.image_session_id, public.xnat_imagescandata.id, public.xnat_imagesessiondata.uid FROM public.xnat_mrscandata, public.xnat_imagescandata, public.xnat_imagesessiondata WHERE public.xnat_mrscandata.xnat_imagescandata_id=public.xnat_imagescandata.xnat_imagescandata_id AND public.xnat_mrscandata.parameters_voxelres_x is NULL AND public.xnat_imagescandata.image_session_id=public.xnat_imagesessiondata.ID;"
    results = server_utils.connect_to_db(command, True)
    
    for row in results:
        SCAN_ID = row[0]
        SESSION_ID = row[1]
        SESSION_LABEL = row[2]
        image_scandata_id = row[3]
        
        print "sql rows: " +SCAN_ID +" "+ SESSION_ID+" "+ SESSION_LABEL +" "+ image_scandata_id

       
        if SCAN_ID != "0" and SCAN_ID != "99":
            #IN_FOLDER = archive+project+"\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\"
            IN_FOLDER = archive+project+"\\arc001\\"+SESSION_LABEL+"\\scans\\"+SCAN_ID+"\\DICOM\\"
            print IN_FOLDER
            fov_value_one_row = 0
            fov_value_two_row = 0
            fov_value_one_colum = 0
            fov_value_two_column = 0
            fov_count_one = 0
            fov_count_two = 0
            file_count = 0
            pixelSpacing =""
            sliceThickness=""
            for file in os.listdir(IN_FOLDER):


                print file
                if file.endswith(".dcm"):
                    #read dicome header
                    try:
                        dataset = pydicom.read_file(IN_FOLDER + file)
                        scanID = dataset.SeriesNumber
                        rows = dataset.Rows
                        columns = dataset.Columns
                       
                        
                        if file_count==0:
                            sliceThickness = dataset.SliceThickness
                            pixelSpacing = str(dataset.PixelSpacing).replace("[","").replace("]","").replace("'","")
                            # imageOrientation = dataset.ImageOrientation
                            fov_value_one_column = columns
                            fov_value_one_rows = rows
                            fov_count_one+= 1
                        else:
                            if rows == fov_value_one_rows:
                                fov_count_one+= 1 
                            elif fov_value_two_column == columns:
                                fov_count_two+= 1
                            
                            elif fov_value_two_column == 0 and rows !=   fov_value_one_column:
                                fov_value_two_column = columns
                                fov_value_two_rows = rows
                                fov_count_two+= 1
                            else:
                                raise Exception                                    
                        print str(file_count) + " " + str(fov_value_one_column)+" "+str(fov_count_one)+ " " + str(fov_value_two_column)+" "+str(fov_count_two)
                        file_count+= 1        
                                     
                        pass
                    except Exception as detail:
                        fe.write("unable to process: " + SCAN_ID+ " " +SESSION_ID+ " " +SESSION_LABEL + "\n")
                        fe.write(detail)
                        fe.write( "\n")
                        print "error " + SESSION_LABEL, detail
                    #break
                    
            if fov_count_one < 2:
                if fov_count_two > threshold:
                      fov_columns = fov_value_two_column
                      fov_rows = fov_value_two_rows
            if fov_count_two < 2:
                if fov_count_one > threshold:
                      fov_columns = fov_value_one_column
                      fov_rows = fov_value_one_rows
            else:
                fe.write("unable to process: " + SCAN_ID+ " " +SESSION_ID+ " " +SESSION_LABEL + "\n")
                pass
             
                      
                
            
            command = "UPDATE public.xnat_mrscandata SET parameters_voxelres_x={} ,parameters_voxelres_y={} ,parameters_voxelres_z={},parameters_fov_x={},parameters_fov_y={},  WHERE public.xnat_mrscandata.xnat_imagescandata_id = {}".format(pixelSpacing.split(",")[0], pixelSpacing.split(",")[1], sliceThickness, fov_rows,fov_columns, image_scandata_id )  
            
            #server_utils.connect_to_db(command, True)
            print command
            
            fs.write(command + "\n")
            
    fe.close()
    fs.close()
                      
          
if __name__ == "__main__":
    main(sys.argv[1:])
             
   

