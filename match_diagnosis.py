import sys
import os
import csv
import subprocess
import fileinput
import datetime
import time
import shutil
import server_utils as svr


sessions_from_subject_label = 'SELECT COUNT(public.xnat_subjectassessordata.id)  FROM public.xnat_subjectdata, public.xnat_subjectassessordata,public.xnat_mrsessiondata  WHERE public.xnat_mrsessiondata.id = public.xnat_subjectassessordata.id AND public.xnat_subjectassessordata.subject_id = public.xnat_subjectdata.id AND public.xnat_subjectdata.label LIKE '

sessions = 'SELECT public.xnat_subjectdata.label, public.xnat_subjectassessordata.id, public.xnat_subjectassessordata.subject_id FROM public.xnat_subjectdata, public.xnat_subjectassessordata  WHERE public.xnat_subjectassessordata.subject_id = public.xnat_subjectdata.id '




def dob_convert(dob):
    dob = dob.replace(' 00:00:00','')
    dates = dob.split('/')
    
    formated_date = dates[2] + dates [1] + dates[0]
    return formated_date

def main(argv): 
    """Match Diaghnosis: From neuropathology reports, matches with XNAT subjects and writes to brc_neuropathology the diagnosis fields.    """
    svr.configure_server()   
    fsql = svr.make_file('metadata', 'neuropathology', 'sql')
    command = 'SELECT public.xnat_subjectdata.label, public.xnat_subjectdata.id FROM public.xnat_subjectdata'
    subject_list = svr.connect_to_db(command, True)
    
    
    csv_file = str(sys.argv[1])
    diagnosis_text_search = str(sys.argv[2])
    print 'Diagnosis search:', diagnosis_text_search
    
        
    
    matches = 0
    total_sessions = 0
    rows = 0
    
    print 'Searching for subject matches...'
    try:
        with open(csv_file) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                first_name = row['firstname'] 
                last_name = row['lastname']
                name = svr.dcm_tidy_string(last_name + "_" + first_name)
                
                hospital_number = svr.dcm_tidy_string(row['medrec_num'])
                dob = row['date_of_birth']
                #dob YYYYMMDD
                try:
                    subject_id_dob = dob_convert(dob)      
                except Exception as detail:
                    print 'cannot complete this subject:' + name
                    #fsql.close();
                    pass
                
                #print name +  '_' + subject_id_dob +  '_' + hospital_number

                
                #specnum_formatted,lastname,firstname,date_of_birth,gender,name,name,text_data,specimen_id,texttype_id,datetime_taken,datetime_rec,accession_date,prettyprint_name,first_signout,last_signout,prettyprint_name,supp_signout_1,supp_signout_2,supp_signout_3,supp_signout_4,supp_signout_5,medrec_num,numwheel_id,specnum_year,specnum_num,part_inst,part_designator,part_description
                id = svr.match_subject(hospital_number, name, subject_id_dob, subject_list)
                rows = rows+1
                if not 'NO MATCH' in id:
                
                    command = sessions_from_subject_label + '\'%'+id+ '%\''
                    #print command
                    diagnosis = row['text_data'].replace('\n',' ').replace("'",'"')
                    diagnosis_short = row['part_description'].replace('\n',' ').replace("'",'"')
                    date_taken = row['datetime_taken'].split(' ')
                    date_rec = row['datetime_rec'].split(' ')
                    
                    
                    
                    diagnosis_text_search = diagnosis_text_search.lower()
                    diagnosis_short = diagnosis_short.lower()
                    diagnosis =  diagnosis.lower()
                    if 'none' in diagnosis_text_search.lower() or diagnosis_text_search in diagnosis_short or diagnosis_text_search in diagnosis:
                        matches = matches +1 
                        write_np = "INSERT INTO brc.brc_neuropathology  VALUES ('{}','{}','{}','{}','{}' );".format(date_taken[0], date_rec[0], id, diagnosis, diagnosis_short)
                        fsql.write(write_np + '\n')
                        #svr.connect_to_db(write_np,False)
                    
                        sessions_count = svr.connect_to_db(command, True)
                        for ses in sessions_count:
                            total_sessions = total_sessions + ses[0]

                    #print id + " " + str(matches) + " " + str(total_sessions)
    except Exception as detail:
        fsql.close()
    print 'Numebr of Matches:', str(matches), ' out of ', str(rows)
    print 'Total Sesisons:', str(total_sessions)
    fsql.close()
            

    
    
    
    


if __name__ == "__main__":
    main(sys.argv[1:])

