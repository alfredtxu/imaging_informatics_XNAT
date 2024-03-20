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
import math
#from dax import XnatUtils 




    
def main(argv):
    """ Extracts line from one file using a serach terma nd writes these lines to another fil: inf_file out_file serach_text """
    in_file  = str(sys.argv[1]).replace(" ","")
    out_file  = str(sys.argv[2]).replace(" ","")
    search_text  = str(sys.argv[3])
    print "Searching for ", search_text ," in ", in_file, " writing to ", out_file
    f = open(in_file, 'r' )
    f_out = open(out_file, 'w' )
    for line in f:
        if search_text in line:
            f_out.write(line)
    f.close()
    f_out.close()


        

if __name__ == "__main__":
    main(sys.argv[1:])