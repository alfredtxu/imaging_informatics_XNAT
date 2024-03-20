import sys
import os
import csv
import subprocess

import fileinput
import datetime
import time
import shutil

import server_utils as svr
import sql_commands as sql_xnat



svr.configure_server() 
fe = svr.make_file('', 'err_', 'log') 

if __name__ == "__main__":


    fe.close()