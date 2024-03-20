Powershell.exe -executionpolicy remotesigned -File  E:\SOFTWARE\Scripts\find_missing_niftis_import.pS1 scanlist_full.csv nifti_todo_17nov.csv

E:\\XNAT\\xnat_tools\\XNATRestClient -host http://localhost:8080/brcii -u import -p tropmi -m POST -remote /data/JSESSION/ > temp.txt
set /p JSESSION=<temp.txt

Powershell.exe -executionpolicy remotesigned -File  E:\SOFTWARE\Scripts\dcm2niix_upload_missing.ps1 %JSESSION% nifti_todo_17nov2.csv


