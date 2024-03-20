E:

E:\\XNAT\\xnat_tools\\XNATRestClient -host http://localhost:8080/brcii -u import -p tropmi -m POST -remote /data/JSESSION/ > temp.txt
set /p JSESSION=<temp.txt
Powershell.exe -executionpolicy remotesigned -File  E:\SOFTWARE\Scripts\dcm2nii_import_sql.ps1 %JSESSION%