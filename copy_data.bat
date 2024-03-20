e:

xcopy  E:\a_imagepool_mr\archive_21  E:\DATA_IMPORT\archive_21\ /E /S /Y /q
E:\\XNAT\\xnat_tools\\XNATRestClient -host http://localhost:8080/brcii -u import -p tropmi -m POST -remote /data/JSESSION/ > temp.txt
set /p JSESSION=<temp.txt


java -jar E:\SOFTWARE\Scripts\brc_import_2015.jar %JSESSION% nif E:\DATA_IMPORT\archive_21 DATA E




