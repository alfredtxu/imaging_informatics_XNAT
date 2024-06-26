Param(



)

$DRIVE = "E:"
$PROJECT = "DATA"
$Logfile = "$DRIVE\LOGS\fix_prearchive.log"

Function LogWrite
{
   Param ([string]$logstring)
   Add-content $Logfile -value $logstring
}



$NIFTI_FOLDER= "$DRIVE\NIFTI_DATA\$SESSION_ID"
$REST_CLIENT="$DRIVE\XNAT\xnat_tools\XNATRestClient.exe"
$XNAT_HOST="http://localhost:8080/brcii"

$7ZIP_PATH ="$DRIVE\XNAT\7-Zip\7z.exe"
$DCMNII_PATH="$DRIVE\SOFTWARE\mricron\dcm2nii.exe"
$INI_FILE="$DRIVE\SOFTWARE\mricron\dcm2nii.ini"

$CSVFILE = "$DRIVE\LOGS\prearchive_list.csv"

$DRIVE

$proc = Start-Process -FilePath $REST_CLIENT  -Argument " -host $XNAT_HOST -u import -p tropmi -remote ""/data/prearchive/projects/DATA?format=csv""  -m GET >> $CSVFILE"   -NoNewWindow  -PassThru -Wait -RedirectStandardOutput $CSVFILE 
 
  LogWrite "$REST_CLIENT -host $XNAT_HOST -u import -p tropmi -remote ""/data/prearchive/projects/$PROJECT?format=csv""  -m GET >> $CSVFILE" 
  
  $data = Import-Csv  $CSVFILE |  ForEach-Object {
         

         
		$URL = $_.url    
        $proc = Start-Process -FilePath $REST_CLIENT  -Argument " -host $XNAT_HOST -u import -p tropmi -m POST -remote ""/data/services/archive?src=$URL&overwrite=append"" " -NoNewWindow  -PassThru -Wait
        LogWrite "$REST_CLIENT -host $XNAT_HOST -u import -p tropmi -m POST -remote ""/data/services/archive?src=$URL&overwrite=append"""     

      
      
    } 



