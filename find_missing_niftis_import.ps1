Param(
  [string]$CSV_FILE,
  [string]$LogfileShort
)
$DRIVE = "E:"
$PROJECT = "DATA"
$Logfile = "$DRIVE\LOGS\$LogfileShort"

Function LogWrite
{
   Param ([string]$logstring)

   Add-content $Logfile -value $logstring
}



$SESSION_UID = $SESSION_UID -replace '\.','_'
$REMOTE = "/data/archive/experiments"
$REMOTE_END= "?extract=true&overwrite=delete&format=NIFTI&content=NIFTI_RAW"""

$REST_CLIENT="$DRIVE\SOFTWARE\xnat_tools\XNATRestClient.exe"
$XNAT_HOST="http://localhost:8080/brcii"

$SCANFILE = "$DRIVE\LOGS\$CSV_FILE"

LogWrite "ID,session,labels"

$LINE_NUMBER = 1

	$data = Import-Csv  $SCANFILE |  ForEach-Object {
	
	    $SCAN_ID = $_.ID -replace " ",""     
        $SESSION_ID = $_.session  -replace " ",""     
        $SESSION_LABEL = $_.labels -replace " ",""  
        $CATALOG_NAME
        $REMOTE = "/data/archive/experiments/$SESSION_ID/scans/$SCAN_ID/resources/NIFTI/files/nifti.zip$REMOTE_END"
    
 		$OUT_FOLDER = "$DRIVE\XNAT_DATA\archive\DATA\arc001\$SESSION_LABEL\scans\$SCAN_ID\NIFTI\"
	  	$IN_FOLDER =  "$DRIVE\XNAT_DATA\archive\DATA\arc001\$SESSION_LABEL\scans\$SCAN_ID"
        
        $CATALOG_NAME = "$DRIVE\XNAT_DATA\archive\DATA\arc001\$SESSION_LABEL\scans\$SCAN_ID\NIFTI\NIFTI_catalog.xml"
        

        $LINE_NUMBER++
      if ($SCAN_ID -eq "99" -Or $SCAN_ID -eq "0" -Or $SCAN_ID.length  -gt 2){
      
      } else{
      

                if(Test-Path $CATALOG_NAME ){	
                            
                      Write-Host " $SESSION_ID "
                      $LINE_NUMBER = 0;
                 
           
                } else if(Test-Path $OUT_FOLDER ){
                    Write-Host "-------------------------------------------------------"
                    Write-Host "  $SESSION_LABEL , $SCAN_ID , $SESSION_ID   $CATALOG_NAME"
                    LogWrite "  $SCAN_ID ,  $SESSION_ID, $SESSION_LABEL"
                    
                    # UPLOAD AS NO NIFTI_CATALOG
                    Get-ChildItem   -Filter *.nii.gz | ForEach-Object{
                        $REMOTE = "/data/archive/experiments/$SESSION_ID/scans/$SCAN_ID/resources/NIFTI/files/$_$REMOTE_END"
                        Write-Host " $REST_CLIENT -host $XNAT_HOST -user_session $JSESSION -m PUT -remote ""$REMOTE -local $OUT_FOLDER$_  "
                        $A = Start-Process -FilePath $REST_CLIENT  -Argument " -host $XNAT_HOST -user_session $JSESSION -m PUT -remote ""$REMOTE -local $OUT_FOLDER$_  " -NoNewWindow -PassThru -Wait 
                    }
                   
                }
   
       }
}


 






 
 