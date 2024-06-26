Param(
  [string]$CSV_FILE
)
$DRIVE = "E:"
$PROJECT = "DATA"
$Logfile = "$DRIVE\LOGS\missing_nitis_16oct.csv"

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

$7ZIP_PATH ="$DRIVE\SOFTWARE\7-Zip\7z.exe"

$SCANFILE = "$DRIVE\LOGS\$CSV_FILE"

LogWrite " ID, session, label "

	$data = Import-Csv  $SCANFILE |  ForEach-Object {
	
	    $SCAN_ID = $_.ID -replace " ",""     
        $SESSION_ID = $_.session  -replace " ",""     
        $SESSION_LABEL = $_.labels -replace " ",""  
        $CATALOG_NAME
        $REMOTE = "/data/archive/experiments/$SESSION_ID/scans/$SCAN_ID/resources/NIFTI/files/nifti.zip$REMOTE_END"
    
 		    $OUT_FOLDER = "$DRIVE\XNAT_DATA\archive\DATA\arc001\$SESSION_LABEL\scans\$SCAN_ID\NIFTI\"
	  	  $IN_FOLDER =  "$DRIVE\XNAT_DATA\archive\DATA\arc001\$SESSION_LABEL\scans\$SCAN_ID"
        
        $CATALOG_NAME = "$DRIVE\XNAT_DATA\archive\DATA\arc001\$SESSION_LABEL\scans\$SCAN_ID\NIFTI\NIFTI_catalog.xml"
        

    
      if ($SCAN_ID -eq "99" -Or $SCAN_ID -eq "0" -Or $SCAN_ID.length  -gt 3){
      
      } else{
      
                if(Test-Path $CATALOG_NAME){	
                 Write-Host " catalog name: $CATALOG_NAME "
                    cd $OUT_FOLDER
                      Get-ChildItem   -Filter *.nii | ForEach-Object{
                            $proc = Start-Process -FilePath $7ZIP_PATH -Argument "a -tgzip $_.gz $_ "  -NoNewWindow -PassThru -Wait 
                            if($proc.ExitCode -eq 0){
                                Write-Host "rewriting catalog"
                                (gc $CATALOG_NAME ) -replace ".nii", ".nii.gz" 
                                Remove-Item $_
                        }
                      }
                  
                }
           
            } 
   
       }



 






 
 