Param(
  [string]$CSV_FILE
)
$DRIVE = "E:"
$PROJECT = "DATA"
$Logfile = "$DRIVE\LOGS\small_zips_niftis_16oct.csv"

Function LogWrite
{
   Param ([string]$logstring)

   Add-content $Logfile -value $logstring
}



$SCANFILE = "$DRIVE\LOGS\$CSV_FILE"

LogWrite " ID, session, labels"

	$data = Import-Csv  $SCANFILE |  ForEach-Object {
	
	    $SCAN_ID = $_.ID -replace " ",""     
        $SESSION_ID = $_.session  -replace " ",""     
        $SESSION_LABEL = $_.labels -replace " ",""  

        
        $ZIP_NAME = "$DRIVE\XNAT_DATA\archive\DATA\arc001\$SESSION_LABEL\scans\$SCAN_ID\NIFTI\nifti.zip"
    
     if($SCAN_ID -eq "0" -Or $SCAN_ID -eq "99"){
      
      } else{
     
            if(Test-Path $ZIP_NAME){	
                      if((Get-Item $ZIP_NAME).length -lt 2kb){
                       Write-Host "  $SESSION_LABEL , $SCAN_ID , $SESSION_ID   "
                            LogWrite "  $SCAN_ID ,  $SESSION_ID, $SESSION_LABEL"
                            Remove-Item $ZIP_NAME
                      }
                } 
      
      }
              
   }
       



 






 
 