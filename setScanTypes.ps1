Param(
  [string]$SESSION_ID,
  [string]$SESSION_LABEL,
  [string]$SESSION_UID,
  [string]$JSESSION,
  [string]$REMOTE
)

$Logfile = "D:\emails\nifti_logs\nifti.log"

Function LogWrite
{
   Param ([string]$logstring)

   Add-content $Logfile -value $logstring
}




$SESSION_UID = $SESSION_UID -replace '\.','_'
$REMOTE_END= "?extract=true&overwrite=delete&format=NIFTI&content=NIFTI_RAW"""


$NIFTI_FOLDER= "D:\NIFTI_DATA\$SESSION_ID"
$REST_CLIENT="C:\XNAT\xnat_tools\XNATRestClient.exe"
$XNAT_HOST="http://localhost:8080/xnat/"

$7ZIP_PATH ="C:\Program Files\7-Zip\7z.exe"
$DCMNII_PATH="C:\XNAT\mricron\dcm2nii.exe"
$INI_FILE="C:\XNAT\mricron\dcm2nii.ini"

$CSVFILE = "D:\$SESSION_ID.csv"

d:

$proc = Start-Process -FilePath $REST_CLIENT  -Argument " -host $XNAT_HOST -user_session $JSESSION -remote ""/data/archive/experiments/$SESSION_ID/scans?columns=UID,ID,type,xnat:mrSessionData/fieldStrength&format=csv""  -m GET  "  -NoNewWindow -PassThru -Wait -RedirectStandardOutput D:\$SESSION_ID.csv 
 


 
 Write-Host "Start-Process -FilePath $REST_CLIENT  -Argument "" -host $XNAT_HOST -user_session $JSESSION -remote ""/data/archive/experiments/$SESSION_ID/scans?columns=UID,ID,type,xnat:mrSessionData/fieldStrength&format=csv""  -m GET -local $CSVFILE" ""
 
$data = Import-Csv  $CSVFILE |  ForEach-Object {
        $SCAN_ID = $_.ID         
        $UID = $_.UID -replace '\.','_'
        $TYPE = $_.type -replace '\.','_'
        $FIELDSTRENGTH = $_."xnat:mrSessionData/fieldStrength" -replace '\.','_'
       
       
       If($MODALITY -match 'T1' -Or $MODALITY -match 't1'){
       
             Write-Host  " uid: $UID"
            $SCAN_FOLDER =  "$SESSION_UID-$UID"
            $OUT_FOLDER = "D:\XNAT_DATA\storage\archive\DATA\arc001\$SESSION_LABEL\SCANS\$SCAN_ID\NIFTI\files\"
            $IN_FOLDER =  "D:\XNAT_DATA\storage\archive\DATA\arc001\$SESSION_LABEL\SCANS\$SCAN_FOLDER\"
            
            if(!(Test-Path -Path $IN_FOLDER)){  
                $IN_FOLDER =  "D:\XNAT_DATA\storage\archive\DATA\arc001\$SESSION_LABEL\SCANS\scans-$SCAN_FOLDER\"
                 LogWrite " non-standard path  $IN_FOLDER"
            }
             if((Test-Path -Path $IN_FOLDER)){
            
                $ZIP_NAME= "$SESSION_ID-$SCAN_ID-$MODALITY-$FIELDSTRENGTH.zip"
                $REMOTE2= "$REMOTE/$SCAN_ID/resources/NIFTI/files/$ZIP_NAME$REMOTE_END" 
                
                Write-Host  " in: $IN_FOLDER"
                Write-Host  "out: $OUT_FOLDER"  
                Write-Host  "zip out: $NIFTI_FOLDER\$ZIP_NAME"  
                Write-Host   "remote: $REMOTE2"
                
        		
                Remove-Item -Recurse -Force  $OUT_FOLDER
                

        		mkdir $OUT_FOLDER
                
                
        	    cd $IN_FOLDER
                $files = Get-ChildItem  $IN_FOLDER 
                
                $proc = Start-Process -FilePath $DCMNII_PATH  -Argument " -b  $INI_FILE -g N -i Y -g Y -o $OUT_FOLDER " -NoNewWindow -PassThru -Wait 
                   
                Write-Host "converted to nifti files" SCAN_ID
               
          
                cd $OUT_FOLDER
                $proc = Start-Process -FilePath $7ZIP_PATH -Argument "a -tzip $ZIP_NAME *.nii"  -NoNewWindow -PassThru -Wait 
             

                mkdir $NIFTI_FOLDER
                copy  -Path "$OUT_FOLDER$ZIP_NAME" -Destination $NIFTI_FOLDER
                Write-Host "copied files" SCAN_ID

                $files = Get-ChildItem  $OUT_FOLDER -Filter *.nii
                foreach ($file in $files){
                	del $file
           	    }

        		Write-Host "  -host $XNAT_HOST -u $u -p $p -m PUT -remote ""$REMOTE2 -local $OUT_FOLDER$ZIP_NAME   "
        		$A = Start-Process -FilePath $REST_CLIENT  -Argument " -host $XNAT_HOST -user_session $JSESSION -m PUT -remote ""$REMOTE2 -local $OUT_FOLDER$ZIP_NAME  " -NoNewWindow -PassThru -Wait 
            
           }
           else{
            LogWrite " ERROR CANNOT FIND ANY PATH:  $IN_FOLDER"
           }
           
       }
       

   

        
    } 

     del $CSVFILE

