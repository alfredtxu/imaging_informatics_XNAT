Param(
  [string]$SUBFOLDER
  
)


$DRIVE = "E:"
$PROJECT = "TEST"
$Logfile = "$DRIVE\LOGS\ARCHIVE_LIST.csv"

Function LogWrite
{
   Param ([string]$logstring)

   Add-content $Logfile -value $logstring
}



$ARCHIVE = "E:\b_imagepool_ct"
 Get-ChildItem  $ARCHIVE | foreach ($_) { LogWrite "$ARCHIVE  ,  $_" }









 
 