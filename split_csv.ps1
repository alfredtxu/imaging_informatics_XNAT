Param(
  [string]$CSV_FILE,
  [int]$LINES
)
$DRIVE = "E:"
$PROJECT = "DATA"


Function LogWrite
{
   Param ([string]$logstring)
  
   Add-content $Logfile -value $logstring
}
$FILE_NUMBER = 1
$LINE_NUMBER = 1
$Logfile = $CSV_FILE -replace ".csv","$FILE_NUMBER.csv" 
$HEADER = "labels, ID, session"
 LogWrite " $HEADER "

$data = Import-Csv  $CSV_FILE |  ForEach-Object {
    
    $SESSION_LABEL =  $_.labels -replace " ",""     
    $SCAN_ID = $_.ID -replace " ",""     
    $SESSION_ID = $_.session  -replace " ",""     
   

    LogWrite "$SESSION_LABEL , $SCAN_ID , $SESSION_ID"
        
     $LINE_NUMBER++
     if($LINE_NUMBER -gt $LINES){
        $FILE_NUMBER++
        $LINE_NUMBER = 1
        $Logfile = $CSV_FILE -replace ".csv","$FILE_NUMBER.csv"
        Add-content $Logfile -value "$HEADER"
     }
       
}


 






 
 