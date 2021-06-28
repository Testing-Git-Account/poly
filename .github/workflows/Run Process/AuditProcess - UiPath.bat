chdir /d C:\Audit Process\Code
python Main.py
chdir /d C:\Audit Process\Code\Output Files
copy "Audit Process Report.xlsx" "C:\Audit Process\Run Process\Audit Process Report.xlsx"
copy "Process_Log.txt" "C:\Audit Process\Run Process\Process_Log.txt"
TIMEOUT 10
EXIT