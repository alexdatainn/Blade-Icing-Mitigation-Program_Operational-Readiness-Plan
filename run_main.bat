@echo off
REM Activate the conda environment
call C:\Users\aebrahimi\.conda\envs\pip_Algonquin

REM Navigate to the folder containing your main.py script
cd C:\Users\aebrahimi\OneDrive - Liberty\Documents\Project Codes\Ice-FCST-APM-2024

REM Run the Python script
python main.py

REM Deactivate conda environment after execution
call conda deactivate
