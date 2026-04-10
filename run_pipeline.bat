@echo off
echo Running Partner Report Pipeline...
cd /d "%~dp0"
python partner_report_pipeline.py
echo.
echo Done. Press any key to close.
pause
