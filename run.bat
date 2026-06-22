@echo off
setlocal EnableExtensions
cd /d C:\Users\natnd\Desktop\bot\bot

:loop
echo Waiting for 0 seconds, press Ctrl+C then Y to stop, N to restart...

python -u main.py
set "EXITCODE=%ERRORLEVEL%"

echo Bot exited with code %EXITCODE%.
echo Restarting bot in 5s...
timeout /t 5 /nobreak >nul
goto loop