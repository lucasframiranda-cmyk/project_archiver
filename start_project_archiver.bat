@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if %errorlevel%==0 (
    python main.py
    exit /b %errorlevel%
)

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 main.py
    exit /b %errorlevel%
)

echo Python nao encontrado no PATH.
pause
endlocal
