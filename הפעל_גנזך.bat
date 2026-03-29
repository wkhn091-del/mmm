@echo off
title גנזך — ספרייה יהודית
chcp 65001 > nul
cd /d "%~dp0"

echo.
echo  ╔══════════════════════════════════╗
echo  ║      גנזך — ספרייה יהודית       ║
echo  ║   מפעיל שרת מקומי...            ║
echo  ╚══════════════════════════════════╝
echo.

:: Check Python
python --version > nul 2>&1
if errorlevel 1 (
    echo שגיאה: Python לא מותקן!
    echo הורד מ: https://python.org
    pause
    exit
)

:: Install deps if needed
if not exist ".deps_installed" (
    echo מתקין תלויות...
    pip install flask flask-cors requests beautifulsoup4 reportlab --quiet
    echo installed > .deps_installed
)

:: Start server in background
start /B python server.py

:: Wait for server to start
echo ממתין לשרת...
timeout /t 3 /nobreak > nul

:: Open browser
start http://localhost:5000

echo.
echo  ✅ גנזך פועל! 
echo  פתח דפדפן בכתובת: http://localhost:5000
echo.
echo  לסגירה — לחץ כאן ואז Ctrl+C
echo.
pause
