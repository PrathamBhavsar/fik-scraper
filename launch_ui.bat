@echo off
title FikFap Scraper Clean UI
echo Starting FikFap Scraper Clean UI...
echo Logs will open in separate terminal window
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.7+ from https://python.org
    pause
    exit /b 1
)

REM Run the clean launcher
python launch_ui.py

REM Keep window open if there was an error
if errorlevel 1 (
    echo.
    echo An error occurred. Check the output above.
    pause
)
