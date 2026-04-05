@echo off
REM Walgreens Stock Watcher Startup Script
REM This script starts the Flask application

echo.
echo ==================================================
echo  Walgreens Pokemon Card Stock Watcher
echo ==================================================
echo.

REM Change to backend directory
cd /d "%~dp0backend"

REM Use the virtual environment Python if it exists
set PYTHON_EXE=python
if exist "..\\.venv\\Scripts\\python.exe" (
    set PYTHON_EXE=..\\.venv\\Scripts\\python.exe
    echo Using virtual environment Python
)

REM Check if Python is installed
%PYTHON_EXE% --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python not found!
    echo Please install Python 3.8 or higher and add it to PATH
    echo https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Check if requirements are installed
%PYTHON_EXE% -c "import flask" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing dependencies... Please wait
    %PYTHON_EXE% -m pip install -q -r requirements.txt
    if %errorlevel% neq 0 (
        echo ERROR: Failed to install dependencies
        echo Run: %PYTHON_EXE% -m pip install -r requirements.txt
        pause
        exit /b 1
    )
)

REM Check if .env file exists
if not exist ".env" (
    echo WARNING: .env file not found
    echo Copy .env.example to .env and add your Discord webhook URL
    echo.
)

REM Start the application
echo Starting Flask application...
echo.
echo Open your browser and go to: http://localhost:5000
echo Press Ctrl+C to stop the server
echo.

%PYTHON_EXE% app.py

echo.
echo Walgreens Stock Watcher stopped
pause
