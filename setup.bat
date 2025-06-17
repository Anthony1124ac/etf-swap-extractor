@echo off
echo Checking Python 3.11 installation...

where python3.11 >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Python 3.11 is not installed. Please install it from https://www.python.org/downloads/
    exit /b 1
)

echo Creating virtual environment...
python3.11 -m venv venv

echo Activating virtual environment...
call venv\Scripts\activate

echo Upgrading pip...
python -m pip install --upgrade pip

echo Installing required packages...
pip install -r requirements.txt

echo.
echo Setup complete! To start the application:
echo 1. Activate the virtual environment: venv\Scripts\activate
echo 2. Run the application: python app.py
echo 3. Open your browser to: http://localhost:5000
echo.

pause 