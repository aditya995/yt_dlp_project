@echo off
setlocal

:: ===== SETTINGS =====
set "PYTHON_VERSION=3.11.6"
set "INSTALLER=python-%PYTHON_VERSION%-amd64.exe"
set "DOWNLOAD_URL=https://www.python.org/ftp/python/%PYTHON_VERSION%/%INSTALLER%"
set "SHORTCUT_NAME=yt-aditya.lnk"
set "STARTUP_FOLDER=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "BAT_PATH=%~dp0yt-aditya.bat"
set "APP_FILE=%~dp0app.pyw"
:: =====================

:: --- Check existing Python version ---
for /f "tokens=2 delims= " %%i in ('python --version 2^>^&1') do set "INSTALLED_VERSION=%%i"

if "%INSTALLED_VERSION%"=="%PYTHON_VERSION%" (
    echo Python %PYTHON_VERSION% is already installed.
    goto check_pip
)

:: --- Install Python silently ---
echo Downloading Python %PYTHON_VERSION%...
powershell -Command "Invoke-WebRequest -Uri '%DOWNLOAD_URL%' -OutFile '%TEMP%\%INSTALLER%'"

echo Installing Python silently...
%TEMP%\%INSTALLER% /quiet InstallAllUsers=1 PrependPath=1 Include_test=0

echo Python installation complete.

:check_pip
:: --- Ensure pip and requirements ---
echo Checking for pip...
python -m ensurepip --upgrade

if exist "requirements.txt" (
    echo Installing packages from requirements.txt...
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
) else (
    echo requirements.txt not found. Skipping package installation.
)

:: --- Create Startup Shortcut ---
echo Creating shortcut in Startup folder...

> "%TEMP%\CreateShortcut.vbs" echo Set WshShell = WScript.CreateObject("WScript.Shell")
>> "%TEMP%\CreateShortcut.vbs" echo Set Shortcut = WshShell.CreateShortcut(WshShell.ExpandEnvironmentStrings("%STARTUP_FOLDER%\%SHORTCUT_NAME%"))
>> "%TEMP%\CreateShortcut.vbs" echo Shortcut.TargetPath = WshShell.ExpandEnvironmentStrings("%BAT_PATH%")
>> "%TEMP%\CreateShortcut.vbs" echo Shortcut.WorkingDirectory = WshShell.ExpandEnvironmentStrings("%~dp0")
>> "%TEMP%\CreateShortcut.vbs" echo Shortcut.WindowStyle = 7
>> "%TEMP%\CreateShortcut.vbs" echo Shortcut.Description = "Auto start yt-aditya"
>> "%TEMP%\CreateShortcut.vbs" echo Shortcut.Save

cscript //nologo "%TEMP%\CreateShortcut.vbs"
del "%TEMP%\CreateShortcut.vbs"
echo Shortcut created in Startup folder.

:: --- Run app.pyw silently ---
if exist "%APP_FILE%" (
    echo Running app.pyw silently...
    start "" pythonw.exe "%APP_FILE%"
) else (
    echo app.pyw not found.
)

:end
exit /b
