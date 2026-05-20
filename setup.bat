@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

set "ROOT=%~dp0"
set "ROOT=%ROOT:~0,-1%"

echo ============================================================
echo  StreamPark Ops Skill - Setup
echo  Root: %ROOT%
echo ============================================================

REM 1. Python 3.12+
echo.
echo [1/3] Detect Python 3.12+...
set "PY="
py -3.12 --version >nul 2>&1 && set "PY=py -3.12"
if not defined PY (where python3.12 >nul 2>&1 && set "PY=python3.12")
REM Astral uv installs (e.g. Astral/CPython3.12.12)
if not defined PY (
    for /f "tokens=1" %%t in ('py --list 2^>nul ^| findstr /i "CPython3.12"') do (
        if not defined PY set "PY=py -V:%%t"
    )
)
if not defined PY (
    where python >nul 2>&1 && (
        for /f "tokens=2" %%v in ('python --version 2^>^&1') do (
            echo %%v | findstr /b "3.12 3.13" >nul && set "PY=python"
        )
    )
)
if not defined PY (
    echo   X Python 3.12+ not found. Install from https://www.python.org/downloads/
    exit /b 1
)
echo   OK using: %PY%

REM 2. venv
echo.
echo [2/3] Create .venv...
if not exist "%ROOT%\.venv\Scripts\python.exe" (
    %PY% -m venv "%ROOT%\.venv" || (echo   X venv failed & exit /b 1)
    echo   OK created
) else (
    echo   OK already exists
)

REM 3. deps
echo.
echo [3/3] Install requirements...
call "%ROOT%\.venv\Scripts\activate.bat"
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r "%ROOT%\requirements.txt" || (echo   X pip install failed & exit /b 1)
echo   OK deps installed

REM 4. config check
echo.
if not exist "%ROOT%\config.ini" (
    if exist "%ROOT%\config.ini.example" (
        copy "%ROOT%\config.ini.example" "%ROOT%\config.ini" >nul
        echo   ! config.ini generated from template. Edit it before use:
        echo     %ROOT%\config.ini
    )
)

echo.
echo ============================================================
echo  Done. Verify with:
echo    .venv\Scripts\python.exe scripts\sp_apps_list.py --env uat
echo ============================================================
endlocal
