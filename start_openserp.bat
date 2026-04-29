@echo off
chcp 65001 >nul
title OpenSERP - Search Engine (port 7000)

echo.
echo ============================================================
echo   OpenSERP Search Engine
echo   Biarkan terminal ini tetap terbuka selama crawling!
echo ============================================================
echo.

REM Cari openserp.exe di lokasi umum
set OPENSERP_EXE=

if exist "openserp\openserp.exe"           set OPENSERP_EXE=openserp\openserp.exe
if exist "C:\openserp\openserp.exe"        set OPENSERP_EXE=C:\openserp\openserp.exe
if exist "%USERPROFILE%\openserp\openserp.exe" set OPENSERP_EXE=%USERPROFILE%\openserp\openserp.exe
if exist "openserp.exe"                    set OPENSERP_EXE=openserp.exe

if "%OPENSERP_EXE%"=="" (
    echo [ERROR] openserp.exe tidak ditemukan!
    echo.
    echo Download dari:
    echo https://github.com/karust/openserp/releases/download/v0.7.2/openserp-windows-amd64-0.7.2.tgz
    echo.
    echo Lalu extract ke salah satu folder ini:
    echo   - C:\openserp\
    echo   - %USERPROFILE%\openserp\
    echo   - Folder yang sama dengan script ini
    echo.
    pause
    exit /b 1
)

echo Ditemukan: %OPENSERP_EXE%
echo Menjalankan di http://localhost:7000
echo.
echo Tekan Ctrl+C untuk berhenti.
echo.

"%OPENSERP_EXE%" serve -a 0.0.0.0 -p 7000

echo.
echo [INFO] OpenSERP berhenti.
pause
