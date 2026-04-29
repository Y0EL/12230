@echo off
chcp 65001 >nul
title Mega Crawler Bot

echo.
echo ============================================================
echo   MEGA CRAWLER BOT
echo ============================================================
echo.

REM Cek venv ada
if not exist venv\Scripts\activate.bat (
    echo [ERROR] Virtual environment belum dibuat.
    echo         Jalankan setup.bat terlebih dahulu.
    pause
    exit /b 1
)

REM Cek .env ada
if not exist .env (
    echo [ERROR] File .env belum ada.
    echo         Jalankan setup.bat atau copy .env.example ke .env
    pause
    exit /b 1
)

REM Cek OpenSERP jalan (opsional, tidak blokir)
curl -s --connect-timeout 2 http://localhost:7000 >nul 2>&1
if errorlevel 1 (
    echo [WARNING] OpenSERP tidak terdeteksi di port 7000.
    echo           Jalankan start_openserp.bat di terminal lain untuk hasil terbaik.
    echo           Crawler tetap bisa jalan dengan DuckDuckGo sebagai fallback.
    echo.
)

REM Aktifkan venv
call venv\Scripts\activate.bat

REM Minta query jika tidak ada argumen
if "%~1"=="" (
    echo Masukkan query crawling:
    echo Contoh: cyber defense exhibition 2026
    echo Contoh: DSEI 2025 exhibitors
    echo Contoh: defense expo Asia 2026 global
    echo.
    set /p QUERY="Query: "
    if "!QUERY!"=="" (
        echo [ERROR] Query tidak boleh kosong.
        pause
        exit /b 1
    )
    echo.
    echo Opsi tambahan (tekan Enter untuk skip):
    set /p EXTRA_FLAGS="Flags (contoh: --max 100 --no-enrich): "
    echo.
    setlocal enabledelayedexpansion
    python run.py "!QUERY!" !EXTRA_FLAGS!
) else (
    python run.py %*
)

echo.
echo ============================================================
echo   Selesai! Cek folder output\ untuk file Excel dan CSV.
echo ============================================================
echo.
pause
