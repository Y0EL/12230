@echo off
chcp 65001 >nul
title Mega Crawler Bot - Setup

echo.
echo ============================================================
echo   MEGA CRAWLER BOT - Setup Pertama Kali
echo ============================================================
echo.

REM Cek Python ada
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python tidak ditemukan. Install dari https://python.org
    pause
    exit /b 1
)

echo [1/5] Membuat virtual environment...
if exist venv (
    echo       venv sudah ada, skip.
) else (
    python -m venv venv
    if errorlevel 1 ( echo [ERROR] Gagal buat venv & pause & exit /b 1 )
    echo       OK
)

echo.
echo [2/5] Mengaktifkan venv dan install dependencies...
call venv\Scripts\activate.bat
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo [ERROR] pip install gagal. Cek koneksi internet.
    pause
    exit /b 1
)
echo       OK

echo.
echo [3/5] Install Playwright Chromium...
playwright install chromium
if errorlevel 1 (
    echo [WARNING] Playwright install gagal. Coba jalankan manual:
    echo           venv\Scripts\activate ^&^& playwright install chromium
) else (
    echo       OK
)

echo.
echo [4/5] Cek file .env...
if exist .env (
    echo       .env sudah ada.
) else (
    copy .env.example .env >nul
    echo       .env dibuat dari .env.example
    echo.
    echo *** WAJIB: Buka file .env dan isi OPENAI_API_KEY=sk-... ***
)

echo.
echo [5/5] Cek folder output...
if not exist output mkdir output
echo       OK

echo.
echo ============================================================
echo   Setup selesai!
echo.
echo   Langkah berikutnya:
echo   1. Buka .env dan isi OPENAI_API_KEY
echo   2. Download OpenSERP binary dari README (tanpa Docker)
echo   3. Jalankan start_openserp.bat di terminal tersendiri
echo   4. Jalankan run.bat untuk mulai crawl
echo ============================================================
echo.
pause
