@echo off
echo 🚀 IDM Video Downloader - Setup Script
echo ==========================================
echo.

echo 📋 Step 1: Installing Python dependencies...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo ❌ Failed to install Python packages
    echo Please check your Python installation
    pause
    exit /b 1
)

echo.
echo 📋 Step 2: Installing Playwright browsers...
playwright install
if %errorlevel% neq 0 (
    echo ⚠️ Playwright installation had issues
    echo This may still work, continuing...
)

echo.
echo 📋 Step 3: Testing IDM integration...
python -c "import subprocess, os; paths=[r'C:\Program Files\Internet Download Manager\IDMan.exe', r'C:\Program Files (x86)\Internet Download Manager\IDMan.exe']; idm_path=next((p for p in paths if os.path.exists(p)), None); print('✅ IDM found at:', idm_path) if idm_path else print('❌ IDM not found in standard locations')"

echo.
echo 📋 Step 4: Setup complete!
echo.
echo 🎯 Next steps:
echo 1. Edit usage_example.py and change BASE_URL to your target website
echo 2. Run: python usage_example.py
echo 3. Check the SETUP_GUIDE.md for detailed instructions
echo.
echo ✅ Ready to use! Press any key to exit...
pause > nul
