@echo off
echo ========================================
echo Building and Pushing MeteoService to GHCR
echo ========================================

REM Set variables - MODIFY THIS
set GITHUB_USERNAME=sitscholl
set IMAGE_NAME=meteo-service
set TAG=latest

REM Prompt for token (more secure)
set /p GITHUB_TOKEN=Enter your GitHub Personal Access Token: 

REM Derived variables
REM Create one at GitHub Settings > Developer settings > Personal access tokens. Select scopes: write:packages, read:packages, delete:packages
set FULL_IMAGE_NAME=ghcr.io/%GITHUB_USERNAME%/%IMAGE_NAME%:%TAG%

echo.
echo Using image name: %FULL_IMAGE_NAME%
echo.

echo Logging in to GitHub Container Registry...
echo %GITHUB_TOKEN% | docker login ghcr.io -u %GITHUB_USERNAME% --password-stdin

if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to login to GHCR
    pause
    exit /b 1
)

echo Login successful!
echo.

echo Building Docker image...
docker build -t %FULL_IMAGE_NAME% .

if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to build Docker image
    pause
    exit /b 1
)

echo Build successful!
echo.

echo Pushing to GitHub Container Registry...
docker push %FULL_IMAGE_NAME%

if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to push to GHCR
    pause
    exit /b 1
)

echo.
echo ========================================
echo SUCCESS! Image pushed to GHCR
echo ========================================
echo.
echo Your image is now available at:
echo %FULL_IMAGE_NAME%
echo.
echo To pull on any machine:
echo docker pull %FULL_IMAGE_NAME%
echo.
pause