@echo off
setlocal DisableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

set "INVEST_CONDA_EXE="
if defined CONDA_EXE if exist "%CONDA_EXE%" set "INVEST_CONDA_EXE=%CONDA_EXE%"
for %%I in (conda.exe) do if not defined INVEST_CONDA_EXE if not "%%~$PATH:I"=="" set "INVEST_CONDA_EXE=%%~$PATH:I"
if not defined INVEST_CONDA_EXE if exist "%USERPROFILE%\miniconda3\Scripts\conda.exe" set "INVEST_CONDA_EXE=%USERPROFILE%\miniconda3\Scripts\conda.exe"
if not defined INVEST_CONDA_EXE if exist "%USERPROFILE%\anaconda3\Scripts\conda.exe" set "INVEST_CONDA_EXE=%USERPROFILE%\anaconda3\Scripts\conda.exe"

if not defined INVEST_CONDA_EXE (
  echo 未找到 Conda。请在 Anaconda Prompt 中运行此脚本，或先安装 Miniconda。
  pause
  exit /b 1
)

echo 正在使用 Conda 的 invest 环境启动本地服务……
echo 首页：http://127.0.0.1:8000
echo 股票标记：http://127.0.0.1:8000/marked-stocks
echo 按 Ctrl+C 停止服务。
echo.
"%INVEST_CONDA_EXE%" run -n invest --no-capture-output python -m uvicorn backend.app.main:app --reload --host 127.0.0.1 --port 8000
set "INVEST_EXIT_CODE=%ERRORLEVEL%"

if not "%INVEST_EXIT_CODE%"=="0" (
  echo.
  echo 启动失败，请查看上方提示。请确认 invest 环境及项目依赖已安装，且 8000 端口未被占用。
  pause
)
endlocal & exit /b %INVEST_EXIT_CODE%
