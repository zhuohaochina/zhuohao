@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
set "SERVER_RUNTIME=%ROOT%\server\.runtime"
set "CLIENT_RUNTIME=%ROOT%\client\.runtime"
set "SERVER_LOG_OUT=%SERVER_RUNTIME%\dev.out.log"
set "SERVER_LOG_ERR=%SERVER_RUNTIME%\dev.err.log"
set "SERVER_PID_FILE=%SERVER_RUNTIME%\dev.pid"
set "CLIENT_LOG_OUT=%CLIENT_RUNTIME%\dev.out.log"
set "CLIENT_LOG_ERR=%CLIENT_RUNTIME%\dev.err.log"
set "CLIENT_PID_FILE=%CLIENT_RUNTIME%\dev.pid"
set "ACTION=%~1"
if "%ACTION%"=="" set "ACTION=toggle"

if /I "%ACTION%"=="toggle" goto :do_toggle
if /I "%ACTION%"=="start" goto :do_start
if /I "%ACTION%"=="stop" goto :do_stop
if /I "%ACTION%"=="restart" goto :do_restart
if /I "%ACTION%"=="status" goto :do_status
if /I "%ACTION%"=="help" goto :usage

echo Invalid command: %ACTION%
goto :usage

:usage
echo.
echo Usage:
echo   manage.bat ^(no args / double-click = toggle^)
echo   manage.bat toggle
echo   manage.bat start
echo   manage.bat stop
echo   manage.bat restart
echo   manage.bat status
echo.
echo Double-click behavior:
echo   - If services are stopped, start backend/frontend and open the page
echo   - If services are running, stop them
echo.
endlocal
exit /b 1

:do_toggle
call :is_running
if "%RUNNING%"=="1" (
    echo Detected running services. Switching to stop...
    goto :do_stop
) else (
    echo Detected stopped services. Switching to start...
    goto :do_start
)

:do_start
title Project Start Script
echo ========================================
echo   Data Collector - Start / Restart
echo ========================================
echo.

echo [1/3] Stopping old project processes...
call :stop_core silent
echo       Old processes cleaned.

timeout /t 1 /nobreak >nul

echo.
echo [2/3] Starting backend in hidden mode (port 3000)...
powershell -NoProfile -ExecutionPolicy Bypass -Command "New-Item -ItemType Directory -Force '%SERVER_RUNTIME%' | Out-Null; $wd='%ROOT%\server'; $p=Start-Process -FilePath 'node.exe' -WorkingDirectory $wd -ArgumentList '.\node_modules\ts-node-dev\lib\bin.js','--respawn','src/index.ts' -WindowStyle Hidden -RedirectStandardOutput '%SERVER_LOG_OUT%' -RedirectStandardError '%SERVER_LOG_ERR%' -PassThru; $p.Id | Set-Content -Encoding ASCII '%SERVER_PID_FILE%'"

echo       Waiting for backend health check...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ok=$false; for($i=0; $i -lt 30; $i++){ try { $r=Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:3000/api/health' -TimeoutSec 1; if($r.StatusCode -eq 200){ $ok=$true; break } } catch {}; Start-Sleep -Seconds 1 }; if($ok){ exit 0 } else { exit 1 }"
if errorlevel 1 (
    echo       Backend not ready yet. Continue startup anyway.
) else (
    echo       Backend is healthy.
)

echo.
echo [3/3] Starting frontend in hidden mode (port 5173)...
powershell -NoProfile -ExecutionPolicy Bypass -Command "New-Item -ItemType Directory -Force '%CLIENT_RUNTIME%' | Out-Null; $wd='%ROOT%\client'; $p=Start-Process -FilePath 'node.exe' -WorkingDirectory $wd -ArgumentList '.\node_modules\vite\bin\vite.js' -WindowStyle Hidden -RedirectStandardOutput '%CLIENT_LOG_OUT%' -RedirectStandardError '%CLIENT_LOG_ERR%' -PassThru; $p.Id | Set-Content -Encoding ASCII '%CLIENT_PID_FILE%'"

echo       Waiting for frontend readiness check...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ok=$false; for($i=0; $i -lt 20; $i++){ try { $r=Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:5173' -TimeoutSec 1; if($r.StatusCode -ge 200 -and $r.StatusCode -lt 500){ $ok=$true; break } } catch {}; Start-Sleep -Milliseconds 500 }; if($ok){ exit 0 } else { exit 1 }"
if errorlevel 1 (
    echo       Frontend not ready yet. Opening browser anyway.
) else (
    echo       Frontend is reachable.
)
echo       Opening frontend in default browser...
start "" "http://localhost:5173"

echo.
echo ========================================
echo   Started successfully
echo   - Backend:  http://localhost:3000
echo   - Frontend: http://localhost:5173
echo ========================================
echo.
echo Logs:
echo   - %SERVER_LOG_OUT%
echo   - %SERVER_LOG_ERR%
echo   - %CLIENT_LOG_OUT%
echo   - %CLIENT_LOG_ERR%
echo PID files:
echo   - %SERVER_PID_FILE%
echo   - %CLIENT_PID_FILE%
echo.
echo Double-click manage.bat again to stop services.
echo.
endlocal
exit /b 0

:do_stop
title Project Stop Script
echo ========================================
echo   Data Collector - Stop Services
echo ========================================
echo.
call :stop_core verbose
echo.
echo ========================================
echo   Stop completed
echo ========================================
echo.
endlocal
exit /b 0

:do_restart
title Project Restart Script
echo Restarting services...
call :stop_core silent
call :do_start
exit /b 0

:do_status
title Project Status Script
set "HAS_LISTEN=0"
set "HAS_PID=0"
set "PORT_INFO="

for %%p in (3000 5173 5174 5175 5176 5177 5178 5179 5180) do (
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr /R /C:":%%p .*LISTENING" 2^>nul') do (
        set "HAS_LISTEN=1"
        set "PORT_INFO=!PORT_INFO! %%p(pid=%%a)"
    )
)

if exist "%SERVER_PID_FILE%" set "HAS_PID=1"
if exist "%CLIENT_PID_FILE%" set "HAS_PID=1"

echo ========================================
echo   Data Collector - Service Status
echo ========================================
if "%HAS_LISTEN%"=="1" (
    echo Ports listening:!PORT_INFO!
) else (
    echo Ports listening: none
)
if "%HAS_PID%"=="1" (
    echo PID files: present
) else (
    echo PID files: missing
)
echo ========================================
echo.
endlocal
exit /b 0

:is_running
set "RUNNING=0"
for %%p in (3000 5173 5174 5175 5176 5177 5178 5179 5180) do (
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr /R /C:":%%p .*LISTENING" 2^>nul') do (
        set "RUNNING=1"
    )
)
if exist "%SERVER_PID_FILE%" set "RUNNING=1"
if exist "%CLIENT_PID_FILE%" set "RUNNING=1"
exit /b 0

:stop_core
set "MODE=%~1"
set "FOUND=0"
set "PORT_KILLED=0"
set "PS_COUNT=0"
if not exist "%SERVER_RUNTIME%" mkdir "%SERVER_RUNTIME%" >nul 2>&1
set "COUNT_FILE=%SERVER_RUNTIME%\stop-killed.count"

if /I not "%MODE%"=="silent" echo [1/3] Stopping by PID files...
call :kill_from_pidfile "%SERVER_PID_FILE%"
call :kill_from_pidfile "%CLIENT_PID_FILE%"

if /I not "%MODE%"=="silent" echo [2/3] Stopping project dev processes...
if exist "%COUNT_FILE%" del /f /q "%COUNT_FILE%" >nul 2>&1
powershell -NoProfile -ExecutionPolicy Bypass -Command "$root='%ROOT%'; $countFile='%COUNT_FILE%'; $all=@(Get-CimInstance Win32_Process); $ids=New-Object 'System.Collections.Generic.HashSet[int]'; foreach($p in $all){ $cmd=$p.CommandLine; if(-not $cmd){ continue }; if($cmd -like '*codex.js*'){ continue }; $isTarget = ($cmd -like ('*'+$root+'\server*')) -or ($cmd -like ('*'+$root+'\client*')) -or ($cmd -like '*ts-node-dev\lib\bin.js*') -or ($cmd -like '*ts-node-dev\lib\wrap.js*') -or ($cmd -like '*vite\bin\vite.js*'); if($isTarget){ [void]$ids.Add([int]$p.ProcessId) } }; $changed=$true; while($changed){ $changed=$false; foreach($p in $all){ $id=[int]$p.ProcessId; $pp=[int]$p.ParentProcessId; if($ids.Contains($pp) -and -not $ids.Contains($id)){ [void]$ids.Add($id); $changed=$true } } }; foreach($id in @($ids)){ $cur=$all | Where-Object { $_.ProcessId -eq $id } | Select-Object -First 1; for($i=0; $i -lt 6 -and $cur; $i++){ $pp=[int]$cur.ParentProcessId; if($pp -le 0){ break }; $parent=$all | Where-Object { $_.ProcessId -eq $pp } | Select-Object -First 1; if(-not $parent){ break }; if(($parent.Name -eq 'node.exe' -or $parent.Name -eq 'cmd.exe') -and $parent.CommandLine -notlike '*codex.js*'){ [void]$ids.Add([int]$parent.ProcessId) }; $cur=$parent } }; foreach($id in ($ids | Sort-Object -Descending)){ Stop-Process -Id $id -Force -ErrorAction SilentlyContinue }; Set-Content -Encoding ASCII -Path $countFile -Value $ids.Count"
if exist "%COUNT_FILE%" (
    set /p PS_COUNT=<"%COUNT_FILE%"
    del /f /q "%COUNT_FILE%" >nul 2>&1
)
if not defined PS_COUNT set "PS_COUNT=0"
for /f "delims=0123456789" %%x in ("!PS_COUNT!") do set "PS_COUNT=0"
if not "!PS_COUNT!"=="0" set "FOUND=1"

if /I not "%MODE%"=="silent" echo [3/3] Stopping listeners on ports 3000 and 5173-5180...
call :kill_ports
if "!PORT_KILLED!"=="1" (
    timeout /t 1 /nobreak >nul
    set "PORT_KILLED=0"
    call :kill_ports
)

if /I not "%MODE%"=="silent" (
    if "!FOUND!"=="1" (
        echo Services stopped.
    ) else (
        echo No running service found.
    )
)
exit /b 0

:kill_from_pidfile
set "PIDFILE=%~1"
if exist "%PIDFILE%" (
    set "PID_VALUE="
    set /p PID_VALUE=<"%PIDFILE%"
    if defined PID_VALUE (
        taskkill /F /T /PID !PID_VALUE! >nul 2>&1 && set "FOUND=1"
    )
    del /f /q "%PIDFILE%" >nul 2>&1
)
exit /b 0

:kill_ports
for %%p in (3000 5173 5174 5175 5176 5177 5178 5179 5180) do (
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr /R /C:":%%p .*LISTENING" 2^>nul') do (
        taskkill /F /T /PID %%a >nul 2>&1 && (
            set "FOUND=1"
            set "PORT_KILLED=1"
        )
    )
)
exit /b 0
