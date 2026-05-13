#!/bin/bash
set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
SERVER_RUNTIME="$ROOT/server/.runtime"
CLIENT_RUNTIME="$ROOT/client/.runtime"
SERVER_LOG_OUT="$SERVER_RUNTIME/dev.out.log"
SERVER_LOG_ERR="$SERVER_RUNTIME/dev.err.log"
SERVER_PID_FILE="$SERVER_RUNTIME/dev.pid"
CLIENT_LOG_OUT="$CLIENT_RUNTIME/dev.out.log"
CLIENT_LOG_ERR="$CLIENT_RUNTIME/dev.err.log"
CLIENT_PID_FILE="$CLIENT_RUNTIME/dev.pid"
ACTION="${1:-toggle}"
PORTS=(3000 5173 5174 5175 5176 5177 5178 5179 5180)

usage() {
  cat <<EOF

Usage:
  ./manage.command (no args / double-click = toggle)
  ./manage.command toggle
  ./manage.command start
  ./manage.command stop
  ./manage.command restart
  ./manage.command status

Double-click behavior:
  - If services are stopped, start backend/frontend and open the page
  - If services are running, stop them

EOF
}

pid_is_running() {
  local pid="$1"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

kill_tree() {
  local pid="$1"
  local child

  if ! pid_is_running "$pid"; then
    return 1
  fi

  while IFS= read -r child; do
    if [[ -n "$child" ]]; then
      kill_tree "$child" || true
    fi
  done < <(pgrep -P "$pid" 2>/dev/null || true)

  kill "$pid" 2>/dev/null || true
  sleep 0.2
  kill -9 "$pid" 2>/dev/null || true
  return 0
}

kill_from_pidfile() {
  local pidfile="$1"
  local pid=""

  if [[ ! -f "$pidfile" ]]; then
    return 1
  fi

  pid="$(tr -dc '0-9' < "$pidfile")"
  rm -f "$pidfile"
  if [[ -n "$pid" ]]; then
    kill_tree "$pid"
    return $?
  fi

  return 1
}

port_pids() {
  local port="$1"
  lsof -ti "tcp:$port" -sTCP:LISTEN 2>/dev/null | sort -u
}

is_running() {
  local port pid

  for pidfile in "$SERVER_PID_FILE" "$CLIENT_PID_FILE"; do
    if [[ -f "$pidfile" ]]; then
      pid="$(tr -dc '0-9' < "$pidfile")"
      if [[ -n "$pid" ]] && pid_is_running "$pid"; then
        return 0
      fi
    fi
  done

  for port in "${PORTS[@]}"; do
    if [[ -n "$(port_pids "$port")" ]]; then
      return 0
    fi
  done

  return 1
}

check_prerequisites() {
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm was not found. Please install Node.js first."
    return 1
  fi

  if [[ ! -d "$ROOT/server/node_modules" || ! -d "$ROOT/client/node_modules" ]]; then
    echo "Dependencies are missing. Run these once before starting:"
    echo "  npm install"
    echo "  cd server && npm install"
    echo "  cd ../client && npm install"
    return 1
  fi

  return 0
}

wait_for_url() {
  local url="$1"
  local tries="$2"
  local delay="$3"
  local i

  for ((i = 0; i < tries; i++)); do
    if curl -fsS --max-time 1 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$delay"
  done

  return 1
}

stop_core() {
  local mode="${1:-verbose}"
  local found=0
  local pid port

  mkdir -p "$SERVER_RUNTIME" "$CLIENT_RUNTIME"

  if [[ "$mode" != "silent" ]]; then
    echo "[1/3] Stopping by PID files..."
  fi
  kill_from_pidfile "$SERVER_PID_FILE" && found=1
  kill_from_pidfile "$CLIENT_PID_FILE" && found=1

  if [[ "$mode" != "silent" ]]; then
    echo "[2/3] Stopping project dev processes..."
  fi
  while read -r pid command; do
    if [[ -n "$pid" && "$command" == *"$ROOT/"* ]]; then
      kill_tree "$pid" && found=1
    fi
  done < <(ps -axo pid=,command= 2>/dev/null || true)

  if [[ "$mode" != "silent" ]]; then
    echo "[3/3] Stopping listeners on ports 3000 and 5173-5180..."
  fi
  for port in "${PORTS[@]}"; do
    while IFS= read -r pid; do
      if [[ -n "$pid" ]]; then
        kill_tree "$pid" && found=1
      fi
    done < <(port_pids "$port")
  done

  rm -f "$SERVER_PID_FILE" "$CLIENT_PID_FILE"

  if [[ "$mode" != "silent" ]]; then
    if [[ "$found" -eq 1 ]]; then
      echo "Services stopped."
    else
      echo "No running service found."
    fi
  fi
}

do_start() {
  echo "========================================"
  echo "  Data Collector - Start / Restart"
  echo "========================================"
  echo

  check_prerequisites || return 1

  echo "[1/3] Stopping old project processes..."
  stop_core silent
  echo "      Old processes cleaned."
  sleep 1

  mkdir -p "$SERVER_RUNTIME" "$CLIENT_RUNTIME"

  echo
  echo "[2/3] Starting backend in background mode (port 3000)..."
  (
    cd "$ROOT/server" || exit 1
    nohup npm run dev > "$SERVER_LOG_OUT" 2> "$SERVER_LOG_ERR" &
    echo $! > "$SERVER_PID_FILE"
  )

  echo "      Waiting for backend health check..."
  if wait_for_url "http://localhost:3000/api/health" 30 1; then
    echo "      Backend is healthy."
  else
    echo "      Backend not ready yet. Continue startup anyway."
  fi

  echo
  echo "[3/3] Starting frontend in background mode (port 5173)..."
  (
    cd "$ROOT/client" || exit 1
    nohup npm run dev > "$CLIENT_LOG_OUT" 2> "$CLIENT_LOG_ERR" &
    echo $! > "$CLIENT_PID_FILE"
  )

  echo "      Waiting for frontend readiness check..."
  if wait_for_url "http://localhost:5173" 40 0.5; then
    echo "      Frontend is reachable."
  else
    echo "      Frontend not ready yet. Opening browser anyway."
  fi

  echo "      Opening frontend in default browser..."
  open "http://localhost:5173" >/dev/null 2>&1 || true

  echo
  echo "========================================"
  echo "  Started successfully"
  echo "  - Backend:  http://localhost:3000"
  echo "  - Frontend: http://localhost:5173"
  echo "========================================"
  echo
  echo "Logs:"
  echo "  - $SERVER_LOG_OUT"
  echo "  - $SERVER_LOG_ERR"
  echo "  - $CLIENT_LOG_OUT"
  echo "  - $CLIENT_LOG_ERR"
  echo
  echo "Double-click manage.command again to stop services."
}

do_stop() {
  echo "========================================"
  echo "  Data Collector - Stop Services"
  echo "========================================"
  echo
  stop_core verbose
  echo
  echo "========================================"
  echo "  Stop completed"
  echo "========================================"
}

do_status() {
  local has_listen=0
  local has_pid=0
  local port pids pid

  echo "========================================"
  echo "  Data Collector - Service Status"
  echo "========================================"

  for port in "${PORTS[@]}"; do
    pids="$(port_pids "$port" | paste -sd, -)"
    if [[ -n "$pids" ]]; then
      has_listen=1
      echo "Port $port listening: pid=$pids"
    fi
  done

  if [[ "$has_listen" -eq 0 ]]; then
    echo "Ports listening: none"
  fi

  for pidfile in "$SERVER_PID_FILE" "$CLIENT_PID_FILE"; do
    if [[ -f "$pidfile" ]]; then
      has_pid=1
      pid="$(tr -dc '0-9' < "$pidfile")"
      echo "PID file: $pidfile${pid:+ (pid=$pid)}"
    fi
  done

  if [[ "$has_pid" -eq 0 ]]; then
    echo "PID files: missing"
  fi

  echo "========================================"
}

case "$ACTION" in
  toggle)
    if is_running; then
      echo "Detected running services. Switching to stop..."
      do_stop
    else
      echo "Detected stopped services. Switching to start..."
      do_start
    fi
    ;;
  start)
    do_start
    ;;
  stop)
    do_stop
    ;;
  restart)
    echo "Restarting services..."
    stop_core silent
    do_start
    ;;
  status)
    do_status
    ;;
  help|--help|-h)
    usage
    ;;
  *)
    echo "Invalid command: $ACTION"
    usage
    exit 1
    ;;
esac

echo
echo "This window can be closed."
