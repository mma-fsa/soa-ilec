#!/usr/bin/env bash
# start_services.sh
# Usage: ./start_services.sh /path/to/mcp_server.log
# - Activates conda env 'soa-ilec'
# - Starts MCP server, redirecting STDOUT to the given log file
# - Starts Jupyter Lab (no token/password) on port 8585
# - Keeps running until Ctrl+C, then shuts down both services

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 /path/to/mcp_server.log"
  exit 1
fi

LOGFILE="$1"
LOGDIR="$(dirname "$LOGFILE")"
mkdir -p "$LOGDIR"
: > "$LOGFILE"   # create or truncate

JUPYTER_LOG="${LOGDIR}/jupyter_8585.log"

# --- Load conda and activate env: soa-ilec ---
if command -v conda >/dev/null 2>&1; then
  # shellcheck disable=SC1091
  source "$(conda info --base)/etc/profile.d/conda.sh"
else
  echo "conda not found on PATH. Install Miniforge/Mambaforge and ensure 'conda' is available."
  exit 1
fi
conda activate soa-ilec

MCP_PID=""
JUPYTER_PID=""

cleanup() {
  echo
  echo "[shutdown] Caught signal. Stopping services…"

  # --- Stop Jupyter Lab (prefer graceful stop) ---
  if [[ -n "${JUPYTER_PID}" ]] && kill -0 "${JUPYTER_PID}" 2>/dev/null; then
    if command -v jupyter >/dev/null 2>&1; then
      # Try clean shutdown by port (works for modern Jupyter Server)
      if jupyter server list 2>/dev/null | grep -qE ':\s*8585/?'; then
        echo "[shutdown] jupyter server stop 8585"
        jupyter server stop 8585 || true
      fi
    fi
    # If it's still alive, send TERM then KILL
    if kill -0 "${JUPYTER_PID}" 2>/dev/null; then
      echo "[shutdown] TERM Jupyter (PID ${JUPYTER_PID})"
      kill "${JUPYTER_PID}" 2>/dev/null || true
      sleep 2
    fi
    if kill -0 "${JUPYTER_PID}" 2>/dev/null; then
      echo "[shutdown] KILL Jupyter (PID ${JUPYTER_PID})"
      kill -9 "${JUPYTER_PID}" 2>/dev/null || true
    fi
  fi

  # --- Stop MCP server ---
  if [[ -n "${MCP_PID}" ]] && kill -0 "${MCP_PID}" 2>/dev/null; then
    echo "[shutdown] TERM MCP server (PID ${MCP_PID})"
    kill "${MCP_PID}" 2>/dev/null || true
    sleep 2
    if kill -0 "${MCP_PID}" 2>/dev/null; then
      echo "[shutdown] KILL MCP server (PID ${MCP_PID})"
      kill -9 "${MCP_PID}" 2>/dev/null || true
    fi
  fi

  echo "[shutdown] Done."
}

trap cleanup INT TERM

# --- Start MCP server (stdout -> LOGFILE) ---
# If you want ONLY stdout, keep the single '>' redirection.
# If you prefer to include stderr too, change to '>> "$LOGFILE" 2>&1'.
echo "[start] Launching MCP server…"
bash start_mcp_server.sh > "$LOGFILE" 2>&1 &
MCP_PID=$!
echo "[start] MCP server PID: ${MCP_PID}, logging to: ${LOGFILE}"

# --- Start Jupyter Lab (no password) on port 8585 ---
echo "[start] Launching Jupyter Lab on http://127.0.0.1:8585 (no token)…"
jupyter lab \
  --no-browser \
  --port 8585 \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --NotebookApp.token='' \
  --NotebookApp.password='' \
  >> "$JUPYTER_LOG" 2>&1 &
JUPYTER_PID=$!
echo "[start] Jupyter PID: ${JUPYTER_PID}, logs: ${JUPYTER_LOG}"

echo
echo "[ready] Press Ctrl+C to stop both services."

# --- Keep script alive until Ctrl+C (trap will run cleanup) ---
# 'wait' without args waits for all current child processes.
# If either child exits on its own, we also trigger cleanup and exit.
set +e
wait -n "${MCP_PID}" "${JUPYTER_PID}"
EXITED=$?
echo "[monitor] A child process exited (code ${EXITED}). Initiating shutdown…"
cleanup
exit "${EXITED}"
