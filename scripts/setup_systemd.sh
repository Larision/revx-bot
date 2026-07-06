#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="revx-bot"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="${SUDO_USER:-$(id -un)}"
PYTHON_BIN=""
START_SERVICE=0

usage() {
  cat <<'EOF'
Usage: scripts/setup_systemd.sh [options]

Ensures git is installed and creates/enables a systemd service for revx-bot.

Options:
  --service-name NAME   systemd unit name without .service (default: revx-bot)
  --repo DIR            repository directory (default: parent of this script)
  --user USER           user that will run the bot (default: sudo caller/current user)
  --python PATH         python executable (default: .venv/bin/python if present, else python3)
  --start               start/restart the service after installing it
  -h, --help            show this help

Examples:
  sudo scripts/setup_systemd.sh
  sudo scripts/setup_systemd.sh --start
  sudo scripts/setup_systemd.sh --service-name revx-bot --user botuser --repo /opt/revx-bot --start
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service-name)
      SERVICE_NAME="${2:?missing value for --service-name}"
      shift 2
      ;;
    --repo)
      REPO_DIR="${2:?missing value for --repo}"
      shift 2
      ;;
    --user)
      RUN_USER="${2:?missing value for --user}"
      shift 2
      ;;
    --python)
      PYTHON_BIN="${2:?missing value for --python}"
      shift 2
      ;;
    --start)
      START_SERVICE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This setup script is only supported on Linux." >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1 || [[ ! -d /run/systemd/system ]]; then
  echo "systemd is not available or not running on this system." >&2
  exit 1
fi

if [[ "$EUID" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    sudo_args=(
      "$0"
      --service-name "$SERVICE_NAME"
      --repo "$REPO_DIR"
      --user "$RUN_USER"
    )
    if [[ -n "$PYTHON_BIN" ]]; then
      sudo_args+=(--python "$PYTHON_BIN")
    fi
    if [[ "$START_SERVICE" -eq 1 ]]; then
      sudo_args+=(--start)
    fi
    exec sudo "${sudo_args[@]}"
  fi
  echo "Root privileges are required to install packages and write systemd units." >&2
  exit 1
fi

if ! id "$RUN_USER" >/dev/null 2>&1; then
  echo "User '$RUN_USER' does not exist." >&2
  exit 1
fi

if [[ ! -d "$REPO_DIR" ]]; then
  echo "Repository directory does not exist: $REPO_DIR" >&2
  exit 1
fi

if [[ ! -f "$REPO_DIR/main.py" ]]; then
  echo "main.py not found in repository directory: $REPO_DIR" >&2
  exit 1
fi

install_git() {
  if command -v git >/dev/null 2>&1; then
    echo "git already installed: $(git --version)"
    return
  fi

  echo "git is not installed. Installing..."
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y git
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y git
  elif command -v yum >/dev/null 2>&1; then
    yum install -y git
  elif command -v zypper >/dev/null 2>&1; then
    zypper --non-interactive install git
  elif command -v pacman >/dev/null 2>&1; then
    pacman -Sy --noconfirm git
  else
    echo "No supported package manager found. Install git manually." >&2
    exit 1
  fi
}

install_git

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "$REPO_DIR/.venv/bin/python" ]]; then
    PYTHON_BIN="$REPO_DIR/.venv/bin/python"
  else
    PYTHON_BIN="$(command -v python3 || true)"
  fi
fi

if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
  echo "Python executable not found or not executable: ${PYTHON_BIN:-<empty>}" >&2
  echo "Create a venv first or pass --python /path/to/python." >&2
  exit 1
fi

UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"

cat > "$UNIT_PATH" <<EOF
[Unit]
Description=revx-bot trading bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${REPO_DIR}
ExecStart=${PYTHON_BIN} ${REPO_DIR}/main.py
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"

echo "Installed systemd unit: $UNIT_PATH"
echo "Service enabled: ${SERVICE_NAME}.service"
echo "Python: $PYTHON_BIN"
echo "Repo: $REPO_DIR"
echo "User: $RUN_USER"

if [[ "$START_SERVICE" -eq 1 ]]; then
  systemctl restart "${SERVICE_NAME}.service"
  systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
else
  echo "Service not started. To start it:"
  echo "  sudo systemctl start ${SERVICE_NAME}.service"
fi
