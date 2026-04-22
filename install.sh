#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${SYNCREMNAWAVE_REPO:-https://github.com/LaRsonOFFai/SyncRemnawave.git}"
INSTALL_ROOT="${HOME}/.local/share/SyncRemnawave"
USER_BIN_DIR="${HOME}/.local/bin"
PREFERRED_BIN_DIR="/usr/local/bin"
VENV_DIR="${INSTALL_ROOT}/venv"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

BIN_DIR="${USER_BIN_DIR}"
if [ -d "${PREFERRED_BIN_DIR}" ] && [ -w "${PREFERRED_BIN_DIR}" ]; then
  BIN_DIR="${PREFERRED_BIN_DIR}"
elif [ ! -e "${PREFERRED_BIN_DIR}" ] && [ -w "$(dirname "${PREFERRED_BIN_DIR}")" ]; then
  mkdir -p "${PREFERRED_BIN_DIR}"
  BIN_DIR="${PREFERRED_BIN_DIR}"
fi

mkdir -p "${INSTALL_ROOT}" "${USER_BIN_DIR}" "${BIN_DIR}"
python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/pip" install --upgrade pip
"${VENV_DIR}/bin/pip" install --upgrade --force-reinstall --no-cache-dir "git+${REPO_URL}"

cat > "${BIN_DIR}/remnasync" <<EOF
#!/usr/bin/env bash
exec "${VENV_DIR}/bin/remnasync" "\$@"
EOF
chmod +x "${BIN_DIR}/remnasync"

cat > "${BIN_DIR}/sync-remnawave" <<EOF
#!/usr/bin/env bash
exec "${VENV_DIR}/bin/remnasync" "\$@"
EOF
chmod +x "${BIN_DIR}/sync-remnawave"

echo
echo "SyncRemnawave installed."
echo "Command directory: ${BIN_DIR}"
if command -v remnasync >/dev/null 2>&1; then
  echo "Command available as: $(command -v remnasync)"
elif [[ ":$PATH:" != *":${BIN_DIR}:"* ]]; then
  echo "Add this directory to PATH and reopen the shell:"
  echo "  export PATH=\"${BIN_DIR}:\$PATH\""
  echo "Or run directly:"
  echo "  ${BIN_DIR}/remnasync"
fi
if [ -r /dev/tty ] && [ -w /dev/tty ]; then
  echo "Starting setup wizard..."
  "${VENV_DIR}/bin/remnasync" init </dev/tty >/dev/tty
else
  echo "Interactive terminal was not detected."
  echo "Run this manually to finish setup:"
  echo "  ${VENV_DIR}/bin/remnasync init"
fi
