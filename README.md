# SyncRemnawave

`SyncRemnawave` is a production-ready one-way synchronizer for two Remnawave panels over the official REST API.

- Reads only from panel A
- Writes only to panel B
- Syncs squads and users
- Syncs nodes only when enabled
- Supports dry-run, retries, state file fallback, and interactive setup

## Quick Install

After this repository is published to GitHub, users will be able to install it with one command.

### Linux / macOS

```bash
curl -fsSL https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.sh | bash
```

### Windows PowerShell

```powershell
irm https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.ps1 | iex
```

The installer creates a virtual environment, installs the app, and opens the setup wizard where the user enters:

- source panel URL
- source token
- destination panel URL
- destination token
- whether to sync users
- whether to sync squads
- whether to sync nodes
- what to do with missing imported users

## Manual Install

### From GitHub

```bash
python -m pip install "git+https://github.com/LaRsonOFFai/SyncRemnawave.git"
sync-remnawave init
sync-remnawave --dry-run
sync-remnawave
```

### Local Development

```bash
python -m pip install -e .
sync-remnawave init
```

## Commands

```bash
sync-remnawave init
sync-remnawave --dry-run
sync-remnawave --sync-squads --sync-users
sync-remnawave --sync-squads --sync-users --sync-nodes
```

## Config File

By default the interactive wizard stores the user config in a per-user config directory:

- Windows: `%APPDATA%\\SyncRemnawave\\.env`
- Linux/macOS: `$XDG_CONFIG_HOME/SyncRemnawave/.env` or `~/.config/SyncRemnawave/.env`

You can also point to a custom config file:

```bash
sync-remnawave init --config-file ./my-sync.env
sync-remnawave --config-file ./my-sync.env --dry-run
```

## Environment Variables

```dotenv
SRC_URL=https://panel-a.example.com
SRC_API_KEY=replace-with-source-jwt-or-bearer-token
DST_URL=https://panel-b.example.com
DST_API_KEY=replace-with-destination-jwt-or-bearer-token
ENABLE_USER_SYNC=true
ENABLE_SQUAD_SYNC=true
ENABLE_NODE_SYNC=false
DISABLE_MISSING_USERS=true
DELETE_MISSING_USERS=false
PAGE_SIZE=100
REQUEST_TIMEOUT=20
STATE_FILE=sync_state.json
LOG_LEVEL=INFO
```

## Notes

- Authentication follows the current Remnawave OpenAPI and uses `Authorization: Bearer ...`
- The client also sends `X-Api-Key` for compatibility with older deployments
- Squad metadata is not exposed in the official OpenAPI, so squad identity fallback uses the state file
- User and node metadata use the official metadata endpoints when available
