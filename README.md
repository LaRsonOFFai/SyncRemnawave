# SyncRemnawave

## English

### What This App Does

`SyncRemnawave` copies data one way from one Remnawave panel to another over the official REST API.

- Panel A = source
- Panel B = destination
- Data is read only from A
- Data is written only to B

It can sync:

- squads
- users
- nodes, only if you enable node sync

### What You Need

Prepare these values before setup:

- source panel URL
- source JWT or Bearer token
- destination panel URL
- destination JWT or Bearer token

Use the base panel URL, for example:

```text
https://panel-a.example.com
https://panel-b.example.com
```

Do not add `/api` manually.

### Install With One Command

#### Linux / macOS

```bash
curl -fsSL https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.sh | bash
```

The installer tries to place `remnasync` into `/usr/local/bin` when it has permission.
If that is not possible, it falls back to `~/.local/bin` and prints the exact command to use.

#### Windows PowerShell

```powershell
irm https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.ps1 | iex
```

### Command Name

After installation, run the app from any directory with:

```bash
remnasync
```

Compatibility alias:

```bash
sync-remnawave
```

### Setup Wizard

Start the wizard with:

```bash
remnasync init
```

The wizard will ask for:

- language
- source panel URL
- source token
- destination panel URL
- destination token
- whether to sync users
- whether to sync squads
- whether to sync nodes
- whether missing imported users should be disabled or deleted
- sync times in `HH:MM` format, separated by spaces

Example:

```text
03:00 04:00 12:00 14:00 20:00 23:59
```

On Linux/macOS, these times are automatically written to `crontab`.

The wizard does not start synchronization silently:

1. it shows a summary of your settings
2. it asks whether to save them
3. if the wizard was started automatically because config is missing, it asks whether to start sync now

### Run The Sync

Safe test run:

```bash
remnasync --dry-run
```

Real sync:

```bash
remnasync
```

### Useful Commands

```bash
remnasync init
remnasync --dry-run
remnasync --sync-squads --sync-users
remnasync --sync-squads --sync-users --sync-nodes
```

### Custom Config File

```bash
remnasync init --config-file ./my-sync.env
remnasync --config-file ./my-sync.env --dry-run
remnasync --config-file ./my-sync.env
```

Default config location:

- Windows: `%APPDATA%\SyncRemnawave\.env`
- Linux/macOS: `~/.config/SyncRemnawave/.env`

### Manual Install

```bash
python -m pip install --upgrade --force-reinstall --no-cache-dir "git+https://github.com/LaRsonOFFai/SyncRemnawave.git"
remnasync init
remnasync --dry-run
```

### Notes

- Authentication uses `Authorization: Bearer ...`
- The app also sends `X-Api-Key` for compatibility with some deployments
- Squad metadata is not available in the current official Remnawave OpenAPI, so squad identity falls back to the state file
- User and node metadata are synced through official metadata endpoints when available

---

## Русский

### Что Делает Программа

`SyncRemnawave` синхронизирует данные в одну сторону между двумя панелями Remnawave через официальный REST API.

- Панель A = источник
- Панель B = назначение
- Данные читаются только из A
- Данные записываются только в B

Можно синхронизировать:

- squads
- users
- nodes, только если вы включите синхронизацию нод

### Что Нужно Подготовить

Перед настройкой подготовьте:

- URL панели-источника
- JWT или Bearer token панели-источника
- URL панели-назначения
- JWT или Bearer token панели-назначения

Указывайте базовый адрес панели, например:

```text
https://panel-a.example.com
https://panel-b.example.com
```

Не добавляйте `/api` вручную.

### Установка Одной Командой

#### Linux / macOS

```bash
curl -fsSL https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.sh | bash
```

Установщик пытается положить `remnasync` в `/usr/local/bin`, если на это есть права.
Если это невозможно, он использует `~/.local/bin` и показывает точную команду для запуска.

#### Windows PowerShell

```powershell
irm https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.ps1 | iex
```

### Имя Команды

После установки программу можно запускать из любой папки так:

```bash
remnasync
```

Совместимый алиас тоже оставлен:

```bash
sync-remnawave
```

### Мастер Настройки

Запуск мастера:

```bash
remnasync init
```

Мастер спросит:

- какой язык использовать
- URL панели-источника
- токен панели-источника
- URL панели-назначения
- токен панели-назначения
- нужно ли синхронизировать users
- нужно ли синхронизировать squads
- нужно ли синхронизировать nodes
- что делать с импортированными пользователями, которых больше нет на источнике
- время запуска синхронизации в формате `ЧЧ:ММ` через пробел

Пример:

```text
03:00 04:00 12:00 14:00 20:00 23:59
```

На Linux/macOS это расписание автоматически записывается в `crontab`.

Мастер настройки не запускает синхронизацию молча:

1. сначала показывает итоговую сводку настроек
2. потом спрашивает, сохранять ли их
3. если мастер открылся автоматически из-за отсутствия конфига, отдельно спрашивает, запускать ли синхронизацию прямо сейчас

### Запуск Синхронизации

Безопасная проверка:

```bash
remnasync --dry-run
```

Реальная синхронизация:

```bash
remnasync
```

### Полезные Команды

```bash
remnasync init
remnasync --dry-run
remnasync --sync-squads --sync-users
remnasync --sync-squads --sync-users --sync-nodes
```

### Свой Файл Конфигурации

```bash
remnasync init --config-file ./my-sync.env
remnasync --config-file ./my-sync.env --dry-run
remnasync --config-file ./my-sync.env
```

Конфиг по умолчанию хранится здесь:

- Windows: `%APPDATA%\SyncRemnawave\.env`
- Linux/macOS: `~/.config/SyncRemnawave/.env`

### Ручная Установка

```bash
python -m pip install --upgrade --force-reinstall --no-cache-dir "git+https://github.com/LaRsonOFFai/SyncRemnawave.git"
remnasync init
remnasync --dry-run
```

### Примечания

- Для авторизации используется `Authorization: Bearer ...`
- Дополнительно программа отправляет `X-Api-Key` для совместимости с некоторыми установками
- Для `squads` в текущем официальном OpenAPI Remnawave нет metadata endpoints, поэтому используется fallback через state file
- Для `users` и `nodes` metadata синхронизируется через официальные endpoints, если они доступны
