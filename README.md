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

### What You Need Before Starting

You will need:

- the URL of the source panel
- the token of the source panel
- the URL of the destination panel
- the token of the destination panel

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

#### Windows PowerShell

```powershell
irm https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.ps1 | iex
```

The installer will:

1. install the app
2. open the setup wizard
3. ask you for your panel URLs, tokens, and sync options

### First Setup

You can start the setup wizard at any time with:

```bash
sync-remnawave init
```

The wizard will ask:

- where to read data from
- where to write data to
- whether to sync users
- whether to sync squads
- whether to sync nodes
- whether missing imported users should be disabled or deleted

The setup wizard does not start synchronization by itself. It saves your configuration first, and then you can start a dry run or a real sync yourself.

### Run The Sync

#### Safe test run

```bash
sync-remnawave --dry-run
```

This does not write anything to the destination panel. It only shows what would happen.

#### Real sync

```bash
sync-remnawave
```

### Useful Commands

```bash
sync-remnawave init
sync-remnawave --dry-run
sync-remnawave --sync-squads --sync-users
sync-remnawave --sync-squads --sync-users --sync-nodes
```

### Custom Config File

If you want to store the config in a custom location:

```bash
sync-remnawave init --config-file ./my-sync.env
sync-remnawave --config-file ./my-sync.env --dry-run
sync-remnawave --config-file ./my-sync.env
```

By default the config file is stored here:

- Windows: `%APPDATA%\SyncRemnawave\.env`
- Linux/macOS: `~/.config/SyncRemnawave/.env`

### Manual Install

If you do not want to use the one-line installer:

```bash
python -m pip install "git+https://github.com/LaRsonOFFai/SyncRemnawave.git"
sync-remnawave init
sync-remnawave --dry-run
```

### Notes

- Authentication uses `Authorization: Bearer ...`
- The app also sends `X-Api-Key` for compatibility with some deployments
- Squads do not have official metadata endpoints in the current Remnawave OpenAPI, so the app uses a state file fallback for squad identity
- User and node metadata are synced through official metadata endpoints when available

---

## Русский

### Что Делает Эта Программа

`SyncRemnawave` синхронизирует данные в одну сторону между двумя панелями Remnawave через официальный REST API.

- Панель A = источник
- Панель B = назначение
- Данные читаются только из A
- Данные записываются только в B

Можно синхронизировать:

- squads
- users
- nodes, только если вы включите синхронизацию нод

### Что Нужно Перед Началом

Подготовьте:

- URL панели-источника
- токен панели-источника
- URL панели-назначения
- токен панели-назначения

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

#### Windows PowerShell

```powershell
irm https://raw.githubusercontent.com/LaRsonOFFai/SyncRemnawave/main/install.ps1 | iex
```

Установщик:

1. установит программу
2. откроет мастер настройки
3. попросит ввести URL панелей, токены и опции синхронизации

### Первая Настройка

Если нужно заново открыть мастер настройки:

```bash
sync-remnawave init
```

Мастер спросит:

- откуда читать данные
- куда записывать данные
- нужно ли синхронизировать users
- нужно ли синхронизировать squads
- нужно ли синхронизировать nodes
- что делать с импортированными пользователями, которых больше нет на источнике

Мастер настройки сам по себе не запускает синхронизацию. Сначала он сохраняет настройки, а затем вы уже отдельно запускаете `--dry-run` или обычную синхронизацию.

### Запуск Синхронизации

#### Безопасная проверка

```bash
sync-remnawave --dry-run
```

В этом режиме ничего не записывается в панель назначения. Программа только показывает, что она собирается сделать.

#### Реальная синхронизация

```bash
sync-remnawave
```

### Полезные Команды

```bash
sync-remnawave init
sync-remnawave --dry-run
sync-remnawave --sync-squads --sync-users
sync-remnawave --sync-squads --sync-users --sync-nodes
```

### Свой Файл Конфигурации

Если вы хотите хранить настройки в своём файле:

```bash
sync-remnawave init --config-file ./my-sync.env
sync-remnawave --config-file ./my-sync.env --dry-run
sync-remnawave --config-file ./my-sync.env
```

По умолчанию конфиг сохраняется сюда:

- Windows: `%APPDATA%\SyncRemnawave\.env`
- Linux/macOS: `~/.config/SyncRemnawave/.env`

### Ручная Установка

Если не хотите использовать установку одной строкой:

```bash
python -m pip install "git+https://github.com/LaRsonOFFai/SyncRemnawave.git"
sync-remnawave init
sync-remnawave --dry-run
```

### Примечания

- Для авторизации используется `Authorization: Bearer ...`
- Дополнительно программа отправляет `X-Api-Key` для совместимости с некоторыми установками
- Для `squads` в текущем официальном OpenAPI Remnawave нет metadata endpoints, поэтому для них используется fallback через state file
- Для `users` и `nodes` metadata синхронизируется через официальные endpoints, если они доступны
