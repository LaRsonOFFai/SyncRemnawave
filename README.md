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
- infra billing providers and node billing records together with node sync

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

When you run `remnasync` in an interactive terminal with no extra arguments, it opens a simple menu where you can:

- start synchronization
- run `dry-run`
- create or restore a panel backup
- open the full setup wizard
- quickly toggle saved options such as node sync on or off
- check for app updates and install them

Pressing `Ctrl+C` once returns you to the main menu instead of showing a traceback. Press `Ctrl+C` three times quickly to exit the program.

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

### Backup And Restore

Open the interactive menu:

```bash
remnasync
```

Choose `Backup / restore panel`.

The backup tool will ask for the Remnawave panel directory. The default path is:

```text
/opt/remnawave
```

You can keep the default or enter a custom path. Backups are saved locally by default:

```text
~/.config/SyncRemnawave/backups
```

You can also add one or more S3-compatible accounts from the same menu. For S3 you need:

- endpoint URL, or empty value for AWS
- region
- bucket
- prefix
- access key
- secret key

While entering backup settings, type `q`, `back`, or `exit` to cancel and return to the menu. The backup menu also lets you view configured accounts, delete S3 accounts, and set a retention period for old local/S3 backups. Retention is disabled by default; set it to `0` to keep it disabled.

The same backup menu can configure Telegram notifications. You need:

- `BOT_TOKEN` from `@BotFather`, for example `123456789:ABC...`
- `CHAT_ID` for a user, group, or channel
- optional `TOPIC_ID` for Telegram forum groups

Do not enter the bot username like `@my_backup_bot`; Telegram API requires the numeric token from BotFather.

S3 settings are stored in:

```text
~/.config/SyncRemnawave/backup.json
```

S3 and Telegram settings are stored in the same file. It is created with owner-only permissions where the operating system supports it.

During restore you can either extract the backup into a safe restore folder or restore directly into the panel path. Direct restore first moves the existing panel directory aside to a `.pre_restore_YYYYMMDD_HHMMSS` folder. After backup or restore, Telegram notifications are sent when configured. After restore, the app can immediately start panel synchronization, and it will suggest doing that automatically when the backup is older than 24 hours.

### Updating The App

Open the interactive menu:

```bash
remnasync
```

Choose `Update SyncRemnawave`. The app checks the Git branch it was installed from and reinstalls itself when a newer commit is available. Restart `remnasync` after updating.
If the installed version is already current, the app prints that the latest version is installed and stays in the menu.
If a new version is available, the app asks for confirmation before installing it.

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
- Infra billing sync uses the official `infra-billing/providers` and `infra-billing/nodes` API routes
- Infra billing history records are not mirrored automatically
- The built-in backup tool archives the panel directory. If your database lives outside that directory, make sure your deployment stores the required data under the selected path or add a separate database dump to your operational backup plan.

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
- infra billing providers и billing records нод вместе с node sync

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

Если запустить `remnasync` в обычном интерактивном терминале без дополнительных аргументов, откроется простое меню, где можно:

- запустить синхронизацию
- запустить `dry-run`
- сделать или восстановить бекап панели
- открыть полный мастер настройки
- быстро переключить сохранённые опции, например включить или выключить синхронизацию nodes
- проверить обновления программы и установить их

Одно нажатие `Ctrl+C` возвращает в главное меню без traceback. Чтобы закрыть программу, нажмите `Ctrl+C` три раза быстро.

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

### Бекап И Восстановление

Откройте интерактивное меню:

```bash
remnasync
```

Выберите `Бекап / восстановление панели`.

Бекапер спросит путь к папке панели Remnawave. Стандартный путь:

```text
/opt/remnawave
```

Можно оставить стандартный путь или указать свой. По умолчанию бекапы сохраняются локально:

```text
~/.config/SyncRemnawave/backups
```

В этом же меню можно добавить один или несколько S3-compatible аккаунтов. Для S3 нужны:

- endpoint URL, либо пустое значение для AWS
- region
- bucket
- prefix
- access key
- secret key

Во время ввода backup-настроек можно ввести `q`, `й`, `back` или `exit`, чтобы отменить действие и вернуться в меню. В меню бекапера также можно посмотреть текущие аккаунты, удалить S3 аккаунт и настроить retention для старых локальных/S3 бекапов. Retention по умолчанию отключен; значение `0` оставляет его выключенным.

В этом же меню можно настроить Telegram уведомления. Для этого нужны:

- `BOT_TOKEN` от `@BotFather`, например `123456789:ABC...`
- `CHAT_ID` пользователя, группы или канала
- опциональный `TOPIC_ID` для Telegram forum-групп

Не вводите username бота вида `@my_backup_bot`; Telegram API нужен именно числовой token от BotFather.

Настройки S3 хранятся здесь:

```text
~/.config/SyncRemnawave/backup.json
```

Настройки S3 и Telegram хранятся в одном файле. Файл создаётся с правами только для владельца, если операционная система это поддерживает.

При восстановлении можно распаковать бекап в безопасную отдельную папку или восстановить прямо в путь панели. При прямом восстановлении текущая папка панели сначала переносится в `.pre_restore_YYYYMMDD_HHMMSS`. Если Telegram настроен, после backup или restore программа отправит уведомление. После восстановления программа может сразу запустить синхронизацию панелей, а если бекап старше 24 часов, она предложит это автоматически.

### Обновление Программы

Откройте интерактивное меню:

```bash
remnasync
```

Выберите `Обновить SyncRemnawave`. Программа проверит Git ветку, из которой была установлена, и переустановит себя, если появился новый commit. После обновления перезапустите `remnasync`.
Если установлена актуальная версия, программа напишет, что версия последняя, и останется в меню.
Если доступна новая версия, программа сначала спросит подтверждение на установку.

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
- Infra billing синхронизируется через официальные routes `infra-billing/providers` и `infra-billing/nodes`
- История infra billing платежей автоматически не зеркалируется
- Встроенный бекапер архивирует папку панели. Если база данных находится вне этой папки, убедитесь, что нужные данные действительно лежат в выбранном пути, или добавьте отдельный dump базы в свой production-план бекапов.
