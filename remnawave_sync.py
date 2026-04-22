from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, TextIO

import httpx
from dotenv import load_dotenv
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)


LOGGER = logging.getLogger("remnawave_sync")
SOURCE_PANEL_LABEL = "A"
APP_NAME = "SyncRemnawave"


class SyncError(Exception):
    pass


class RetriableHttpError(SyncError):
    def __init__(self, message: str, response: httpx.Response | None = None) -> None:
        super().__init__(message)
        self.response = response


class FatalApiError(SyncError):
    pass


class IdempotentNoOpApiError(SyncError):
    def __init__(self, message: str, error_code: str | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def normalize_base_url(url: str) -> str:
    return url.rstrip("/")


def default_config_dir() -> Path:
    if os.name == "nt":
        base_dir = Path(os.getenv("APPDATA", Path.home()))
    else:
        base_dir = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base_dir / APP_NAME


def default_config_file() -> Path:
    return default_config_dir() / ".env"


def default_state_file() -> Path:
    return default_config_dir() / "sync_state.json"


def build_auth_headers(api_key: str) -> dict[str, str]:
    token = api_key.strip()
    if not token:
        raise ValueError("Empty API key/token provided")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    if token.lower().startswith("bearer "):
        bearer_value = token
        raw_token = token.split(" ", 1)[1].strip()
    else:
        bearer_value = f"Bearer {token}"
        raw_token = token

    headers["Authorization"] = bearer_value
    headers["X-Api-Key"] = raw_token
    return headers


def clean_none(data: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


def bool_to_env(value: bool) -> str:
    return "true" if value else "false"


@contextmanager
def open_console_streams() -> Iterator[tuple[TextIO, TextIO]]:
    if sys.stdin.isatty() and sys.stdout.isatty():
        yield sys.stdin, sys.stdout
        return

    if os.name == "nt":
        input_target = "CONIN$"
        output_target = "CONOUT$"
    else:
        input_target = "/dev/tty"
        output_target = "/dev/tty"

    try:
        console_in = open(input_target, "r", encoding="utf-8", newline="")
        console_out = open(output_target, "w", encoding="utf-8", newline="")
    except OSError as exc:
        raise SyncError(
            "Interactive setup requires a terminal. Run 'sync-remnawave init' directly in a shell session."
        ) from exc

    try:
        yield console_in, console_out
    finally:
        console_in.close()
        console_out.close()


def read_prompt_line(prompt: str) -> str:
    with open_console_streams() as (console_in, console_out):
        console_out.write(prompt)
        console_out.flush()
        line = console_in.readline()
    if line == "":
        raise SyncError("Interactive setup was cancelled because no terminal input is available.")
    return line.rstrip("\r\n")


def prompt_text(label: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = read_prompt_line(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        print("Value is required.")


def prompt_secret(label: str, has_default: bool = False) -> str:
    suffix = " [saved]" if has_default else ""
    while True:
        try:
            with open_console_streams() as (_, console_out):
                value = getpass.getpass(f"{label}{suffix}: ", stream=console_out).strip()
        except (EOFError, KeyboardInterrupt) as exc:
            raise SyncError("Interactive setup was cancelled.") from exc
        if value:
            return value
        if has_default:
            return ""
        print("Value is required.")


def prompt_bool(label: str, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        value = read_prompt_line(f"{label} [{suffix}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes", "1", "true"}:
            return True
        if value in {"n", "no", "0", "false"}:
            return False
        print("Please answer yes or no.")


def prompt_int(label: str, default: int) -> int:
    while True:
        value = read_prompt_line(f"{label} [{default}]: ").strip()
        if not value:
            return default
        try:
            return int(value)
        except ValueError:
            print("Please enter an integer.")


def prompt_float(label: str, default: float) -> float:
    while True:
        value = read_prompt_line(f"{label} [{default}]: ").strip()
        if not value:
            return default
        try:
            return float(value)
        except ValueError:
            print("Please enter a number.")


def write_env_file(path: Path, values: Mapping[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
    path.write_text(content, encoding="utf-8")


def run_setup_wizard(config_file: Path) -> int:
    config_file = config_file.expanduser()
    existing_env: dict[str, str] = {}
    if config_file.exists():
        for line in config_file.read_text(encoding="utf-8").splitlines():
            if "=" not in line or line.strip().startswith("#"):
                continue
            key, value = line.split("=", 1)
            existing_env[key.strip()] = value.strip()

    default_state = existing_env.get("STATE_FILE", str(default_state_file()))

    print(f"{APP_NAME} setup wizard")
    print("This will create or update your personal .env config file.")
    print(f"Config path: {config_file}")
    print()

    src_url = normalize_base_url(prompt_text("Source panel URL", existing_env.get("SRC_URL", "https://panel-a.example.com")))
    src_api_key = prompt_secret("Source JWT or Bearer token", has_default="SRC_API_KEY" in existing_env) or existing_env.get("SRC_API_KEY", "")
    dst_url = normalize_base_url(prompt_text("Destination panel URL", existing_env.get("DST_URL", "https://panel-b.example.com")))
    dst_api_key = prompt_secret("Destination JWT or Bearer token", has_default="DST_API_KEY" in existing_env) or existing_env.get("DST_API_KEY", "")
    enable_user_sync = prompt_bool("Sync users", parse_bool(existing_env.get("ENABLE_USER_SYNC"), True))
    enable_squad_sync = prompt_bool("Sync squads", parse_bool(existing_env.get("ENABLE_SQUAD_SYNC"), True))
    enable_node_sync = prompt_bool("Sync nodes", parse_bool(existing_env.get("ENABLE_NODE_SYNC"), False))
    disable_missing_users = prompt_bool("Disable imported users missing on source", parse_bool(existing_env.get("DISABLE_MISSING_USERS"), True))
    delete_missing_users = prompt_bool("Delete imported users missing on source", parse_bool(existing_env.get("DELETE_MISSING_USERS"), False))
    page_size = prompt_int("User page size", int(existing_env.get("PAGE_SIZE", "100")))
    request_timeout = prompt_float("HTTP request timeout in seconds", float(existing_env.get("REQUEST_TIMEOUT", "20")))
    state_file = prompt_text("State file path", default_state)
    log_level = prompt_text("Log level", existing_env.get("LOG_LEVEL", "INFO")).upper()

    write_env_file(
        config_file,
        {
            "SRC_URL": src_url,
            "SRC_API_KEY": src_api_key,
            "DST_URL": dst_url,
            "DST_API_KEY": dst_api_key,
            "ENABLE_USER_SYNC": bool_to_env(enable_user_sync),
            "ENABLE_SQUAD_SYNC": bool_to_env(enable_squad_sync),
            "ENABLE_NODE_SYNC": bool_to_env(enable_node_sync),
            "DISABLE_MISSING_USERS": bool_to_env(disable_missing_users),
            "DELETE_MISSING_USERS": bool_to_env(delete_missing_users),
            "PAGE_SIZE": str(page_size),
            "REQUEST_TIMEOUT": str(request_timeout),
            "STATE_FILE": state_file,
            "LOG_LEVEL": log_level,
        },
    )

    print()
    print(f"Configuration saved to: {config_file}")
    print("Run the sync with:")
    print("  sync-remnawave --dry-run")
    print("  sync-remnawave")
    return 0


def diff_dict(old: Mapping[str, Any], new: Mapping[str, Any], allowed_fields: Iterable[str]) -> dict[str, Any]:
    changed: dict[str, Any] = {}
    for field_name in allowed_fields:
        if field_name not in new:
            continue
        if old.get(field_name) != new.get(field_name):
            changed[field_name] = new[field_name]
    return changed


@dataclass(slots=True)
class SyncConfig:
    src_url: str
    src_api_key: str
    dst_url: str
    dst_api_key: str
    enable_user_sync: bool = True
    enable_squad_sync: bool = True
    enable_node_sync: bool = False
    disable_missing_users: bool = True
    delete_missing_users: bool = False
    page_size: int = 100
    request_timeout: float = 20.0
    dry_run: bool = False
    log_level: str = "INFO"
    state_file: Path = field(default_factory=default_state_file)

    @classmethod
    def from_env_and_args(cls, args: argparse.Namespace) -> "SyncConfig":
        src_url = os.getenv("SRC_URL", "").strip()
        src_api_key = os.getenv("SRC_API_KEY", "").strip()
        dst_url = os.getenv("DST_URL", "").strip()
        dst_api_key = os.getenv("DST_API_KEY", "").strip()

        if not src_url or not src_api_key or not dst_url or not dst_api_key:
            raise ValueError("SRC_URL, SRC_API_KEY, DST_URL and DST_API_KEY must be set")

        enable_user_sync = parse_bool(os.getenv("ENABLE_USER_SYNC"), True)
        enable_squad_sync = parse_bool(os.getenv("ENABLE_SQUAD_SYNC"), True)
        enable_node_sync = parse_bool(os.getenv("ENABLE_NODE_SYNC"), False)
        disable_missing_users = parse_bool(os.getenv("DISABLE_MISSING_USERS"), True)
        delete_missing_users = parse_bool(os.getenv("DELETE_MISSING_USERS"), False)
        page_size = int(os.getenv("PAGE_SIZE", "100"))
        request_timeout = float(os.getenv("REQUEST_TIMEOUT", "20"))
        state_file = Path(os.getenv("STATE_FILE", str(default_state_file()))).expanduser()

        if args.sync_users:
            enable_user_sync = True
        if args.sync_squads:
            enable_squad_sync = True
        if args.sync_nodes:
            enable_node_sync = True
        if args.disable_missing_users:
            disable_missing_users = True
        if args.delete_missing_users:
            delete_missing_users = True
        if args.page_size is not None:
            page_size = args.page_size
        if args.log_level is not None:
            log_level = args.log_level
        else:
            log_level = os.getenv("LOG_LEVEL", "INFO")

        return cls(
            src_url=normalize_base_url(src_url),
            src_api_key=src_api_key,
            dst_url=normalize_base_url(dst_url),
            dst_api_key=dst_api_key,
            enable_user_sync=enable_user_sync,
            enable_squad_sync=enable_squad_sync,
            enable_node_sync=enable_node_sync,
            disable_missing_users=disable_missing_users,
            delete_missing_users=delete_missing_users,
            page_size=page_size,
            request_timeout=request_timeout,
            dry_run=args.dry_run,
            log_level=log_level,
            state_file=Path(args.state_file).expanduser() if args.state_file else state_file,
        )


@dataclass(slots=True)
class Summary:
    squads_created: int = 0
    squads_updated: int = 0
    users_created: int = 0
    users_updated: int = 0
    users_disabled: int = 0
    users_deleted: int = 0
    nodes_created: int = 0
    nodes_updated: int = 0
    errors: int = 0


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "squads": {"internal": {}, "external": {}},
                "users": {},
                "nodes": {},
            }
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SyncError(f"Failed to read state file {self.path}: {exc}") from exc

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")

    def get_mapping(self, entity: str, source_uuid: str, kind: str | None = None) -> str | None:
        bucket = self._bucket(entity, kind)
        return bucket.get(source_uuid)

    def set_mapping(self, entity: str, source_uuid: str, dest_uuid: str, kind: str | None = None) -> None:
        bucket = self._bucket(entity, kind)
        bucket[source_uuid] = dest_uuid

    def remove_mapping(self, entity: str, source_uuid: str, kind: str | None = None) -> None:
        bucket = self._bucket(entity, kind)
        bucket.pop(source_uuid, None)

    def reverse_mapping(self, entity: str, kind: str | None = None) -> dict[str, str]:
        bucket = self._bucket(entity, kind)
        return {dest_uuid: source_uuid for source_uuid, dest_uuid in bucket.items()}

    def _bucket(self, entity: str, kind: str | None = None) -> dict[str, str]:
        if entity == "squads":
            if kind not in {"internal", "external"}:
                raise ValueError("Squad kind must be 'internal' or 'external'")
            return self.data.setdefault("squads", {}).setdefault(kind, {})
        return self.data.setdefault(entity, {})


def _is_retryable_exception(exc: BaseException) -> bool:
    if isinstance(exc, RetriableHttpError):
        return True
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.PoolTimeout)):
        return True
    return False


def _log_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    LOGGER.warning(
        "Retrying HTTP call after attempt %s due to %s",
        retry_state.attempt_number,
        exc,
    )


class RemnawaveClient:
    def __init__(self, base_url: str, api_key: str, timeout: float, name: str) -> None:
        self.base_url = base_url
        self.name = name
        self.client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers=build_auth_headers(api_key),
        )

    def close(self) -> None:
        self.client.close()

    @retry(
        retry=retry_if_exception(_is_retryable_exception),
        wait=wait_exponential(multiplier=1, min=1, max=20),
        stop=stop_after_attempt(5),
        before_sleep=_log_retry,
        reraise=True,
    )
    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        expected_statuses: set[int] | None = None,
    ) -> Any:
        expected = expected_statuses or {200, 201}
        try:
            response = self.client.request(method, path, params=params, json=json_body)
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.PoolTimeout) as exc:
            LOGGER.error("%s request %s %s failed: %s", self.name, method, path, exc)
            raise

        if response.status_code in {429, 500, 502, 503, 504}:
            raise RetriableHttpError(
                f"{self.name} {method} {path} returned {response.status_code}: {response.text}",
                response=response,
            )

        if response.status_code not in expected:
            error_code: str | None = None
            error_message: str | None = None
            try:
                error_payload = response.json()
            except ValueError:
                error_payload = None

            if isinstance(error_payload, dict):
                raw_error_code = error_payload.get("errorCode")
                raw_message = error_payload.get("message")
                error_code = raw_error_code if isinstance(raw_error_code, str) else None
                error_message = raw_message if isinstance(raw_message, str) else None

            if response.status_code == 400 and error_code in {"A029", "A030"}:
                raise IdempotentNoOpApiError(
                    error_message or f"{self.name} {method} {path} is already in target state",
                    error_code=error_code,
                )

            LOGGER.error(
                "%s request %s %s failed with status %s: %s",
                self.name,
                method,
                path,
                response.status_code,
                response.text,
            )
            raise FatalApiError(f"{self.name} {method} {path} failed with status {response.status_code}")

        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    def unwrap_response(self, payload: Any) -> Any:
        if isinstance(payload, dict) and "response" in payload:
            return payload["response"]
        return payload

    def extract_items(self, payload: Any, candidates: list[str]) -> tuple[list[dict[str, Any]], int | None]:
        response = self.unwrap_response(payload)
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)], len(response)
        if isinstance(response, dict):
            total = response.get("total")
            for candidate in candidates:
                value = response.get(candidate)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)], total if isinstance(total, int | float) else None
        raise SyncError(f"Unexpected list response shape from {self.name}: {payload!r}")

    def list_users(self, page_size: int) -> list[dict[str, Any]]:
        users: list[dict[str, Any]] = []
        start = 0
        while True:
            payload = self.request("GET", "/api/users", params={"size": page_size, "start": start})
            items, total = self.extract_items(payload, ["users"])
            users.extend(items)
            if not items:
                break
            if total is not None and len(users) >= int(total):
                break
            if len(items) < page_size:
                break
            start += len(items)
        return users

    def list_internal_squads(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/api/internal-squads")
        items, _ = self.extract_items(payload, ["internalSquads"])
        return items

    def list_external_squads(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/api/external-squads")
        items, _ = self.extract_items(payload, ["externalSquads"])
        return items

    def list_nodes(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/api/nodes")
        items, _ = self.extract_items(payload, ["nodes"])
        return items

    def list_config_profiles(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/api/config-profiles")
        items, _ = self.extract_items(payload, ["configProfiles"])
        return items

    def list_inbounds(self) -> list[dict[str, Any]]:
        payload = self.request("GET", "/api/config-profiles/inbounds")
        items, _ = self.extract_items(payload, ["inbounds"])
        return items

    def create_user(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("POST", "/api/users", json_body=payload))

    def update_user(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("PATCH", "/api/users", json_body=payload))

    def delete_user(self, user_uuid: str) -> None:
        self.request("DELETE", f"/api/users/{user_uuid}", expected_statuses={200})

    def disable_user(self, user_uuid: str) -> None:
        self.request("POST", f"/api/users/{user_uuid}/actions/disable", expected_statuses={200})

    def enable_user(self, user_uuid: str) -> None:
        self.request("POST", f"/api/users/{user_uuid}/actions/enable", expected_statuses={200})

    def create_internal_squad(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("POST", "/api/internal-squads", json_body=payload))

    def update_internal_squad(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("PATCH", "/api/internal-squads", json_body=payload))

    def create_external_squad(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("POST", "/api/external-squads", json_body=payload))

    def update_external_squad(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("PATCH", "/api/external-squads", json_body=payload))

    def create_node(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("POST", "/api/nodes", json_body=payload))

    def update_node(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self.unwrap_response(self.request("PATCH", "/api/nodes", json_body=payload))

    def disable_node(self, node_uuid: str) -> None:
        self.request("POST", f"/api/nodes/{node_uuid}/actions/disable", expected_statuses={200})

    def enable_node(self, node_uuid: str) -> None:
        self.request("POST", f"/api/nodes/{node_uuid}/actions/enable", expected_statuses={200})

    def get_user_metadata(self, user_uuid: str) -> dict[str, Any] | None:
        try:
            payload = self.request("GET", f"/api/metadata/user/{user_uuid}", expected_statuses={200, 404})
        except FatalApiError:
            raise
        if payload is None:
            return None
        if isinstance(payload, dict) and "response" not in payload:
            return None
        response = self.unwrap_response(payload)
        if isinstance(response, dict):
            value = response.get("metadata")
            return value if isinstance(value, dict) else None
        return None

    def put_user_metadata(self, user_uuid: str, metadata: Mapping[str, Any]) -> None:
        self.request(
            "PUT",
            f"/api/metadata/user/{user_uuid}",
            json_body={"metadata": metadata},
            expected_statuses={200},
        )

    def get_node_metadata(self, node_uuid: str) -> dict[str, Any] | None:
        payload = self.request("GET", f"/api/metadata/node/{node_uuid}", expected_statuses={200, 404})
        if payload is None:
            return None
        if isinstance(payload, dict) and "response" not in payload:
            return None
        response = self.unwrap_response(payload)
        if isinstance(response, dict):
            value = response.get("metadata")
            return value if isinstance(value, dict) else None
        return None

    def put_node_metadata(self, node_uuid: str, metadata: Mapping[str, Any]) -> None:
        self.request(
            "PUT",
            f"/api/metadata/node/{node_uuid}",
            json_body={"metadata": metadata},
            expected_statuses={200},
        )


class MetadataMapper:
    def __init__(self, state_store: StateStore) -> None:
        self.state_store = state_store

    def imported_metadata(self, source_uuid: str) -> dict[str, str]:
        return {
            "source_panel": SOURCE_PANEL_LABEL,
            "source_uuid": source_uuid,
        }

    def get_source_uuid_from_metadata(self, metadata: Mapping[str, Any] | None) -> str | None:
        if not metadata:
            return None
        source_panel = metadata.get("source_panel")
        source_uuid = metadata.get("source_uuid")
        if source_panel == SOURCE_PANEL_LABEL and isinstance(source_uuid, str):
            return source_uuid
        return None


def map_squad_payload(source_squad: Mapping[str, Any]) -> dict[str, Any]:
    if "inbounds" in source_squad:
        return clean_none(
            {
                "name": source_squad.get("name"),
                "inbounds": source_squad.get("inbounds"),
            }
        )
    return clean_none(
        {
            "name": source_squad.get("name"),
            "templates": source_squad.get("templates"),
            "subscriptionSettings": source_squad.get("subscriptionSettings"),
            "hostOverrides": source_squad.get("hostOverrides"),
            "responseHeaders": source_squad.get("responseHeaders"),
            "hwidSettings": source_squad.get("hwidSettings"),
            "customRemarks": source_squad.get("customRemarks"),
            "subpageConfigUuid": source_squad.get("subpageConfigUuid"),
        }
    )


def map_user_payload(source_user: Mapping[str, Any], squad_mapping: Mapping[str, Any]) -> dict[str, Any]:
    active_internal_squads: list[str] = []
    for squad in source_user.get("activeInternalSquads", []) or []:
        source_uuid = squad.get("uuid") if isinstance(squad, dict) else None
        if source_uuid:
            dest_uuid = squad_mapping.get(("internal", source_uuid))
            if dest_uuid:
                active_internal_squads.append(dest_uuid)

    external_squad_uuid = source_user.get("externalSquadUuid")
    mapped_external_squad_uuid = squad_mapping.get(("external", external_squad_uuid)) if external_squad_uuid else None

    return clean_none(
        {
            "username": source_user.get("username"),
            "status": source_user.get("status"),
            "shortUuid": source_user.get("shortUuid"),
            "trojanPassword": source_user.get("trojanPassword"),
            "vlessUuid": source_user.get("vlessUuid"),
            "ssPassword": source_user.get("ssPassword"),
            "trafficLimitBytes": source_user.get("trafficLimitBytes"),
            "trafficLimitStrategy": source_user.get("trafficLimitStrategy"),
            "expireAt": source_user.get("expireAt"),
            "createdAt": source_user.get("createdAt"),
            "lastTrafficResetAt": source_user.get("lastTrafficResetAt"),
            "description": source_user.get("description"),
            "tag": source_user.get("tag"),
            "telegramId": source_user.get("telegramId"),
            "email": source_user.get("email"),
            "hwidDeviceLimit": source_user.get("hwidDeviceLimit"),
            "activeInternalSquads": active_internal_squads,
            "externalSquadUuid": mapped_external_squad_uuid,
        }
    )


def map_node_payload(source_node: Mapping[str, Any]) -> dict[str, Any]:
    config_profile = source_node.get("configProfile") or {}
    active_inbounds = config_profile.get("activeInbounds") or []
    inbound_uuids = [
        inbound.get("uuid")
        for inbound in active_inbounds
        if isinstance(inbound, dict) and inbound.get("uuid")
    ]
    payload = clean_none(
        {
            "name": source_node.get("name"),
            "address": source_node.get("address"),
            "port": source_node.get("port"),
            "isTrafficTrackingActive": source_node.get("isTrafficTrackingActive"),
            "trafficLimitBytes": source_node.get("trafficLimitBytes"),
            "notifyPercent": source_node.get("notifyPercent"),
            "trafficResetDay": source_node.get("trafficResetDay"),
            "countryCode": source_node.get("countryCode"),
            "consumptionMultiplier": source_node.get("consumptionMultiplier"),
            "configProfile": {
                "activeConfigProfileUuid": config_profile.get("activeConfigProfileUuid"),
                "activeInbounds": inbound_uuids,
            }
            if config_profile.get("activeConfigProfileUuid") and inbound_uuids
            else None,
            "tags": source_node.get("tags"),
        }
    )
    return payload


def inbound_fingerprint(inbound: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        inbound.get("tag"),
        inbound.get("type"),
        inbound.get("network"),
        inbound.get("security"),
        inbound.get("port"),
    )


def config_profile_fingerprint(profile: Mapping[str, Any]) -> tuple[Any, ...]:
    inbound_keys = sorted(inbound_fingerprint(inbound) for inbound in profile.get("inbounds", []) or [] if isinstance(inbound, dict))
    return (
        profile.get("name"),
        tuple(inbound_keys),
    )


class SquadSyncService:
    def __init__(
        self,
        src: RemnawaveClient,
        dst: RemnawaveClient,
        state_store: StateStore,
        summary: Summary,
        dry_run: bool,
        dst_inbound_by_fingerprint: Mapping[tuple[Any, ...], str],
    ) -> None:
        self.src = src
        self.dst = dst
        self.state_store = state_store
        self.summary = summary
        self.dry_run = dry_run
        self.dst_inbound_by_fingerprint = dst_inbound_by_fingerprint
        self.mapping: dict[tuple[str, str], str] = {}

    def sync(self) -> dict[tuple[str, str], str]:
        self._sync_external()
        self._sync_internal()
        return self.mapping

    def _match_dest_squad(
        self,
        source_squad: Mapping[str, Any],
        dest_by_uuid: Mapping[str, dict[str, Any]],
        dest_by_name: Mapping[str, dict[str, Any]],
        kind: str,
    ) -> dict[str, Any] | None:
        source_uuid = source_squad["uuid"]
        mapped_uuid = self.state_store.get_mapping("squads", source_uuid, kind=kind)
        if mapped_uuid and mapped_uuid in dest_by_uuid:
            return dest_by_uuid[mapped_uuid]
        squad_name = source_squad.get("name")
        if isinstance(squad_name, str):
            return dest_by_name.get(squad_name)
        return None

    def _sync_external(self) -> None:
        source_items = self.src.list_external_squads()
        dest_items = self.dst.list_external_squads()
        dest_by_uuid = {item["uuid"]: item for item in dest_items}
        dest_by_name = {item["name"]: item for item in dest_items}
        source_uuids = {item["uuid"] for item in source_items}

        for source_squad in source_items:
            source_uuid = source_squad["uuid"]
            payload = map_squad_payload(source_squad)
            allowed_update_fields = {
                "name",
                "templates",
                "subscriptionSettings",
                "hostOverrides",
                "responseHeaders",
                "hwidSettings",
                "customRemarks",
                "subpageConfigUuid",
            }
            dest_squad = self._match_dest_squad(source_squad, dest_by_uuid, dest_by_name, "external")

            try:
                if dest_squad is None:
                    if self.dry_run:
                        LOGGER.info("CREATE squad external name=%s", source_squad.get("name"))
                        planned_uuid = f"dry-run-external-{source_uuid}"
                        self.mapping[("external", source_uuid)] = planned_uuid
                    else:
                        created = self.dst.create_external_squad({"name": payload["name"]})
                        final_payload = diff_dict({}, payload, allowed_update_fields)
                        if final_payload:
                            final_payload["uuid"] = created["uuid"]
                            created = self.dst.update_external_squad(final_payload)
                        self.summary.squads_created += 1
                        self.mapping[("external", source_uuid)] = created["uuid"]
                        self.state_store.set_mapping("squads", source_uuid, created["uuid"], kind="external")
                        LOGGER.info("created squad external name=%s uuid=%s", source_squad.get("name"), created["uuid"])
                    continue

                self.mapping[("external", source_uuid)] = dest_squad["uuid"]
                self.state_store.set_mapping("squads", source_uuid, dest_squad["uuid"], kind="external")
                patch = diff_dict(dest_squad, payload, allowed_update_fields)
                if patch:
                    patch["uuid"] = dest_squad["uuid"]
                    if self.dry_run:
                        LOGGER.info("UPDATE squad external name=%s fields=%s", source_squad.get("name"), sorted(patch))
                    else:
                        self.dst.update_external_squad(patch)
                        self.summary.squads_updated += 1
                        LOGGER.info("updated squad external name=%s uuid=%s", source_squad.get("name"), dest_squad["uuid"])
                else:
                    LOGGER.info("skipped squad external name=%s reason=no_changes", source_squad.get("name"))
            except Exception as exc:
                self.summary.errors += 1
                LOGGER.exception("failed syncing external squad %s: %s", source_squad.get("name"), exc)

        imported_dest = self.state_store.reverse_mapping("squads", kind="external")
        for dest_uuid, original_source_uuid in imported_dest.items():
            if original_source_uuid not in source_uuids:
                LOGGER.warning("destination external squad uuid=%s no longer exists on source; not deleting", dest_uuid)

    def _sync_internal(self) -> None:
        source_items = self.src.list_internal_squads()
        dest_items = self.dst.list_internal_squads()
        dest_by_uuid = {item["uuid"]: item for item in dest_items}
        dest_by_name = {item["name"]: item for item in dest_items}
        source_uuids = {item["uuid"] for item in source_items}

        for source_squad in source_items:
            source_uuid = source_squad["uuid"]
            inbound_ids: list[str] = []
            missing_inbounds: list[tuple[Any, ...]] = []
            for inbound in source_squad.get("inbounds", []) or []:
                if not isinstance(inbound, dict):
                    continue
                fingerprint = inbound_fingerprint(inbound)
                dest_inbound_uuid = self.dst_inbound_by_fingerprint.get(fingerprint)
                if dest_inbound_uuid:
                    inbound_ids.append(dest_inbound_uuid)
                else:
                    missing_inbounds.append(fingerprint)

            if missing_inbounds:
                LOGGER.warning(
                    "SKIP squad internal name=%s reason=missing_inbounds fingerprints=%s",
                    source_squad.get("name"),
                    missing_inbounds,
                )
                continue

            payload = {"name": source_squad.get("name"), "inbounds": inbound_ids}
            allowed_update_fields = {"name", "inbounds"}
            dest_squad = self._match_dest_squad(source_squad, dest_by_uuid, dest_by_name, "internal")

            try:
                if dest_squad is None:
                    if self.dry_run:
                        LOGGER.info("CREATE squad internal name=%s", source_squad.get("name"))
                        planned_uuid = f"dry-run-internal-{source_uuid}"
                        self.mapping[("internal", source_uuid)] = planned_uuid
                    else:
                        created = self.dst.create_internal_squad(payload)
                        self.summary.squads_created += 1
                        self.mapping[("internal", source_uuid)] = created["uuid"]
                        self.state_store.set_mapping("squads", source_uuid, created["uuid"], kind="internal")
                        LOGGER.info("created squad internal name=%s uuid=%s", source_squad.get("name"), created["uuid"])
                    continue

                self.mapping[("internal", source_uuid)] = dest_squad["uuid"]
                self.state_store.set_mapping("squads", source_uuid, dest_squad["uuid"], kind="internal")
                current_inbounds = [item.get("uuid") for item in dest_squad.get("inbounds", []) if isinstance(item, dict)]
                current_state = {"name": dest_squad.get("name"), "inbounds": current_inbounds}
                patch = diff_dict(current_state, payload, allowed_update_fields)
                if patch:
                    patch["uuid"] = dest_squad["uuid"]
                    if self.dry_run:
                        LOGGER.info("UPDATE squad internal name=%s fields=%s", source_squad.get("name"), sorted(patch))
                    else:
                        self.dst.update_internal_squad(patch)
                        self.summary.squads_updated += 1
                        LOGGER.info("updated squad internal name=%s uuid=%s", source_squad.get("name"), dest_squad["uuid"])
                else:
                    LOGGER.info("skipped squad internal name=%s reason=no_changes", source_squad.get("name"))
            except Exception as exc:
                self.summary.errors += 1
                LOGGER.exception("failed syncing internal squad %s: %s", source_squad.get("name"), exc)

        imported_dest = self.state_store.reverse_mapping("squads", kind="internal")
        for dest_uuid, original_source_uuid in imported_dest.items():
            if original_source_uuid not in source_uuids:
                LOGGER.warning("destination internal squad uuid=%s no longer exists on source; not deleting", dest_uuid)


class UserSyncService:
    CREATE_ALLOWED_FIELDS = {
        "username",
        "status",
        "shortUuid",
        "trojanPassword",
        "vlessUuid",
        "ssPassword",
        "trafficLimitBytes",
        "trafficLimitStrategy",
        "expireAt",
        "createdAt",
        "lastTrafficResetAt",
        "description",
        "tag",
        "telegramId",
        "email",
        "hwidDeviceLimit",
        "activeInternalSquads",
        "externalSquadUuid",
    }
    UPDATE_ALLOWED_FIELDS = {
        "username",
        "status",
        "trafficLimitBytes",
        "trafficLimitStrategy",
        "expireAt",
        "description",
        "tag",
        "telegramId",
        "email",
        "hwidDeviceLimit",
        "activeInternalSquads",
        "externalSquadUuid",
    }

    def __init__(
        self,
        src: RemnawaveClient,
        dst: RemnawaveClient,
        metadata_mapper: MetadataMapper,
        state_store: StateStore,
        summary: Summary,
        config: SyncConfig,
        squad_mapping: Mapping[tuple[str, str], str],
    ) -> None:
        self.src = src
        self.dst = dst
        self.metadata_mapper = metadata_mapper
        self.state_store = state_store
        self.summary = summary
        self.config = config
        self.squad_mapping = squad_mapping

    def _build_dest_indexes(self, dest_users: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
        by_source_uuid: dict[str, dict[str, Any]] = {}
        by_username: dict[str, dict[str, Any]] = {}
        by_telegram_id: dict[int, dict[str, Any]] = {}
        by_uuid = {user["uuid"]: user for user in dest_users}
        reverse_state = self.state_store.reverse_mapping("users")

        for user in dest_users:
            username = user.get("username")
            telegram_id = user.get("telegramId")
            if isinstance(username, str):
                by_username[username] = user
            if isinstance(telegram_id, int):
                by_telegram_id[telegram_id] = user

            source_uuid = reverse_state.get(user["uuid"])
            if source_uuid:
                by_source_uuid[source_uuid] = user
                continue

            try:
                metadata = self.dst.get_user_metadata(user["uuid"])
            except Exception as exc:
                LOGGER.warning("failed to read metadata for destination user %s: %s", user["uuid"], exc)
                continue

            mapped_source_uuid = self.metadata_mapper.get_source_uuid_from_metadata(metadata)
            if mapped_source_uuid:
                by_source_uuid[mapped_source_uuid] = user
                self.state_store.set_mapping("users", mapped_source_uuid, user["uuid"])

        return by_source_uuid, by_username, by_telegram_id, by_uuid

    def _match_user(
        self,
        source_user: Mapping[str, Any],
        by_source_uuid: Mapping[str, dict[str, Any]],
        by_username: Mapping[str, dict[str, Any]],
        by_telegram_id: Mapping[int, dict[str, Any]],
    ) -> dict[str, Any] | None:
        source_uuid = source_user["uuid"]
        if source_uuid in by_source_uuid:
            return by_source_uuid[source_uuid]
        username = source_user.get("username")
        if isinstance(username, str) and username in by_username:
            return by_username[username]
        telegram_id = source_user.get("telegramId")
        if isinstance(telegram_id, int) and telegram_id in by_telegram_id:
            return by_telegram_id[telegram_id]
        return None

    def _sync_user_status(self, source_user: Mapping[str, Any], dest_user: Mapping[str, Any]) -> None:
        source_status = source_user.get("status")
        dest_uuid = str(dest_user["uuid"])
        dest_status = dest_user.get("status")

        if source_status == "DISABLED":
            if dest_status == "DISABLED":
                LOGGER.info("skipped user status username=%s reason=already_disabled", source_user.get("username"))
                return
            if self.config.dry_run:
                LOGGER.info("DISABLE user username=%s", source_user.get("username"))
            else:
                try:
                    self.dst.disable_user(dest_uuid)
                except IdempotentNoOpApiError:
                    LOGGER.info("skipped user status username=%s reason=already_disabled", source_user.get("username"))
        elif source_status == "ACTIVE":
            if dest_status == "ACTIVE":
                LOGGER.info("skipped user status username=%s reason=already_enabled", source_user.get("username"))
                return
            if self.config.dry_run:
                LOGGER.info("ENABLE user username=%s", source_user.get("username"))
            else:
                try:
                    self.dst.enable_user(dest_uuid)
                except IdempotentNoOpApiError:
                    LOGGER.info("skipped user status username=%s reason=already_enabled", source_user.get("username"))
        elif source_status in {"LIMITED", "EXPIRED"}:
            LOGGER.warning(
                "user %s has status=%s on source, but PATCH /api/users confirms direct updates only for ACTIVE/DISABLED; expireAt and limits were synced, direct status action skipped",
                source_user.get("username"),
                source_status,
            )

    def sync(self) -> None:
        source_users = self.src.list_users(self.config.page_size)
        dest_users = self.dst.list_users(self.config.page_size)
        by_source_uuid, by_username, by_telegram_id, by_uuid = self._build_dest_indexes(dest_users)
        source_user_ids = {user["uuid"] for user in source_users}

        for source_user in source_users:
            try:
                payload = map_user_payload(source_user, self.squad_mapping)
                create_payload = diff_dict({}, payload, self.CREATE_ALLOWED_FIELDS)
                dest_user = self._match_user(source_user, by_source_uuid, by_username, by_telegram_id)

                if dest_user is None:
                    if self.config.dry_run:
                        LOGGER.info("CREATE user username=%s", source_user.get("username"))
                    else:
                        created = self.dst.create_user(create_payload)
                        self.dst.put_user_metadata(created["uuid"], self.metadata_mapper.imported_metadata(source_user["uuid"]))
                        self.state_store.set_mapping("users", source_user["uuid"], created["uuid"])
                        self.summary.users_created += 1
                        LOGGER.info("created user username=%s uuid=%s", source_user.get("username"), created["uuid"])
                        self._sync_user_status(source_user, created)
                    continue

                self.state_store.set_mapping("users", source_user["uuid"], dest_user["uuid"])
                patch = diff_dict(dest_user, payload, self.UPDATE_ALLOWED_FIELDS)
                patch.pop("status", None)
                if patch:
                    patch["uuid"] = dest_user["uuid"]
                    if self.config.dry_run:
                        LOGGER.info("UPDATE user username=%s fields=%s", source_user.get("username"), sorted(patch))
                    else:
                        self.dst.update_user(patch)
                        self.dst.put_user_metadata(dest_user["uuid"], self.metadata_mapper.imported_metadata(source_user["uuid"]))
                        self.summary.users_updated += 1
                        LOGGER.info("updated user username=%s uuid=%s", source_user.get("username"), dest_user["uuid"])
                else:
                    LOGGER.info("skipped user username=%s reason=no_changes", source_user.get("username"))

                if not self.config.dry_run:
                    self._sync_user_status(source_user, dest_user)
            except Exception as exc:
                self.summary.errors += 1
                LOGGER.exception("failed syncing user %s: %s", source_user.get("username"), exc)

        self._handle_missing_users(source_user_ids, by_uuid)

    def _is_imported_dest_user(self, dest_user: Mapping[str, Any]) -> str | None:
        reverse_state = self.state_store.reverse_mapping("users")
        state_source_uuid = reverse_state.get(dest_user["uuid"])
        if state_source_uuid:
            return state_source_uuid
        try:
            metadata = self.dst.get_user_metadata(dest_user["uuid"])
        except Exception as exc:
            LOGGER.warning("failed to inspect metadata for user %s: %s", dest_user["uuid"], exc)
            return None
        source_uuid = self.metadata_mapper.get_source_uuid_from_metadata(metadata)
        if source_uuid:
            self.state_store.set_mapping("users", source_uuid, dest_user["uuid"])
        return source_uuid

    def _handle_missing_users(self, source_user_ids: set[str], dest_by_uuid: Mapping[str, dict[str, Any]]) -> None:
        for dest_user in dest_by_uuid.values():
            source_uuid = self._is_imported_dest_user(dest_user)
            if not source_uuid or source_uuid in source_user_ids:
                continue

            username = dest_user.get("username")
            try:
                if self.config.delete_missing_users:
                    if self.config.dry_run:
                        LOGGER.info("DELETE missing user username=%s", username)
                    else:
                        self.dst.delete_user(dest_user["uuid"])
                        self.state_store.remove_mapping("users", source_uuid)
                        self.summary.users_deleted += 1
                        LOGGER.info("deleted missing user username=%s uuid=%s", username, dest_user["uuid"])
                    continue

                if self.config.disable_missing_users:
                    if self.config.dry_run:
                        LOGGER.info("DISABLE missing user username=%s", username)
                    else:
                        self.dst.disable_user(dest_user["uuid"])
                        self.summary.users_disabled += 1
                        LOGGER.info("disabled missing user username=%s uuid=%s", username, dest_user["uuid"])
                    continue

                LOGGER.warning("missing imported user username=%s uuid=%s but no action configured", username, dest_user["uuid"])
            except Exception as exc:
                self.summary.errors += 1
                LOGGER.exception("failed handling missing user %s: %s", username, exc)


class NodeSyncService:
    UPDATE_ALLOWED_FIELDS = {
        "name",
        "address",
        "port",
        "isTrafficTrackingActive",
        "trafficLimitBytes",
        "notifyPercent",
        "trafficResetDay",
        "countryCode",
        "consumptionMultiplier",
        "configProfile",
        "tags",
    }

    def __init__(
        self,
        src: RemnawaveClient,
        dst: RemnawaveClient,
        metadata_mapper: MetadataMapper,
        state_store: StateStore,
        summary: Summary,
        config: SyncConfig,
        src_profiles: list[dict[str, Any]],
        dst_profiles: list[dict[str, Any]],
        dst_inbound_by_fingerprint: Mapping[tuple[Any, ...], str],
    ) -> None:
        self.src = src
        self.dst = dst
        self.metadata_mapper = metadata_mapper
        self.state_store = state_store
        self.summary = summary
        self.config = config
        self.src_profile_by_uuid = {profile["uuid"]: profile for profile in src_profiles}
        self.dst_profile_by_fingerprint = {
            config_profile_fingerprint(profile): profile for profile in dst_profiles
        }
        self.dst_inbound_by_fingerprint = dst_inbound_by_fingerprint

    def _build_dest_indexes(self, dest_nodes: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        by_source_uuid: dict[str, dict[str, Any]] = {}
        by_name = {node["name"]: node for node in dest_nodes if isinstance(node.get("name"), str)}
        reverse_state = self.state_store.reverse_mapping("nodes")

        for node in dest_nodes:
            source_uuid = reverse_state.get(node["uuid"])
            if source_uuid:
                by_source_uuid[source_uuid] = node
                continue
            try:
                metadata = self.dst.get_node_metadata(node["uuid"])
            except Exception as exc:
                LOGGER.warning("failed to read metadata for destination node %s: %s", node["uuid"], exc)
                continue
            mapped_source_uuid = self.metadata_mapper.get_source_uuid_from_metadata(metadata)
            if mapped_source_uuid:
                by_source_uuid[mapped_source_uuid] = node
                self.state_store.set_mapping("nodes", mapped_source_uuid, node["uuid"])

        return by_source_uuid, by_name

    def _map_node_config(self, source_node: Mapping[str, Any]) -> dict[str, Any] | None:
        source_payload = map_node_payload(source_node)
        config_profile = source_payload.get("configProfile")
        if not config_profile:
            return None

        src_profile_uuid = config_profile.get("activeConfigProfileUuid")
        if not isinstance(src_profile_uuid, str):
            return None

        src_profile = self.src_profile_by_uuid.get(src_profile_uuid)
        if not src_profile:
            return None

        dest_profile = self.dst_profile_by_fingerprint.get(config_profile_fingerprint(src_profile))
        if not dest_profile:
            return None

        mapped_inbounds: list[str] = []
        for inbound in source_node.get("configProfile", {}).get("activeInbounds", []) or []:
            if not isinstance(inbound, dict):
                continue
            dest_inbound_uuid = self.dst_inbound_by_fingerprint.get(inbound_fingerprint(inbound))
            if not dest_inbound_uuid:
                return None
            mapped_inbounds.append(dest_inbound_uuid)

        source_payload["configProfile"] = {
            "activeConfigProfileUuid": dest_profile["uuid"],
            "activeInbounds": mapped_inbounds,
        }
        return source_payload

    def _match_node(
        self,
        source_node: Mapping[str, Any],
        by_source_uuid: Mapping[str, dict[str, Any]],
        by_name: Mapping[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        source_uuid = source_node["uuid"]
        if source_uuid in by_source_uuid:
            return by_source_uuid[source_uuid]
        node_name = source_node.get("name")
        if isinstance(node_name, str):
            return by_name.get(node_name)
        return None

    def sync(self) -> None:
        source_nodes = self.src.list_nodes()
        dest_nodes = self.dst.list_nodes()
        by_source_uuid, by_name = self._build_dest_indexes(dest_nodes)

        for source_node in source_nodes:
            try:
                payload = self._map_node_config(source_node)
                if payload is None:
                    LOGGER.warning(
                        "SKIP node name=%s reason=config_profile_or_inbounds_not_portable",
                        source_node.get("name"),
                    )
                    continue

                dest_node = self._match_node(source_node, by_source_uuid, by_name)
                if dest_node is None:
                    if self.config.dry_run:
                        LOGGER.info("CREATE node name=%s", source_node.get("name"))
                    else:
                        created = self.dst.create_node(payload)
                        self.dst.put_node_metadata(created["uuid"], self.metadata_mapper.imported_metadata(source_node["uuid"]))
                        self.state_store.set_mapping("nodes", source_node["uuid"], created["uuid"])
                        self.summary.nodes_created += 1
                        LOGGER.info("created node name=%s uuid=%s", source_node.get("name"), created["uuid"])
                        self._sync_node_status(source_node, created["uuid"])
                    continue

                self.state_store.set_mapping("nodes", source_node["uuid"], dest_node["uuid"])
                current_node = clean_none(
                    {
                        "name": dest_node.get("name"),
                        "address": dest_node.get("address"),
                        "port": dest_node.get("port"),
                        "isTrafficTrackingActive": dest_node.get("isTrafficTrackingActive"),
                        "trafficLimitBytes": dest_node.get("trafficLimitBytes"),
                        "notifyPercent": dest_node.get("notifyPercent"),
                        "trafficResetDay": dest_node.get("trafficResetDay"),
                        "countryCode": dest_node.get("countryCode"),
                        "consumptionMultiplier": dest_node.get("consumptionMultiplier"),
                        "configProfile": {
                            "activeConfigProfileUuid": (dest_node.get("configProfile") or {}).get("activeConfigProfileUuid"),
                            "activeInbounds": [
                                item.get("uuid")
                                for item in (dest_node.get("configProfile") or {}).get("activeInbounds", [])
                                if isinstance(item, dict) and item.get("uuid")
                            ],
                        }
                        if dest_node.get("configProfile")
                        else None,
                        "tags": dest_node.get("tags"),
                    }
                )
                patch = diff_dict(current_node, payload, self.UPDATE_ALLOWED_FIELDS)
                if patch:
                    patch["uuid"] = dest_node["uuid"]
                    if self.config.dry_run:
                        LOGGER.info("UPDATE node name=%s fields=%s", source_node.get("name"), sorted(patch))
                    else:
                        self.dst.update_node(patch)
                        self.dst.put_node_metadata(dest_node["uuid"], self.metadata_mapper.imported_metadata(source_node["uuid"]))
                        self.summary.nodes_updated += 1
                        LOGGER.info("updated node name=%s uuid=%s", source_node.get("name"), dest_node["uuid"])
                else:
                    LOGGER.info("skipped node name=%s reason=no_changes", source_node.get("name"))

                if not self.config.dry_run:
                    self._sync_node_status(source_node, dest_node["uuid"])
            except Exception as exc:
                self.summary.errors += 1
                LOGGER.exception("failed syncing node %s: %s", source_node.get("name"), exc)

    def _sync_node_status(self, source_node: Mapping[str, Any], dest_node_uuid: str) -> None:
        is_disabled = source_node.get("isDisabled")
        if is_disabled is True:
            self.dst.disable_node(dest_node_uuid)
        elif is_disabled is False:
            self.dst.enable_node(dest_node_uuid)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-way Remnawave panel sync")
    parser.add_argument("command", nargs="?", choices=("sync", "init"), default="sync", help="Run sync or open the interactive setup wizard")
    parser.add_argument("--config-file", type=str, default=str(default_config_file()), help="Path to the .env config file")
    parser.add_argument("--wizard", action="store_true", help="Run the interactive setup wizard and save configuration")
    parser.add_argument("--dry-run", action="store_true", help="Log planned actions without writing to destination")
    parser.add_argument("--sync-users", action="store_true", help="Enable user synchronization")
    parser.add_argument("--sync-squads", action="store_true", help="Enable squad synchronization")
    parser.add_argument("--sync-nodes", action="store_true", help="Enable node synchronization")
    parser.add_argument("--disable-missing-users", action="store_true", help="Disable previously imported users missing on source")
    parser.add_argument("--delete-missing-users", action="store_true", help="Delete previously imported users missing on source")
    parser.add_argument("--page-size", type=int, help="Page size for GET /api/users pagination")
    parser.add_argument("--log-level", type=str, help="Logging level")
    parser.add_argument("--state-file", type=str, help="Path to state file")
    return parser


def preflight_source(client: RemnawaveClient, config: SyncConfig) -> None:
    try:
        if config.enable_squad_sync:
            client.list_external_squads()
            client.list_internal_squads()
        if config.enable_user_sync:
            client.list_users(config.page_size)
        if config.enable_node_sync:
            client.list_nodes()
    except Exception as exc:
        raise SyncError(f"Source preflight failed: {exc}") from exc


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    config_file = Path(args.config_file).expanduser()

    load_dotenv(override=False)
    if config_file.exists():
        load_dotenv(config_file, override=True)

    if args.command == "init" or args.wizard:
        return run_setup_wizard(config_file)

    try:
        config = SyncConfig.from_env_and_args(args)
    except Exception as exc:
        if sys.stdin.isatty() and sys.stdout.isatty():
            print(f"Configuration error: {exc}", file=sys.stderr)
            print("Launching interactive setup wizard...", file=sys.stderr)
            wizard_result = run_setup_wizard(config_file)
            if wizard_result != 0:
                return wizard_result
            load_dotenv(config_file, override=True)
            try:
                config = SyncConfig.from_env_and_args(args)
            except Exception as second_exc:
                print(f"Configuration error after setup: {second_exc}", file=sys.stderr)
                return 2
        else:
            print(f"Configuration error: {exc}", file=sys.stderr)
            print("Run 'sync-remnawave init' or use --config-file to provide a valid .env.", file=sys.stderr)
            return 2

    configure_logging(config.log_level)
    state_store = StateStore(config.state_file)
    summary = Summary()
    src_client = RemnawaveClient(config.src_url, config.src_api_key, config.request_timeout, "source")
    dst_client = RemnawaveClient(config.dst_url, config.dst_api_key, config.request_timeout, "destination")
    metadata_mapper = MetadataMapper(state_store)

    try:
        preflight_source(src_client, config)

        dst_inbounds = dst_client.list_inbounds()
        dst_profiles = dst_client.list_config_profiles()
        src_profiles = src_client.list_config_profiles() if config.enable_node_sync else []
        dst_inbound_by_fingerprint = {
            inbound_fingerprint(item): item["uuid"]
            for item in dst_inbounds
        }

        squad_mapping: dict[tuple[str, str], str] = {}
        if config.enable_squad_sync:
            squad_service = SquadSyncService(
                src=src_client,
                dst=dst_client,
                state_store=state_store,
                summary=summary,
                dry_run=config.dry_run,
                dst_inbound_by_fingerprint=dst_inbound_by_fingerprint,
            )
            squad_mapping = squad_service.sync()
        else:
            for kind in ("internal", "external"):
                for source_uuid, dest_uuid in state_store.data.get("squads", {}).get(kind, {}).items():
                    squad_mapping[(kind, source_uuid)] = dest_uuid

        if config.enable_user_sync:
            user_service = UserSyncService(
                src=src_client,
                dst=dst_client,
                metadata_mapper=metadata_mapper,
                state_store=state_store,
                summary=summary,
                config=config,
                squad_mapping=squad_mapping,
            )
            user_service.sync()

        if config.enable_node_sync:
            node_service = NodeSyncService(
                src=src_client,
                dst=dst_client,
                metadata_mapper=metadata_mapper,
                state_store=state_store,
                summary=summary,
                config=config,
                src_profiles=src_profiles,
                dst_profiles=dst_profiles,
                dst_inbound_by_fingerprint=dst_inbound_by_fingerprint,
            )
            node_service.sync()

        if not config.dry_run:
            state_store.save()
    except SyncError as exc:
        LOGGER.error("sync failed: %s", exc)
        return 1
    except Exception as exc:
        LOGGER.exception("unexpected sync failure: %s", exc)
        return 1
    finally:
        src_client.close()
        dst_client.close()

    LOGGER.info(
        "summary squads_created=%s squads_updated=%s users_created=%s users_updated=%s users_disabled=%s users_deleted=%s nodes_created=%s nodes_updated=%s errors=%s",
        summary.squads_created,
        summary.squads_updated,
        summary.users_created,
        summary.users_updated,
        summary.users_disabled,
        summary.users_deleted,
        summary.nodes_created,
        summary.nodes_updated,
        summary.errors,
    )
    return 0 if summary.errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
