from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version as package_version
import json
import os
from pathlib import Path
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
import httpx
from pydantic import BaseModel, ConfigDict

from piphi_runtime_kit_python import (
    AutomationActionRequest,
    AutomationRegistry,
    IntegrationCommandRequest,
    IntegrationDiscoveryRequest,
    IntegrationDiscoveryResponse,
    IntegrationEventListResponse,
    RuntimeConfig,
    RuntimeConfigApplyResponse,
    RuntimeConfigRemoveResponse,
    RuntimeConfigSnapshot,
    RuntimeConfigSyncResponse,
    RuntimeDiagnosticsResponse,
    RuntimeHealthResponse,
    SQLiteAutomationIdempotencyStore,
    MqttBrokerConfig,
    MqttJsonClient,
    build_source_topic_root,
    build_config_apply_response,
    build_discovery_response,
    build_event_list_response,
    build_local_event_record,
    create_tracked_task,
    create_runtime_starter,
    resolve_core_base_url,
    runtime_lifespan,
    schedule_event_delivery,
    schedule_telemetry_delivery,
    validate_typed_configs,
)
from piphi_runtime_kit_python.fastapi import (
    dispatch_automation_action_from_fastapi,
    sync_runtime_auth_from_fastapi_payload,
)

from .profiles import (
    PROFILE_DEFINITIONS,
    build_entities,
    extract_metrics,
    format_capability_name,
    infer_profile_id,
    list_profiles,
    metric_units,
    normalize_profile_id,
)


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _resolve_discovery_cache_path() -> Path | None:
    value = os.getenv("RTL433_DISCOVERY_CACHE_PATH")
    if value is not None and not value.strip():
        return None
    return Path(value or "/tmp/piphi-network-433mhz-discovery.json")


INTEGRATION_ID = "piphi-network-433mhz"
INTEGRATION_NAME = "PiPhi Network 433MHz Devices"
try:
    INTEGRATION_VERSION = package_version("piphi-network-433mhz")
except PackageNotFoundError:  # pragma: no cover - source checkout without an installed package
    INTEGRATION_VERSION = "0.0.0"
RTL433_BRIDGE_SERVICE_ID = "piphi.service.rtl433-bridge"
RTL433_BRIDGE_URL_ENV = "PIPHI_SERVICE_RTL433_BRIDGE_RUNTIME_URL"
MAX_DISCOVERY_CACHE = 200
DISCOVERY_WAIT_SECONDS = max(
    0.0,
    min(_env_float("RTL433_DISCOVERY_WAIT_SECONDS", 8.0), 18.0),
)
DISCOVERY_CACHE_PATH = _resolve_discovery_cache_path()
MQTT_SOURCE_ENABLED = _env_flag("RTL433_MQTT_ENABLED", False)
MQTT_BROKER_HOSTNAME = os.getenv("MQTT_HOSTNAME", "127.0.0.1")
MQTT_BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_BROKER_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_BROKER_PASSWORD = os.getenv("MQTT_PASSWORD") or None
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID") or f"{INTEGRATION_ID}-subscriber"
MQTT_QOS = int(os.getenv("MQTT_QOS", "0"))
MQTT_TOPIC_ROOT = os.getenv("MQTT_TOPIC_ROOT", build_source_topic_root("rtl433"))
MQTT_PACKET_TOPIC = f"{MQTT_TOPIC_ROOT.rstrip('/')}/packets"
MQTT_RETRY_DELAY_SECONDS = float(os.getenv("MQTT_RETRY_DELAY_SECONDS", "5"))

starter = create_runtime_starter(
    integration_id=INTEGRATION_ID,
    integration_name=INTEGRATION_NAME,
    version=INTEGRATION_VERSION,
    core_base_url=resolve_core_base_url("http://127.0.0.1:31419"),
)
runtime = starter.runtime
registry = starter.registry
telemetry = starter.telemetry_client
event_client = starter.event_client
config_sync = starter.config_sync
recent_seen_devices: OrderedDict[str, dict[str, Any]] = OrderedDict()
discovery_waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = set()
_automation_ledger_path = Path(
    os.getenv(
        "PIPHI_AUTOMATION_LEDGER_PATH",
        "/.piphinetwork/automation-actions.sqlite3",
    )
)
automation_registry = AutomationRegistry(
    idempotency_store=SQLiteAutomationIdempotencyStore(_automation_ledger_path)
)


async def _clear_discovery_cache_action(
    action_request: AutomationActionRequest,
) -> dict[str, Any]:
    count = len(recent_seen_devices)
    recent_seen_devices.clear()
    persist_discovery_cache()
    return {
        "status": "ok",
        "command": action_request.command,
        "result": {"cleared_discovery_records": count},
    }


automation_registry.action("clear_discovery_cache")(_clear_discovery_cache_action)


class Rtl433DeviceConfig(RuntimeConfig):
    profile: str = "auto"
    model: str
    station_id: str
    channel: str | None = None
    alias: str | None = None


class Rtl433Packet(BaseModel):
    model_config = ConfigDict(extra="allow")

    model: str | None = None
    id: str | int | None = None
    channel: str | int | None = None


def _optional_config_attr(config: RuntimeConfig, name: str) -> Any:
    return getattr(config, name, None)


def build_device_key(model: str | None, station_id: Any, channel: Any = None) -> str:
    model_part = str(model or "unknown")
    station_part = str(station_id or "unknown")
    channel_part = str(channel) if channel not in (None, "") else "na"
    return f"{model_part}::{station_part}::{channel_part}"


def extract_station_id(packet: dict[str, Any]) -> str:
    for key in ("id", "device_id", "device", "sid", "unit"):
        value = packet.get(key)
        if value not in (None, ""):
            return str(value)
    return "unknown"


def extract_channel(packet: dict[str, Any]) -> str | None:
    for key in ("channel", "subtype"):
        value = packet.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def make_registry_entry(config: Rtl433DeviceConfig) -> dict[str, Any]:
    profile_id = normalize_profile_id(config.profile)
    device_key = build_device_key(config.model, config.station_id, config.channel)
    return {
        "config_id": _optional_config_attr(config, "config_id") or config.id,
        "device_id": device_key,
        "container_id": _optional_config_attr(config, "container_id"),
        "integration_id": _optional_config_attr(config, "integration_id") or INTEGRATION_ID,
        "profile": profile_id,
        "model": config.model,
        "station_id": config.station_id,
        "channel": config.channel,
        "alias": config.alias,
    }


def append_runtime_event(
    *,
    event_type: str,
    device: dict[str, Any],
    payload: dict[str, Any] | None = None,
    severity: str = "info",
) -> None:
    registry.append_event(
        build_local_event_record(
            event_type=event_type,
            device=device,
            payload=payload,
            source=INTEGRATION_ID,
            severity=severity,
        )
    )


def emit_leak_transition_event(
    *,
    entry: dict[str, Any],
    previous_metrics: dict[str, Any],
    current_metrics: dict[str, Any],
    profile_id: str,
    source_transport: str,
) -> None:
    if "leak_detected" not in previous_metrics or "leak_detected" not in current_metrics:
        return
    previous_value = previous_metrics.get("leak_detected")
    current_value = current_metrics.get("leak_detected")
    if (
        previous_value == current_value
        or not isinstance(previous_value, bool)
        or not isinstance(current_value, bool)
    ):
        return

    event_type = (
        "rtl433.safety.leak.detected"
        if current_value
        else "rtl433.safety.leak.cleared"
    )
    event_payload = {
        "capability": "leak_detected",
        "previous_value": previous_value,
        "current_value": current_value,
        "profile": profile_id,
        "transport": source_transport,
    }
    append_runtime_event(
        event_type=event_type,
        device=entry,
        payload=event_payload,
        severity="warning",
    )
    schedule_event_delivery(
        process_state=runtime.process_state,
        event_client=event_client,
        auth_context=runtime.auth,
        event_type=event_type,
        device=entry,
        payload=event_payload,
        source=INTEGRATION_ID,
        severity="warning",
    )


async def apply_config(config: Rtl433DeviceConfig) -> None:
    entry = make_registry_entry(config)
    registry.set(config.id, entry)
    registry.update_state(
        config.id,
        {
            "profile": entry["profile"],
            "model": entry["model"],
            "station_id": entry["station_id"],
            "channel": entry["channel"],
            "alias": entry["alias"],
            "last_packet_at": None,
        },
    )
    append_runtime_event(
        event_type="rtl433.config.applied",
        device=entry,
        payload={
            "profile": entry["profile"],
            "model": entry["model"],
            "station_id": entry["station_id"],
            "channel": entry["channel"],
        },
    )


async def remove_config(config_id: str) -> bool:
    removed = registry.remove(config_id)
    if removed is None:
        return False
    append_runtime_event(
        event_type="rtl433.config.removed",
        device=removed,
        payload={"config_id": config_id},
    )
    return True


def remember_discovered_device(packet: dict[str, Any]) -> dict[str, Any]:
    profile_id = infer_profile_id(packet)
    model = str(packet.get("model") or "Unknown rtl_433 Device")
    station_id = extract_station_id(packet)
    channel = extract_channel(packet)
    device_key = build_device_key(model, station_id, channel)
    metrics = extract_metrics(packet, profile_id)
    record = {
        "id": device_key,
        "device_id": device_key,
        "profile": profile_id,
        "profile_name": PROFILE_DEFINITIONS[profile_id].name,
        "model": model,
        "station_id": station_id,
        "channel": channel or "",
        "alias": model,
        "last_seen_at": now_iso(),
        "preview_metrics": metrics,
    }
    recent_seen_devices[device_key] = record
    recent_seen_devices.move_to_end(device_key)
    while len(recent_seen_devices) > MAX_DISCOVERY_CACHE:
        recent_seen_devices.popitem(last=False)
    persist_discovery_cache()
    notify_discovery_waiters()
    return record


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def persist_discovery_cache() -> None:
    if not DISCOVERY_CACHE_PATH:
        return

    payload = {
        "version": 1,
        "updated_at": now_iso(),
        "devices": list(recent_seen_devices.values()),
    }
    try:
        DISCOVERY_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = DISCOVERY_CACHE_PATH.with_suffix(f"{DISCOVERY_CACHE_PATH.suffix}.tmp")
        tmp_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        tmp_path.replace(DISCOVERY_CACHE_PATH)
    except OSError as exc:
        print(f"rtl433_discovery_cache_persist_failed path={DISCOVERY_CACHE_PATH} error={exc}")


def load_discovery_cache() -> None:
    if not DISCOVERY_CACHE_PATH:
        return

    if not DISCOVERY_CACHE_PATH.exists():
        return

    try:
        payload = json.loads(DISCOVERY_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"rtl433_discovery_cache_load_failed path={DISCOVERY_CACHE_PATH} error={exc}")
        return

    devices = payload.get("devices") if isinstance(payload, dict) else None
    if not isinstance(devices, list):
        return

    recent_seen_devices.clear()
    for item in devices[-MAX_DISCOVERY_CACHE:]:
        if not isinstance(item, dict):
            continue
        device_id = str(item.get("device_id") or item.get("id") or "").strip()
        if not device_id:
            continue
        if item.get("channel") is None:
            item["channel"] = ""
        recent_seen_devices[device_id] = item
    print(f"rtl433_discovery_cache_loaded count={len(recent_seen_devices)} path={DISCOVERY_CACHE_PATH}")


def notify_discovery_waiters() -> None:
    for loop, event in list(discovery_waiters):
        if loop.is_closed():
            discovery_waiters.discard((loop, event))
            continue
        loop.call_soon_threadsafe(event.set)


def filter_recent_devices(requested_profile: Any = None) -> list[dict[str, Any]]:
    devices = list(recent_seen_devices.values())
    if requested_profile:
        profile = str(requested_profile)
        devices = [device for device in devices if device.get("profile") == profile]
    return devices


async def wait_for_discovery_devices(requested_profile: Any = None) -> list[dict[str, Any]]:
    devices = filter_recent_devices(requested_profile)
    if devices or DISCOVERY_WAIT_SECONDS <= 0:
        return devices

    deadline = time.monotonic() + DISCOVERY_WAIT_SECONDS
    loop = asyncio.get_running_loop()
    event = asyncio.Event()
    waiter = (loop, event)
    discovery_waiters.add(waiter)

    try:
        while time.monotonic() < deadline:
            devices = filter_recent_devices(requested_profile)
            if devices:
                return devices

            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                break

            event.clear()
            try:
                await asyncio.wait_for(event.wait(), timeout=remaining)
            except TimeoutError:
                break
    finally:
        discovery_waiters.discard(waiter)

    return filter_recent_devices(requested_profile)


async def startup_sync(_runtime_context, _client) -> None:
    load_discovery_cache()

    result = await starter.rehydrate_configs(
        client=_client,
        apply_snapshot=apply_runtime_config_snapshot,
        config_model=Rtl433DeviceConfig,
        snapshot_model=RuntimeConfigSnapshot,
    )

    if result.snapshot_applied:
        print(
            "rtl433_startup_rehydrate_complete "
            f"loaded={result.snapshot_config_count} "
            f"generation={result.snapshot_generation} "
            "source=snapshot"
        )

    if result.core_applied:
        print(
            "rtl433_startup_rehydrate_complete "
            f"loaded={result.core_config_count} "
            f"generation={result.core_generation} "
            "source=core"
        )
    elif result.core_error:
        print(f"rtl433_startup_core_rehydrate_failed error={result.core_error}")
    elif result.missing_runtime_auth:
        print("rtl433_startup_missing_runtime_credentials standalone_mode=true")
    elif result.core_attempted:
        print("rtl433_startup_rehydrate_no_configs")

    if not MQTT_SOURCE_ENABLED:
        return
    mqtt_client = MqttJsonClient(
        MqttBrokerConfig(
            hostname=MQTT_BROKER_HOSTNAME,
            port=MQTT_BROKER_PORT,
            username=MQTT_BROKER_USERNAME,
            password=MQTT_BROKER_PASSWORD,
            client_id=MQTT_CLIENT_ID,
            qos=MQTT_QOS,
        )
    )
    create_tracked_task(
        mqtt_client.run_subscription_forever(
            topics=[MQTT_PACKET_TOPIC],
            handler=handle_mqtt_packet,
            retry_delay_seconds=MQTT_RETRY_DELAY_SECONDS,
        ),
        process_state=runtime.process_state,
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    async with runtime_lifespan(runtime, on_startup=startup_sync):
        yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> RuntimeHealthResponse:
    return starter.health_response(
        metadata={
            "active_configs": len(registry.ids()),
            "recent_discovery_count": len(recent_seen_devices),
            "supported_profile_count": len(PROFILE_DEFINITIONS),
            "mqtt_source_enabled": MQTT_SOURCE_ENABLED,
        }
    )


@app.get("/diagnostics")
async def diagnostics() -> RuntimeDiagnosticsResponse:
    return starter.diagnostics_response(
        diagnostics={
            "active_config_ids": registry.ids(),
            "recent_event_count": len(registry.recent_events),
            "recent_discovery_count": len(recent_seen_devices),
            "supported_profiles": list_profiles(),
            "mqtt_packet_topic": MQTT_PACKET_TOPIC if MQTT_SOURCE_ENABLED else None,
        }
    )


@app.get("/ui-config")
async def ui_config() -> dict[str, Any]:
    profile_ids = list(PROFILE_DEFINITIONS.keys())
    profile_names = [profile.name for profile in PROFILE_DEFINITIONS.values()]

    return {
        "schema": {
            "title": "Wireless Sensor Setup",
            "description": (
                "Review the 433 MHz sensor PiPhi heard. Discovery fills most of this in, "
                "and you can leave the channel blank if you do not know it."
            ),
            "type": "object",
            "required": ["model", "station_id"],
            "properties": {
                "profile": {
                    "type": "string",
                    "title": "Device type",
                    "description": (
                        "Auto-detect is recommended. PiPhi will record safe decoded readings "
                        "from each radio packet."
                    ),
                    "enum": profile_ids,
                    "default": "auto",
                },
                "model": {
                    "type": "string",
                    "title": "Sensor model",
                    "description": (
                        "Usually filled from the radio packet. Leave it as discovered unless "
                        "you know it is wrong."
                    ),
                },
                "station_id": {
                    "type": "string",
                    "title": "Sensor ID",
                    "description": "The ID broadcast by the sensor. PiPhi uses it to recognize this device later.",
                },
                "channel": {
                    "type": "string",
                    "title": "Channel (optional)",
                    "description": (
                        "Leave blank if you do not know it. This is only needed when several "
                        "sensors share the same model and ID."
                    ),
                    "default": "",
                },
                "alias": {
                    "type": "string",
                    "title": "Display name",
                    "description": "A friendly name shown in dashboards and device lists.",
                    "default": "",
                },
            },
        },
        "uiSchema": {
            "ui:options": {
                "translations": {
                    "submit": "Save Device",
                },
            },
            "profile": {
                "ui:components": {
                    "stringField": "enumField",
                },
                "ui:options": {
                    "enumNames": profile_names,
                    "flowbite3Select": {
                        "placeholder": "Auto-detect",
                    },
                },
            },
            "model": {
                "ui:options": {
                    "flowbite3Text": {
                        "placeholder": "Fineoffset-WH0290",
                    },
                },
            },
            "station_id": {
                "ui:options": {
                    "flowbite3Text": {
                        "placeholder": "62",
                    },
                },
            },
            "channel": {
                "ui:options": {
                    "flowbite3Text": {
                        "placeholder": "Leave blank if unknown",
                    },
                },
            },
            "alias": {
                "ui:options": {
                    "flowbite3Text": {
                        "placeholder": "Backyard weather sensor",
                    },
                },
            },
        },
        "profiles": list_profiles(),
    }


@app.post("/discover", response_model=IntegrationDiscoveryResponse)
async def discover(
    payload: IntegrationDiscoveryRequest | None = None,
) -> IntegrationDiscoveryResponse:
    requested_profile = None
    discovery_inputs: dict[str, Any] = {}
    if payload and payload.inputs:
        discovery_inputs = dict(payload.inputs)
        requested_profile = payload.inputs.get("profile")

    await configure_discovery_receiver(discovery_inputs)
    devices = await wait_for_discovery_devices(requested_profile)
    return build_discovery_response(devices)


async def configure_discovery_receiver(inputs: dict[str, Any]) -> None:
    settings = {
        "frequency": _clean_discovery_input(inputs.get("radio_frequency")),
        "rtlsdr_device": _clean_discovery_input(inputs.get("rtlsdr_device")),
    }
    settings = {key: value for key, value in settings.items() if value is not None}
    if not settings:
        return

    bridge_url = resolve_rtl433_bridge_url()
    if bridge_url is None:
        raise HTTPException(
            status_code=503,
            detail="The rtl_433 radio service is not ready. Start the connection and try again.",
        )

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.post(f"{bridge_url.rstrip('/')}/radio/config", json=settings)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = "The radio service rejected those receiver settings."
        try:
            response_detail = exc.response.json().get("detail")
            if response_detail:
                detail = str(response_detail)
        except (ValueError, AttributeError):
            pass
        raise HTTPException(status_code=422, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=503,
            detail="The rtl_433 radio service could not be reached. Check the connection and try again.",
        ) from exc


def resolve_rtl433_bridge_url() -> str | None:
    direct_url = str(os.getenv(RTL433_BRIDGE_URL_ENV) or "").strip()
    if direct_url:
        return direct_url

    try:
        bindings = json.loads(os.getenv("PIPHI_SERVICE_BINDINGS", "{}"))
    except json.JSONDecodeError:
        return None
    binding = bindings.get(RTL433_BRIDGE_SERVICE_ID) if isinstance(bindings, dict) else None
    if not isinstance(binding, dict) or binding.get("status") != "available":
        return None
    url = str(binding.get("runtime_url") or binding.get("url") or "").strip()
    return url or None


def _clean_discovery_input(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


@app.post("/config")
async def config(
    payload: Rtl433DeviceConfig,
    request: Request,
) -> RuntimeConfigApplyResponse:
    sync_runtime_auth_from_fastapi_payload(runtime, request, payload)
    await apply_config(payload)
    device_key = build_device_key(payload.model, payload.station_id, payload.channel)
    return build_config_apply_response(
        config_id=_optional_config_attr(payload, "config_id") or payload.id,
        container_id=_optional_config_attr(payload, "container_id"),
        metadata={
            "profile": normalize_profile_id(payload.profile),
            "device_id": device_key,
            "device_key": device_key,
        },
    )


async def apply_runtime_config_snapshot(
    snapshot: RuntimeConfigSnapshot,
) -> RuntimeConfigSyncResponse:
    typed_configs = validate_typed_configs(
        [
            config.model_dump() if isinstance(config, RuntimeConfig) else config
            for config in snapshot.configs
        ],
        Rtl433DeviceConfig,
    )
    return await config_sync.apply_snapshot(
        snapshot=snapshot.model_copy(update={"configs": typed_configs}),
        active_config_ids=registry.ids(),
        apply_config=apply_config,
        remove_config=remove_config,
        get_active_config_ids=registry.ids,
    )


@app.post("/config/sync")
async def sync_config(
    snapshot: RuntimeConfigSnapshot,
    request: Request,
) -> RuntimeConfigSyncResponse:
    runtime.auth.sync_from_headers(request.headers, payload_container_id=snapshot.container_id)
    return await apply_runtime_config_snapshot(snapshot)


@app.post("/deconfigure/{config_id}")
async def deconfigure(config_id: str) -> RuntimeConfigRemoveResponse:
    removed = await remove_config(config_id)
    return RuntimeConfigRemoveResponse(
        config_id=config_id,
        removed=removed,
        metadata={"remaining_configs": registry.ids()},
    )


@app.get("/entities")
async def entities() -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for entry in registry.entries.values():
        device_name = entry.get("alias") or entry["device_id"]
        payload.extend(
            build_entities(
                device_name=device_name,
                device_key=str(entry["device_id"]),
                profile_id=str(entry.get("profile") or "generic_sensor"),
                metric_names=list(entry.get("observed_metrics") or []),
            )
        )
    return payload


@app.get("/state")
async def state(
    refresh: bool = Query(default=False),
    refresh_request_id: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        state_payload = await starter.state.response(
            refresh=refresh,
            refresh_request_id=refresh_request_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        **state_payload,
        "summary": {
            "active_config_count": len(registry.ids()),
            "recent_event_count": len(registry.recent_events),
            "recent_discovery_count": len(recent_seen_devices),
        },
        # ``entries`` is part of this integration's existing public API.
        # Only the refresh receipt is new; do not rename or reshape it.
        "entries": registry.entries,
        "state_snapshots": registry.state_snapshots,
        "recent_seen_devices": list(recent_seen_devices.values())[-25:],
    }


@app.get("/events", response_model=IntegrationEventListResponse)
async def list_events(limit: int = 50) -> IntegrationEventListResponse:
    return build_event_list_response(registry.recent_events[-limit:])


@app.post("/command")
async def command(
    payload: IntegrationCommandRequest,
    request: Request,
) -> dict[str, Any]:
    sync_runtime_auth_from_fastapi_payload(runtime, request, payload)
    if payload.command == "clear_discovery_cache":
        result = await dispatch_automation_action_from_fastapi(
            automation_registry,
            request,
            payload,
        )
        if not result.ok:
            return {
                "status": "failed",
                "command": payload.command,
                "result": {"message": result.error},
                "replayed": result.replayed,
            }
        return {**result.result, "replayed": result.replayed}

    return {
        "status": "unsupported",
        "command": payload.command,
        "result": {"message": f"Unsupported command: {payload.command}"},
    }


@app.post("/ingest/rtl433")
async def ingest_rtl433(packet: Rtl433Packet, request: Request) -> dict[str, Any]:
    runtime.auth.sync_from_headers(request.headers)
    payload = packet.model_dump(exclude_none=True)
    return await process_rtl433_packet(payload, source_transport="http")


async def handle_mqtt_packet(_topic: str, envelope: dict[str, Any]) -> None:
    packet = envelope.get("packet")
    if not isinstance(packet, dict):
        return
    await process_rtl433_packet(packet, source_transport="mqtt")


async def process_rtl433_packet(
    payload: dict[str, Any],
    *,
    source_transport: str,
) -> dict[str, Any]:
    discovered = remember_discovered_device(payload)

    matched_configs = 0
    for config_id, entry in registry.entries.items():
        if str(entry.get("device_id")) != discovered["device_id"]:
            continue

        matched_configs += 1
        configured_profile_id = normalize_profile_id(str(entry.get("profile") or "auto"))
        inferred_profile_id = str(discovered.get("profile") or "generic_sensor")
        profile_id = (
            inferred_profile_id
            if configured_profile_id in {"auto", "generic_sensor"} and inferred_profile_id != "generic_sensor"
            else configured_profile_id
        )
        if profile_id != entry.get("profile"):
            entry["profile"] = profile_id
        metrics = extract_metrics(payload, profile_id)
        current_snapshot = registry.state_snapshots.get(config_id)
        current_state = (
            current_snapshot.get("state")
            if isinstance(current_snapshot, dict)
            else {}
        )
        previous_metrics = (
            current_state.get("last_metrics")
            if isinstance(current_state, dict)
            else {}
        )
        if not isinstance(previous_metrics, dict):
            previous_metrics = {}
        merged_metrics = {**previous_metrics, **metrics}
        entry["observed_metrics"] = sorted(
            set(entry.get("observed_metrics") or []) | set(metrics)
        )
        registry.update_state(
            config_id,
            {
                "last_packet_at": now_iso(),
                "model": discovered["model"],
                "station_id": discovered["station_id"],
                "channel": discovered["channel"],
                "profile": profile_id,
                "last_metrics": merged_metrics,
                "observed_metrics": entry["observed_metrics"],
            },
        )

        if metrics:
            schedule_telemetry_delivery(
                process_state=runtime.process_state,
                telemetry_client=telemetry,
                auth_context=runtime.auth,
                config_id=str(entry["config_id"]),
                device_id=str(entry["device_id"]),
                container_id=entry.get("container_id"),
                metrics=metrics,
                units=metric_units(profile_id, list(metrics.keys())),
            )

        emit_leak_transition_event(
            entry=entry,
            previous_metrics=previous_metrics,
            current_metrics=metrics,
            profile_id=profile_id,
            source_transport=source_transport,
        )

        append_runtime_event(
            event_type="rtl433.packet.matched",
            device=entry,
            payload={
                "transport": source_transport,
                "profile": profile_id,
                "metrics": metrics,
                "model": discovered["model"],
                "station_id": discovered["station_id"],
            },
        )

    if matched_configs == 0:
        append_runtime_event(
            event_type="rtl433.packet.discovered",
            device={
                "config_id": discovered["id"],
                "device_id": discovered["device_id"],
                "integration_id": INTEGRATION_ID,
                "container_id": runtime.auth.container_id or None,
            },
            payload={
                "transport": source_transport,
                "profile": discovered["profile"],
                "model": discovered["model"],
                "station_id": discovered["station_id"],
                "channel": discovered["channel"],
            },
        )

    return {
        "status": "ok",
        "transport": source_transport,
        "device_id": discovered["device_id"],
        "profile": discovered["profile"],
        "matched_configs": matched_configs,
        "metric_names": [
            format_capability_name(metric_name)
            for metric_name in extract_metrics(payload, discovered["profile"]).keys()
        ],
    }
