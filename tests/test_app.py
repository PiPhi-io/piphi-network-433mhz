import importlib
import asyncio
import json
from pathlib import Path
import time

from fastapi.testclient import TestClient
from piphi_runtime_testkit_python import (
    assert_telemetry_sent,
    build_config_payload,
    build_config_snapshot,
    build_runtime_headers,
)

from piphi_network_433mhz.app import app, recent_seen_devices, registry

app_module = importlib.import_module("piphi_network_433mhz.app")


def reset_runtime_state() -> None:
    recent_seen_devices.clear()
    app_module.discovery_waiters.clear()
    registry.entries.clear()
    registry.state_snapshots.clear()
    registry.recent_events.clear()
    app_module.runtime.auth.container_id = ""
    app_module.runtime.auth.internal_token = ""
    app_module.runtime.process_state.background_tasks.clear()


def wait_for(condition, *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for background delivery to complete.")


def test_discover_returns_recent_seen_device() -> None:
    reset_runtime_state()
    client = TestClient(app)

    ingest_response = client.post(
        "/ingest/rtl433",
        json={
            "model": "Nexus-TH",
            "id": 42,
            "channel": 1,
            "temperature_C": 23.1,
            "humidity": 51,
        },
    )
    discover_response = client.post("/discover", json={"inputs": {}})

    assert ingest_response.status_code == 200
    assert discover_response.status_code == 200
    devices = discover_response.json()["devices"]
    assert len(devices) == 1
    assert devices[0]["profile"] == "weather_basic"


def test_discover_uses_blank_channel_when_packet_has_no_channel() -> None:
    reset_runtime_state()
    client = TestClient(app)

    client.post(
        "/ingest/rtl433",
        json={
            "model": "Fineoffset-WH0290",
            "id": 62,
            "temperature_C": 23.1,
            "humidity": 51,
        },
    )

    discover_response = client.post("/discover", json={"inputs": {}})

    assert discover_response.status_code == 200
    devices = discover_response.json()["devices"]
    assert devices[0]["device_id"] == "Fineoffset-WH0290::62::na"
    assert devices[0]["channel"] == ""


def test_ui_config_presents_channel_as_optional_and_user_friendly() -> None:
    reset_runtime_state()
    client = TestClient(app)

    response = client.get("/ui-config")

    assert response.status_code == 200
    payload = response.json()
    schema = payload["schema"]
    ui_schema = payload["uiSchema"]

    assert schema["title"] == "Wireless Sensor Setup"
    assert schema["required"] == ["model", "station_id"]
    assert schema["properties"]["profile"]["title"] == "Device type"
    assert schema["properties"]["profile"]["default"] == "auto"
    assert "enumNames" not in schema["properties"]["profile"]
    assert schema["properties"]["channel"]["title"] == "Channel (optional)"
    assert schema["properties"]["channel"]["default"] == ""
    assert "leave the channel blank" in schema["description"]
    assert ui_schema["profile"]["ui:components"]["stringField"] == "enumField"
    assert ui_schema["profile"]["ui:options"]["enumNames"][0] == "Auto-detect"
    assert ui_schema["ui:options"]["translations"]["submit"] == "Save Device"


def test_discover_can_filter_by_profile() -> None:
    reset_runtime_state()
    client = TestClient(app)

    client.post(
        "/ingest/rtl433",
        json={
            "model": "Nexus-TH",
            "id": 42,
            "temperature_C": 23.1,
            "humidity": 51,
        },
    )
    client.post(
        "/ingest/rtl433",
        json={
            "model": "Generic-Contact",
            "id": 7,
            "state": "open",
        },
    )

    discover_response = client.post("/discover", json={"inputs": {"profile": "contact_sensor"}})

    assert discover_response.status_code == 200
    devices = discover_response.json()["devices"]
    assert len(devices) == 1
    assert devices[0]["profile"] == "contact_sensor"
    assert devices[0]["station_id"] == "7"


def test_discover_applies_manifest_radio_inputs_before_scanning(monkeypatch) -> None:
    reset_runtime_state()
    client = TestClient(app)
    received = []

    async def fake_configure(inputs):
        received.append(inputs)

    monkeypatch.setattr(app_module, "configure_discovery_receiver", fake_configure)
    client.post(
        "/ingest/rtl433",
        json={"model": "Nexus-TH", "id": 42, "temperature_C": 23.1},
    )

    response = client.post(
        "/discover",
        json={"inputs": {"radio_frequency": "915mhz", "rtlsdr_device": "1"}},
    )

    assert response.status_code == 200
    assert received == [{"radio_frequency": "915mhz", "rtlsdr_device": "1"}]


def test_resolve_rtl433_bridge_url_uses_generic_service_binding(monkeypatch) -> None:
    monkeypatch.delenv(app_module.RTL433_BRIDGE_URL_ENV, raising=False)
    monkeypatch.setenv(
        "PIPHI_SERVICE_BINDINGS",
        json.dumps(
            {
                app_module.RTL433_BRIDGE_SERVICE_ID: {
                    "status": "available",
                    "runtime_url": "http://127.0.0.1:8091",
                }
            }
        ),
    )

    assert app_module.resolve_rtl433_bridge_url() == "http://127.0.0.1:8091"


def test_discover_waits_for_next_packet_when_cache_is_empty(monkeypatch) -> None:
    reset_runtime_state()
    monkeypatch.setattr(app_module, "DISCOVERY_WAIT_SECONDS", 1.0)
    monkeypatch.setattr(app_module, "persist_discovery_cache", lambda: None)

    async def run() -> list[dict[str, object]]:
        discovery_task = asyncio.create_task(app_module.wait_for_discovery_devices())
        await asyncio.sleep(0.05)
        await app_module.process_rtl433_packet(
            {
                "model": "Nexus-TH",
                "id": 42,
                "temperature_C": 23.1,
                "humidity": 51,
            },
            source_transport="http",
        )
        return await discovery_task

    devices = asyncio.run(run())

    assert len(devices) == 1
    assert devices[0]["device_id"] == "Nexus-TH::42::na"


def test_discovery_cache_round_trip(tmp_path, monkeypatch) -> None:
    reset_runtime_state()
    cache_path = tmp_path / "discovery.json"
    monkeypatch.setattr(app_module, "DISCOVERY_CACHE_PATH", cache_path)

    app_module.remember_discovered_device(
        {
            "model": "Nexus-TH",
            "id": 42,
            "temperature_C": 23.1,
            "humidity": 51,
        }
    )
    recent_seen_devices.clear()
    app_module.load_discovery_cache()

    devices = list(recent_seen_devices.values())
    assert len(devices) == 1
    assert devices[0]["device_id"] == "Nexus-TH::42::na"


def test_config_entities_state_and_deconfigure_round_trip() -> None:
    reset_runtime_state()
    client = TestClient(app)

    config_response = client.post(
        "/config",
        json={
            "id": "cfg-weather-1",
            "profile": "weather_basic",
            "model": "Nexus-TH",
            "station_id": "42",
            "channel": "1",
            "alias": "Backyard Sensor",
        },
    )
    entities_response = client.get("/entities")
    state_response = client.get("/state")
    deconfigure_response = client.post("/deconfigure/cfg-weather-1")

    assert config_response.status_code == 200
    assert config_response.json()["status"] == "configured"
    assert config_response.json()["metadata"]["device_id"] == "Nexus-TH::42::1"
    assert config_response.json()["metadata"]["device_key"] == "Nexus-TH::42::1"

    entities = entities_response.json()
    assert any(entity["id"] == "Nexus-TH::42::1.temperature_c" for entity in entities)
    assert any(entity["name"] == "Backyard Sensor Temperature C" for entity in entities)

    state_payload = state_response.json()
    assert state_payload["summary"]["active_config_count"] == 1
    assert "cfg-weather-1" in state_payload["entries"]

    assert deconfigure_response.status_code == 200
    assert deconfigure_response.json()["removed"] is True
    assert deconfigure_response.json()["metadata"]["remaining_configs"] == []


def test_ingest_matching_config_updates_state_and_emits_event(monkeypatch) -> None:
    reset_runtime_state()
    client = TestClient(app)
    telemetry_calls: list[dict[str, object]] = []

    def fake_schedule_telemetry_delivery(**kwargs) -> None:
        telemetry_calls.append(kwargs)

    monkeypatch.setattr(
        app_module,
        "schedule_telemetry_delivery",
        fake_schedule_telemetry_delivery,
    )

    client.post(
        "/config",
        json={
            "id": "cfg-contact-1",
            "profile": "contact_sensor",
            "model": "Generic-Contact",
            "station_id": "7",
            "alias": "Garage Door",
        },
    )

    ingest_response = client.post(
        "/ingest/rtl433",
        json={
            "model": "Generic-Contact",
            "id": 7,
            "state": "open",
            "battery_ok": 1,
        },
    )
    state_response = client.get("/state")
    events_response = client.get("/events")

    assert ingest_response.status_code == 200
    payload = ingest_response.json()
    assert payload["matched_configs"] == 1
    assert payload["profile"] == "contact_sensor"
    assert "Contact Open" in payload["metric_names"]

    snapshot = state_response.json()["state_snapshots"]["cfg-contact-1"]["state"]
    assert snapshot["last_metrics"]["contact_open"] is True
    assert snapshot["station_id"] == "7"

    assert telemetry_calls[0]["device_id"] == "Generic-Contact::7::na"
    assert telemetry_calls[0]["metrics"]["contact_open"] is True

    event_types = [event["event_type"] for event in events_response.json()["events"]]
    assert "rtl433.packet.matched" in event_types


def test_leak_packets_emit_only_real_transitions_to_core(monkeypatch) -> None:
    reset_runtime_state()
    client = TestClient(app)
    event_deliveries: list[dict] = []

    monkeypatch.setattr(
        app_module,
        "schedule_telemetry_delivery",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        app_module,
        "schedule_event_delivery",
        lambda **kwargs: event_deliveries.append(kwargs),
    )
    client.post(
        "/config",
        json={
            "id": "cfg-leak-1",
            "profile": "leak_sensor",
            "model": "Generic-Leak",
            "station_id": "8",
            "alias": "Utility Room Leak Sensor",
        },
    )

    def ingest(value: int) -> None:
        response = client.post(
            "/ingest/rtl433",
            json={
                "model": "Generic-Leak",
                "id": 8,
                "leak": value,
                "battery_ok": 1,
            },
        )
        assert response.status_code == 200

    ingest(0)
    ingest(0)
    assert event_deliveries == []

    partial_response = client.post(
        "/ingest/rtl433",
        json={"model": "Generic-Leak", "id": 8, "battery_ok": 1},
    )
    assert partial_response.status_code == 200
    assert event_deliveries == []
    assert client.get("/state").json()["state_snapshots"]["cfg-leak-1"]["state"][
        "last_metrics"
    ]["leak_detected"] is False

    ingest(1)
    ingest(1)
    ingest(0)

    assert [delivery["event_type"] for delivery in event_deliveries] == [
        "rtl433.safety.leak.detected",
        "rtl433.safety.leak.cleared",
    ]
    assert all(delivery["severity"] == "warning" for delivery in event_deliveries)
    assert all(
        delivery["device"]["device_id"] == "Generic-Leak::8::na"
        for delivery in event_deliveries
    )
    assert event_deliveries[0]["payload"] == {
        "capability": "leak_detected",
        "previous_value": False,
        "current_value": True,
        "profile": "leak_sensor",
        "transport": "http",
    }

    event_types = [
        event["event_type"]
        for event in client.get("/events").json()["events"]
    ]
    assert event_types.count("rtl433.safety.leak.detected") == 1
    assert event_types.count("rtl433.safety.leak.cleared") == 1


def test_leak_transition_delivers_authenticated_event_to_mock_core(
    mock_core,
    monkeypatch,
) -> None:
    reset_runtime_state()
    monkeypatch.setattr(app_module.telemetry, "core_base_url", mock_core.base_url)
    monkeypatch.setattr(app_module.event_client, "core_base_url", mock_core.base_url)
    payload = build_config_payload(
        config_id="cfg-leak-core-1",
        container_id="runtime-leak-123",
        integration_id=app_module.INTEGRATION_ID,
        extra={
            "profile": "leak_sensor",
            "model": "Generic-Leak",
            "station_id": "18",
            "alias": "Laundry Leak Sensor",
        },
    )
    headers = build_runtime_headers(
        container_id="runtime-leak-123",
        internal_token="secret-token",
    )

    with TestClient(app) as client:
        assert client.post("/config", json=payload, headers=headers).status_code == 200
        assert client.post(
            "/ingest/rtl433",
            json={"model": "Generic-Leak", "id": 18, "leak": 0},
            headers=headers,
        ).status_code == 200
        assert client.post(
            "/ingest/rtl433",
            json={"model": "Generic-Leak", "id": 18, "leak": 1},
            headers=headers,
        ).status_code == 200

        wait_for(lambda: len(mock_core.event_requests) >= 1)
        event_request = mock_core.assert_event_sent(
            device_id="Generic-Leak::18::na",
            config_id="cfg-leak-core-1",
            event_type="rtl433.safety.leak.detected",
        )
        event_headers = {
            key.lower(): value for key, value in event_request.headers.items()
        }
        assert event_headers["x-container-id"] == "runtime-leak-123"
        assert event_headers["x-piphi-integration-token"] == "secret-token"
        assert event_request.json_body["severity"] == "warning"
        assert event_request.json_body["data"]["current_value"] is True


def test_manifest_declares_fail_closed_security_coverage() -> None:
    manifest = json.loads(
        (Path(__file__).resolve().parents[1] / "src" / "manifest.json").read_text()
    )
    mappings = manifest["security"]["event_mappings"]

    assert manifest["security"]["contract_version"] == "1"
    assert len({mapping["source_event_type"] for mapping in mappings}) == len(mappings)
    assert all(mapping["device_models"] for mapping in mappings)
    assert all(mapping["required_permissions"] for mapping in mappings)
    assert {
        mapping["source_event_type"]: mapping["canonical_event_type"]
        for mapping in mappings
        if mapping["status"] == "implemented"
    } == {
        "rtl433.safety.leak.detected": "safety_leak_detected",
        "rtl433.safety.leak.cleared": "safety_leak_cleared",
    }
    assert {
        mapping["source_event_type"]
        for mapping in mappings
        if mapping["status"] == "excluded"
    } == {"rtl433.contact.opened", "rtl433.packet.matched"}


def test_ingest_generic_config_uses_inferred_weather_metrics(monkeypatch) -> None:
    reset_runtime_state()
    client = TestClient(app)
    telemetry_calls: list[dict[str, object]] = []

    def fake_schedule_telemetry_delivery(**kwargs) -> None:
        telemetry_calls.append(kwargs)

    monkeypatch.setattr(
        app_module,
        "schedule_telemetry_delivery",
        fake_schedule_telemetry_delivery,
    )

    client.post(
        "/config",
        json={
            "id": "cfg-weather-generic",
            "profile": "generic_sensor",
            "model": "Fineoffset-WH0290",
            "station_id": "62",
            "alias": "Weather Sensor",
        },
    )

    ingest_response = client.post(
        "/ingest/rtl433",
        json={
            "model": "Fineoffset-WH0290",
            "id": 62,
            "temperature_C": 21.5,
            "humidity": 48,
            "pressure_hPa": 1009.8,
            "battery_ok": 1,
        },
    )
    entities_response = client.get("/entities")

    assert ingest_response.status_code == 200
    assert ingest_response.json()["profile"] == "weather_basic"
    assert telemetry_calls[0]["metrics"] == {
        "temperature_c": 21.5,
        "humidity_percent": 48,
        "pressure_hpa": 1009.8,
        "battery_ok": 1,
    }
    assert telemetry_calls[0]["units"] == {
        "temperature_c": "C",
        "humidity_percent": "%",
        "pressure_hpa": "hPa",
    }
    assert any(entity["id"] == "Fineoffset-WH0290::62::na.temperature_c" for entity in entities_response.json())
    assert any(entity["id"] == "Fineoffset-WH0290::62::na.pressure_hpa" for entity in entities_response.json())


def test_ingest_matching_config_sends_telemetry_to_mock_core(
    mock_core,
    monkeypatch,
) -> None:
    reset_runtime_state()
    monkeypatch.setattr(app_module.telemetry, "core_base_url", mock_core.base_url)

    payload = build_config_payload(
        config_id="cfg-contact-1",
        container_id="runtime-123",
        integration_id=app_module.INTEGRATION_ID,
        extra={
            "profile": "contact_sensor",
            "model": "Generic-Contact",
            "station_id": "7",
            "alias": "Garage Door",
        },
    )
    headers = build_runtime_headers(container_id="runtime-123", internal_token="secret-token")

    with TestClient(app) as client:
        config_response = client.post("/config", json=payload, headers=headers)
        ingest_response = client.post(
            "/ingest/rtl433",
            json={
                "model": "Generic-Contact",
                "id": 7,
                "state": "open",
                "battery_ok": 1,
            },
            headers=headers,
        )

        assert config_response.status_code == 200
        assert ingest_response.status_code == 200

        wait_for(lambda: len(mock_core.telemetry_requests) >= 1)

        telemetry_request = assert_telemetry_sent(mock_core, device_id="Generic-Contact::7::na")
        telemetry_headers = {key.lower(): value for key, value in telemetry_request.headers.items()}

        assert telemetry_headers["x-container-id"] == "runtime-123"
        assert telemetry_headers["x-piphi-integration-token"] == "secret-token"
        assert telemetry_request.json_body["metrics"]["contact_open"] is True
        assert "contact_open" not in telemetry_request.json_body["units"]


def test_config_sync_replaces_existing_config_using_testkit_builders() -> None:
    reset_runtime_state()

    old_payload = build_config_payload(
        config_id="cfg-old",
        container_id="runtime-123",
        integration_id=app_module.INTEGRATION_ID,
        extra={
            "profile": "weather_basic",
            "model": "Nexus-TH",
            "station_id": "42",
            "channel": "1",
            "alias": "Backyard Sensor",
        },
    )
    new_payload = build_config_payload(
        config_id="cfg-new",
        container_id="runtime-123",
        integration_id=app_module.INTEGRATION_ID,
        extra={
            "profile": "contact_sensor",
            "model": "Generic-Contact",
            "station_id": "7",
            "alias": "Garage Door",
        },
    )
    snapshot = build_config_snapshot(
        configs=[new_payload],
        container_id="runtime-123",
        integration_id=app_module.INTEGRATION_ID,
        generation=5,
    )
    headers = build_runtime_headers(container_id="runtime-123", internal_token="secret-token")

    with TestClient(app) as client:
        config_response = client.post("/config", json=old_payload, headers=headers)
        sync_response = client.post("/config/sync", json=snapshot, headers=headers)
        state_response = client.get("/state")

    assert config_response.status_code == 200
    assert sync_response.status_code == 200

    sync_json = sync_response.json()
    assert sync_json["applied"] == ["cfg-new"]
    assert sync_json["removed"] == ["cfg-old"]
    assert sync_json["active_config_ids"] == ["cfg-new"]
    assert sync_json["generation"] == 5

    state_json = state_response.json()
    assert "cfg-old" not in state_json["entries"]
    assert "cfg-new" in state_json["entries"]
    assert state_json["entries"]["cfg-new"]["profile"] == "contact_sensor"


def test_command_clear_discovery_cache_clears_records() -> None:
    reset_runtime_state()
    client = TestClient(app)

    client.post(
        "/ingest/rtl433",
        json={"model": "Nexus-TH", "id": 42, "temperature_C": 23.1},
    )

    response = client.post("/command", json={"command": "clear_discovery_cache"})

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["result"]["cleared_discovery_records"] == 1
    assert list(recent_seen_devices.values()) == []


def test_command_clear_discovery_cache_handles_empty_cache() -> None:
    reset_runtime_state()
    client = TestClient(app)

    response = client.post("/command", json={"command": "clear_discovery_cache"})

    assert response.status_code == 200
    assert response.json()["result"]["cleared_discovery_records"] == 0


def test_command_clear_cache_replay_does_not_repeat_cache_mutation() -> None:
    reset_runtime_state()
    client = TestClient(app)
    client.post(
        "/ingest/rtl433",
        json={"model": "Nexus-TH", "id": 42, "temperature_C": 23.1},
    )
    headers = {"X-PiPhi-Idempotency-Key": "rtl433-clear-cache-idempotency-1"}

    first = client.post(
        "/command",
        json={"command": "clear_discovery_cache"},
        headers=headers,
    )
    client.post(
        "/ingest/rtl433",
        json={"model": "Nexus-TH", "id": 43, "temperature_C": 24.1},
    )
    replay = client.post(
        "/command",
        json={"command": "clear_discovery_cache"},
        headers=headers,
    )

    assert first.status_code == 200
    assert replay.status_code == 200
    assert first.json()["replayed"] is False
    assert replay.json()["replayed"] is True
    assert replay.json()["result"]["cleared_discovery_records"] == 1
    assert len(recent_seen_devices) == 1


def test_command_returns_unsupported_for_unknown_command() -> None:
    reset_runtime_state()
    client = TestClient(app)

    response = client.post("/command", json={"command": "do_something_else"})

    assert response.status_code == 200
    assert response.json()["status"] == "unsupported"
    assert "Unsupported command" in response.json()["result"]["message"]


def test_ingest_supports_alternate_identity_fields() -> None:
    reset_runtime_state()
    client = TestClient(app)

    response = client.post(
        "/ingest/rtl433",
        json={
            "model": "WeatherSensor",
            "device_id": "sensor-9",
            "subtype": "A",
            "temperature_C": 18.4,
        },
    )
    discover_response = client.post("/discover", json={"inputs": {}})

    assert response.status_code == 200
    assert response.json()["device_id"] == "WeatherSensor::sensor-9::A"

    devices = discover_response.json()["devices"]
    assert devices[0]["station_id"] == "sensor-9"
    assert devices[0]["channel"] == "A"


def test_deconfigure_returns_false_for_missing_config() -> None:
    reset_runtime_state()
    client = TestClient(app)

    response = client.post("/deconfigure/missing-config")

    assert response.status_code == 200
    assert response.json()["removed"] is False
    assert response.json()["status"] == "deconfigured"


def test_health_and_diagnostics_reflect_runtime_state() -> None:
    reset_runtime_state()
    client = TestClient(app)

    client.post(
        "/config",
        json={
            "id": "cfg-generic-1",
            "profile": "generic_sensor",
            "model": "Basic-Sensor",
            "station_id": "5",
        },
    )
    client.post(
        "/ingest/rtl433",
        json={"model": "Basic-Sensor", "id": 5, "battery_ok": 1},
    )

    health_response = client.get("/health")
    diagnostics_response = client.get("/diagnostics")

    assert health_response.status_code == 200
    assert health_response.json()["metadata"]["active_configs"] == 1
    assert health_response.json()["metadata"]["recent_discovery_count"] == 1

    assert diagnostics_response.status_code == 200
    diagnostics = diagnostics_response.json()["diagnostics"]
    assert diagnostics["active_config_ids"] == ["cfg-generic-1"]
    assert diagnostics["recent_event_count"] >= 1
    assert "generic_sensor" in [profile["id"] for profile in diagnostics["supported_profiles"]]


def test_ingest_unmatched_packet_emits_discovery_event_and_skips_telemetry(monkeypatch) -> None:
    reset_runtime_state()
    client = TestClient(app)
    telemetry_calls: list[dict[str, object]] = []

    def fake_schedule_telemetry_delivery(**kwargs) -> None:
        telemetry_calls.append(kwargs)

    monkeypatch.setattr(app_module, "schedule_telemetry_delivery", fake_schedule_telemetry_delivery)

    response = client.post(
        "/ingest/rtl433",
        json={"model": "Leak-Sensor", "id": 11, "water": 1},
    )
    events_response = client.get("/events")

    assert response.status_code == 200
    assert response.json()["matched_configs"] == 0
    assert response.json()["profile"] == "leak_sensor"
    assert telemetry_calls == []
    event_types = [event["event_type"] for event in events_response.json()["events"]]
    assert "rtl433.packet.discovered" in event_types


def test_handle_mqtt_packet_ignores_invalid_envelope() -> None:
    reset_runtime_state()

    result = asyncio.run(app_module.handle_mqtt_packet("piphi/sources/rtl433/packets", {"packet": "bad"}))

    assert result is None
    assert list(recent_seen_devices.values()) == []
    assert registry.recent_events == []


def test_events_limit_returns_only_requested_count() -> None:
    reset_runtime_state()
    client = TestClient(app)

    client.post("/ingest/rtl433", json={"model": "Sensor-A", "id": 1, "battery_ok": 1})
    client.post("/ingest/rtl433", json={"model": "Sensor-B", "id": 2, "battery_ok": 1})
    client.post("/ingest/rtl433", json={"model": "Sensor-C", "id": 3, "battery_ok": 1})

    response = client.get("/events?limit=2")

    assert response.status_code == 200
    assert len(response.json()["events"]) == 2


def test_config_sync_removes_missing_configs_from_snapshot() -> None:
    reset_runtime_state()
    client = TestClient(app)

    client.post(
        "/config",
        json={
            "id": "cfg-old",
            "profile": "generic_sensor",
            "model": "Sensor-Old",
            "station_id": "1",
        },
    )

    response = client.post(
        "/config/sync",
        json={
            "container_id": "runtime-1",
            "integration_id": "piphi-network-433mhz",
            "generation": 7,
            "configs": [
                {
                    "id": "cfg-new",
                    "profile": "contact_sensor",
                    "model": "Sensor-New",
                    "station_id": "2",
                }
            ],
        },
    )
    state_response = client.get("/state")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "synced"
    assert payload["generation"] == 7
    assert payload["applied"] == ["cfg-new"]
    assert payload["removed"] == ["cfg-old"]
    assert payload["active_config_ids"] == ["cfg-new"]

    state_payload = state_response.json()
    assert "cfg-old" not in state_payload["entries"]
    assert "cfg-new" in state_payload["entries"]
