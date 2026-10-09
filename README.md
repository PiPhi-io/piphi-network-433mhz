# piphi-network-433mhz

PiPhi integration for `rtl_433` compatible sub-GHz devices.

This runtime is built on the published Python SDK, `piphi-runtime-kit-python`,
and is designed to work alongside the shared `rtl_433 Radio Bridge` platform
service that listens for radio packets and forwards decoded JSON into the runtime.

## What this integration does

- accepts passive `rtl_433` packets through `POST /ingest/rtl433`
- keeps a cache of recently seen devices for discovery and configuration
- lets users configure discovered devices by profile and identity fields
- exposes PiPhi routes for health, discovery, config, state, events, and commands
- publishes normalized telemetry back to PiPhi Core
- optionally subscribes to the shared MQTT packet topic instead of only relying on HTTP ingest

## Runtime SDK and testkit

This integration now installs the published runtime kit directly from PyPI:

- runtime SDK: `piphi-runtime-kit-python[fastapi,mqtt]==0.8.1`
- local test helper during development: `piphi-runtime-testkit-python`

You do not need a sibling checkout of the runtime SDK just to run the
integration. The local testkit path dependency is only used for the repo's
dev/test workflow.

## Supported starter profiles

The starter profile layer keeps the runtime useful before it becomes deeply
device-specific.

- `weather_basic`
- `contact_sensor`
- `leak_sensor`
- `generic_sensor`

You can expand the profile set over time as you support more `rtl_433` device
families.

## Project layout

- `src/manifest.json` PiPhi integration manifest
- `src/behaviors.json` optional behavior metadata for PiPhi UI
- `src/piphi_network_433mhz/app.py` FastAPI runtime
- `src/piphi_network_433mhz/profiles.py` profile inference and metric mapping
- `tests/` runtime and profile tests

## Local development

Install dependencies:

```bash
pdm install -G dev
```

Run the runtime locally:

```bash
pdm run dev
```

The runtime starts on `http://127.0.0.1:8090`.

Run tests:

```bash
pdm run pytest -q
```

## Important routes

- `GET /health`
- `GET /diagnostics`
- `POST /discover`
- `POST /config`
- `POST /config/sync`
- `POST /deconfigure/{config_id}`
- `GET /entities`
- `GET /state`
- `GET /events`
- `POST /command`
- `POST /ingest/rtl433`

The integration manifest declares optional discovery fields for the receive
frequency and USB receiver selector. When supplied, `/discover` applies them to
the required `piphi.service.rtl433-bridge` through its Core-provided service
binding before it waits for packets.

## Packet sources

The runtime can consume packets in two ways:

- direct HTTP posts to `/ingest/rtl433`
- optional MQTT subscription to the shared source topic

Shared packet topic:

```text
piphi/sources/rtl433/packets
```

Useful environment variables:

- `RTL433_MQTT_ENABLED=true`
- `MQTT_HOSTNAME=127.0.0.1`
- `MQTT_PORT=1883`
- `MQTT_TOPIC_ROOT=piphi/sources/rtl433`

When MQTT is enabled, the runtime reads the packet envelope's `packet` field
and then runs the same normalization path used by the HTTP ingest route.

## Example packet ingest

```bash
curl -X POST http://127.0.0.1:8090/ingest/rtl433 \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "Nexus-TH",
    "id": 42,
    "channel": 1,
    "temperature_C": 22.8,
    "humidity": 49,
    "battery_ok": 1
  }'
```

## Notes

- The main runtime does not talk to SDR hardware directly.
- The manifest requires the shared `piphi.service.rtl433-bridge` platform service so Core can install or reuse the radio bridge.
- Linux is the initial target because the helper/container story is strongest there.

## Hardware access notes

For real RTL-SDR use on Linux, the helper usually needs:

- `rtl_433` installed inside the helper container
- host USB access to the SDR dongle
- `network_mode: "host"`
- `privileged: true`
- an explicit USB device mapping such as `/dev/bus/usb`

That hardware contract lives on the shared `rtl_433 Radio Bridge` service. This
integration depends on that service instead of embedding a private sidecar.
