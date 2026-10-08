from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "experiences/sensors/package.source.json"


def test_native_widgets_match_integration_contract() -> None:
    manifest = json.loads((ROOT / "src/manifest.json").read_text(encoding="utf-8"))
    package = json.loads(SOURCE.read_text(encoding="utf-8"))
    identity = package["identity"]
    assert package["owning_integration_id"] == manifest["id"]
    assert identity["version"] == manifest["version"]
    assert manifest["ui"]["experience_packages"] == [
        {
            "registry_id": f"{identity['publisher_id']}.{identity['package_id']}",
            "version_range": ">=0.1,<1",
            "auto_install": True,
        }
    ]
    assert package["sdk_version_range"] == ">=0.6.1,<0.7"
    widgets = package["widgets"]
    assert len(widgets) == 3
    assert len({widget["id"] for widget in widgets}) == len(widgets)
    for widget in widgets:
        assert widget["runtime"] == "declarative"
        assert widget["presentation"]["shell"] == "core"
        assert "permissions" not in widget
        assert "allowed_commands" not in widget
        slots = {slot["id"]: slot for slot in widget["binding_slots"]}
        assert any(slot["required"] for slot in slots.values())
        assert {item["slot_id"] for item in widget["recipe"]["items"]} == set(slots)
        assert {
            target["binding_slot_id"]
            for target in widget["interaction_targets"]
            if target["kind"] == "binding"
        } == set(slots)
        for slot in slots.values():
            assert slot["binding_modes"] == ["read"]
            assert slot["compatible_integration_ids"] == [manifest["id"]]
            assert set(slot["capability_requirements"]) <= set(manifest["capabilities"])
