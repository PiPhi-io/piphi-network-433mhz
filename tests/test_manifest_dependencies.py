import json
from pathlib import Path


def test_manifest_recommends_the_tested_rtl433_sidecar_release() -> None:
    manifest = json.loads(
        (Path(__file__).resolve().parents[1] / "src" / "manifest.json").read_text()
    )
    dependency = next(
        service
        for service in manifest["requires"]["services"]
        if service["id"] == "piphi.service.rtl433-bridge"
    )

    assert dependency["version_range"] == ">=0.1.0,<1.0.0"
    assert dependency["recommended_version"] == "0.1.4"
    assert dependency["auto_install"] is True
