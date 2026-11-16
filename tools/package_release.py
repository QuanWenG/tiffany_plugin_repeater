"""Validate the built wheel and create a source archive from an explicit file list.

Build the wheel first with: python -m pip wheel . --no-deps --wheel-dir dist
Then run: python tools/package_release.py
For a deployment patch verified by regression checks, explicitly pass
--deployment-report audit/release-<version>.json. The default requires scale acceptance.
"""
import argparse
import hashlib
import json
import tomllib
import zipfile
from email.parser import Parser
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-report", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    version = tomllib.loads((root / "pyproject.toml").read_text("utf-8"))["project"]["version"]
    distribution = root / "dist"
    wheel = distribution / f"tiffany_plugin_repeater-{version}-py3-none-any.whl"
    python_files = sorted((root / "tiffany_plugin_repeater").glob("*.py"))
    package_files = python_files + sorted((root / "tiffany_plugin_repeater" / "web").glob("*.html"))
    with zipfile.ZipFile(wheel) as archive:
        for path in package_files:
            assert archive.read(path.relative_to(root).as_posix()) == path.read_bytes(), path
        metadata_name = next(name for name in archive.namelist() if name.endswith(".dist-info/METADATA"))
        metadata = Parser().parsestr(archive.read(metadata_name).decode())
        assert metadata["Version"] == version and metadata["Requires-Python"] == ">=3.11"
        entry_points = next(name for name in archive.namelist() if name.endswith(".dist-info/entry_points.txt"))
        assert "repeater = tiffany_plugin_repeater:install" in archive.read(entry_points).decode()
        assert not any("__pycache__" in name or name.endswith(".sqlite3") for name in archive.namelist())

    historical_report = root / "audit" / "intelligence-scale-20261023.json"
    report_path = args.deployment_report.resolve() if args.deployment_report else historical_report
    report = json.loads(report_path.read_text("utf-8"))
    source_digest = hashlib.sha256(b"".join(path.read_bytes() for path in python_files)).hexdigest()
    assert report["source_sha256"] == source_digest, "Verification does not match the packaged source"
    if args.deployment_report:
        assert report["kind"] == "deployment" and report["version"] == version
        tests, lint = report["checks"]["unittest"], report["checks"]["ruff"]
        assert tests["exit_code"] == 0 and tests["tests"] > 0 and tests["failures"] == tests["errors"] == 0
        assert lint["exit_code"] == 0
    else:
        assert report["passed"], "Full scale acceptance has not passed"
        assert report["seconds"] >= 1800 and report["rate"] >= 50 and report["groups"] >= 200
        assert report["seed_evidence"] >= 2_000_000

    included = [root / name for name in ("pyproject.toml", "README.md", "LICENSE", "NOTICE.md",
                                        "INTELLIGENCE.md", "CHANGELOG.md", "IMPLEMENTATION_REVIEW.md", "DEPLOYMENT.md")]
    included += package_files
    for folder in ("examples", "tests", "tools"):
        included += [path for path in (root / folder).rglob("*")
                     if path.is_file() and "__pycache__" not in path.parts
                     and path.suffix in (".py", ".toml", ".md", ".sqlite3")]
    included.append(report_path)
    included.append(historical_report)
    included += [path for name in ("hardware.json", "data-integrity.json", "dashboard-browser.json")
                 if (path := root / "audit" / name).is_file()]
    source = distribution / f"tiffany-plugin-repeater-{version}-source.zip"
    with zipfile.ZipFile(source, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(set(included)):
            archive.write(path, "repeater/" + path.relative_to(root).as_posix())
    checksum = distribution / f"SHA256SUMS-{version}.txt"
    checksum.write_text("".join(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + path.name + "\n"
                               for path in (wheel, source)), encoding="utf-8")
    print(json.dumps({"version": version, "wheel": str(wheel), "source": str(source),
                      "source_sha256": source_digest, "files": len(set(included)),
                      "verification": "deployment" if args.deployment_report else "scale"}))


if __name__ == "__main__":
    main()
