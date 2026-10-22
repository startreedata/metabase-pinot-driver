#!/usr/bin/env python3
"""Exercise a candidate driver JAR in the pinned Metabase against a Pinot quickstart.

Requires Python 3.9+, Java 25, and a Pinot controller with the baseballStats sample table.
Uses only Python's standard library. Credentials and the disposable H2 database are
removed on exit; Metabase logs and query evidence remain in --output-dir.
"""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def request(url, body=None, session=None):
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            content = response.read()
            return json.loads(content) if content else None
    except urllib.error.HTTPError as exc:
        # Setup includes a disposable password; never echo its validation payload.
        detail = "setup response omitted" if url.endswith("/api/setup") else exc.read().decode()[:2000]
        raise RuntimeError(f"{req.get_method()} {url}: HTTP {exc.code}: {detail}") from None


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver-jar", type=Path, required=True)
    parser.add_argument("--metabase-jar", type=Path, required=True)
    parser.add_argument("--controller-url", default="http://localhost:9000")
    parser.add_argument("--pinot-version", help="Expected Pinot version; defaults to app_versions.json")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--startup-timeout", type=int, default=180)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    versions = json.loads((root / "app_versions.json").read_text())
    target = versions["metabase"]
    pinot_target = args.pinot_version or versions["pinot"]
    for path in (args.driver_jar, args.metabase_jar):
        require(path.is_file(), f"JAR does not exist: {path}")
    with zipfile.ZipFile(args.metabase_jar) as jar:
        version = dict(line.split("=", 1) for line in jar.read("version.properties").decode().splitlines()
                       if "=" in line and not line.startswith("#"))
    require(version.get("tag") == target,
            f"Metabase JAR is {version.get('tag')}; app_versions.json requires {target}")
    with zipfile.ZipFile(args.driver_jar) as jar:
        manifest = jar.read("metabase-plugin.yaml").decode()
    driver_version = next((line.partition(":")[2].strip().strip("\"'")
                           for line in manifest.splitlines() if line.startswith("  version:")), None)
    require(driver_version == target.removeprefix("v"),
            f"Driver version is {driver_version}; expected {target.removeprefix('v')}")
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else shutil.which("java")
    require(java, "Java is required")
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = (args.output_dir or root / "target" / f"smoke-test-{stamp}").resolve()
    output.mkdir(parents=True, exist_ok=True)
    evidence = {"metabase": version, "driver_version": driver_version,
                "driver_sha256": sha256(args.driver_jar),
                "metabase_sha256": sha256(args.metabase_jar),
                "controller_url": args.controller_url, "expected_pinot_version": pinot_target, "checks": []}
    controller = args.controller_url.rstrip("/")

    def pinot_rows(sql):
        result = request(controller + "/sql", {"sql": sql})
        require(not result.get("exceptions"), f"Pinot rejected reference SQL: {result.get('exceptions')}")
        return result["resultTable"]["rows"]

    process = None
    temporary = tempfile.TemporaryDirectory(prefix="metabase-pinot-smoke-")
    log = (output / "metabase.log").open("w")

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("Smoke test interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        evidence["pinot_version"] = request(controller + "/version")
        actual_pinot_version = evidence["pinot_version"].get("pinot-distribution", "")
        require(actual_pinot_version.split("-", 1)[0] == pinot_target,
                f"Pinot is {actual_pinot_version}; expected {pinot_target}")
        work = Path(temporary.name)
        plugins = work / "plugins"
        plugins.mkdir()
        shutil.copy2(args.driver_jar, plugins / "pinot.metabase-driver.jar")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        # Never inherit an application's DB credentials or plugin directory.
        env = {key: value for key, value in os.environ.items() if not key.startswith("MB_")}
        env.update(MB_DB_TYPE="h2", MB_DB_FILE=str(work / "app-db"),
                   MB_JETTY_HOST="127.0.0.1", MB_JETTY_PORT=str(port),
                   MB_PLUGINS_DIR=str(plugins), MB_ANON_TRACKING_ENABLED="false",
                   MB_CHECK_FOR_UPDATES="false", MB_LOAD_SAMPLE_CONTENT="false")
        process = subprocess.Popen([java, "--add-opens=java.base/java.nio=ALL-UNNAMED", "-Xmx2g",
                                    "-jar", str(args.metabase_jar.resolve())],
                                   cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + args.startup_timeout
        while True:
            require(process.poll() is None, f"Metabase exited during startup; see {output / 'metabase.log'}")
            try:
                if request(base + "/api/health").get("status") == "ok":
                    break
            except (RuntimeError, urllib.error.URLError):
                pass
            require(time.monotonic() < deadline, f"Metabase startup timed out; see {output / 'metabase.log'}")
            time.sleep(1)
        props = request(base + "/api/session/properties")
        require(props["version"]["tag"] == target, "Running Metabase version differs from the build pin")
        require("pinot" in props.get("engines", {}), "Packaged Pinot driver was not discovered")
        session = request(base + "/api/setup", {
            "token": props["setup-token"],
            "user": {"first_name": "Driver", "last_name": "Smoke", "email": "smoke@example.invalid",
                     "password": "Local1!" + secrets.token_urlsafe(30)},
            "prefs": {"site_name": "Disposable Pinot compatibility check"}})["id"]

        def api(path, body=None):
            return request(base + "/api" + path, body, session)

        database = api("/database", {"name": "Pinot compatibility", "engine": "pinot", "is_full_sync": True,
                                     "details": {"controller-endpoint": controller}})
        db_id = database["id"]
        require(database["engine"] == "pinot", "Database did not load the Pinot engine")
        api(f"/database/{db_id}/sync_schema", {})
        deadline = time.monotonic() + 120
        while True:
            metadata = api(f"/database/{db_id}/metadata")
            table = next((table for table in metadata.get("tables", []) if table["name"] == "baseballStats"), None)
            fields = {field["name"]: field for field in (table or {}).get("fields", [])}
            if all(name in fields for name in ("playerID", "yearID", "league")):
                break
            require(time.monotonic() < deadline, "Pinot schema sync did not return baseballStats fields")
            time.sleep(1)
        evidence["schema"] = {"table": table["name"], "field_count": len(fields),
                              "table_count": len(metadata["tables"])}
        evidence["checks"].extend(["packaged-engine-load", "connection", "schema-sync"])
        (output / "schema.json").write_text(json.dumps(metadata, indent=2) + "\n")

        def query(label, body, expected=None, ordered=False):
            result = api("/dataset", dict(body, database=db_id))
            (output / (label + ".json")).write_text(json.dumps(result, indent=2) + "\n")
            require(result.get("status") == "completed", f"{label} failed: {result.get('error', result.get('status'))}")
            rows = result["data"]["rows"]
            if expected is not None:
                # Metabase may serialize large integers as strings for its JS client.
                def normalized(data):
                    values = [tuple(str(value) for value in row) for row in data]
                    return values if ordered else sorted(values)
                require(normalized(rows) == normalized(expected), f"{label} differs from direct Pinot results")
            evidence["checks"].append(label)
            print(f"PASS {label} ({len(rows)} result rows)", flush=True)
            return rows

        def native(label, sql, expected, tags=None, parameters=None):
            body = {"type": "native", "native": {"query": sql, "template-tags": tags or {}},
                    "parameters": parameters or []}
            return query(label, body, expected)

        count_sql = "SELECT COUNT(*) FROM baseballStats"
        total = pinot_rows(count_sql)
        require(int(total[0][0]) > 0, "baseballStats must contain sample rows")
        evidence["baseballStats_count"] = total[0][0]
        native("native-count", count_sql, total)

        def field(name):
            return ["field", fields[name]["id"], {"base-type": fields[name]["base_type"]}]

        selected = query("mbql-fields", {"type": "query", "query": {"source-table": table["id"],
                         "fields": [field("playerID"), field("yearID")], "limit": 5}})
        require(len(selected) == 5 and all(len(row) == 2 for row in selected), "MBQL did not select both requested fields")
        query("mbql-count", {"type": "query", "query": {"source-table": table["id"],
              "aggregation": [["count"]]}}, total)
        query("mbql-breakout", {"type": "query", "query": {"source-table": table["id"],
              "aggregation": [["count"]], "breakout": [field("league")]}},
              pinot_rows("SELECT league, COUNT(*) FROM baseballStats GROUP BY league"))
        query("mbql-sum-order", {"type": "query", "query": {"source-table": table["id"],
              "aggregation": [["sum", field("numberOfGames")]], "breakout": [field("league")],
              "order-by": [["desc", ["aggregation", 0]]], "limit": 3}},
              pinot_rows("SELECT league, SUM(numberOfGames) AS total FROM baseballStats GROUP BY league ORDER BY total DESC LIMIT 3"),
              ordered=True)

        def tag(name, kind):
            return {name: {"id": str(uuid.uuid4()), "name": name, "display-name": name, "type": kind}}

        def parameter(name, kind, value):
            return [{"type": kind, "target": ["variable", ["template-tag", name]], "value": value}]

        number_tags = tag("year", "number")
        number_params = parameter("year", "number", "2004")
        year_count = pinot_rows(count_sql + " WHERE yearID = 2004")
        require(int(year_count[0][0]) > 0, "baseballStats must contain yearID 2004")
        native("native-number-parameter", count_sql + " WHERE yearID = {{year}}", year_count, number_tags, number_params)
        text_tags = tag("league", "text")
        league_count = pinot_rows(count_sql + " WHERE league = 'NL'")
        require(int(league_count[0][0]) > 0, "baseballStats must contain league NL")
        native("native-text-parameter", count_sql + " WHERE league = {{league}}", league_count,
               text_tags, parameter("league", "category", "NL"))
        field_tags = {"league_filter": {"id": str(uuid.uuid4()), "name": "league_filter",
                      "display-name": "League", "type": "dimension", "dimension": field("league"),
                      "widget-type": "category"}}
        native("native-field-filter", count_sql + " WHERE {{league_filter}}", league_count, field_tags,
               [{"type": "string/=", "target": ["dimension", ["template-tag", "league_filter"]], "value": ["NL"]}])
        optional_sql = count_sql + " WHERE 1=1 [[AND yearID = {{year}}]]"
        native("native-optional-omitted", optional_sql, total, number_tags)
        native("native-optional-provided", optional_sql, year_count, number_tags, number_params)
        evidence["status"] = "passed"
    except (Exception, KeyboardInterrupt) as exc:
        evidence.update(status="failed", error=str(exc))
        print(f"FAIL {exc}", file=sys.stderr)
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        log.close()
        temporary.cleanup()
        (output / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print(f"Evidence: {output}", flush=True)
    return 0 if evidence.get("status") == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
