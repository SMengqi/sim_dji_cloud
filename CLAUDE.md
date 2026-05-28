# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

`sim-dji-cloud` is a Python toolkit for **recording** DJI Dock 3 cloud-API MQTT traffic during a flight and **replaying** it offline against a local broker, plus a **dashboard** for visual monitoring. The point is to capture real flight data once and run regression / integration tests against the same trace forever after.

Three subsystems delivered in phases (all live):

1. **Recorder** (`record`, `inspect`, `stop-record`, `validate-config`) — subscribe to DJI cloud broker, write topic-partitioned JSONL + manifest + optional RTMP video.
2. **Player + SelfCheck** (`play`, `selfcheck`, `list`, `repair`) — replay a flight directory to any broker; in-process loopback regression compares record-vs-replay timing/payload.
3. **Dashboard** (`dashboard`) — FastAPI + WebSocket + single-file HTML (Alpine.js + Leaflet) for headless-Linux-friendly browser monitoring.

All three share the same local MQTT broker as the contract surface.

## Common commands

```bash
# Setup (one-time; idempotent)
bash install.sh                          # creates .venv, pip install -e ".[test]" --upgrade, runs unit tests

# Activate venv
source .venv/bin/activate

# Tests
pytest tests/                            # full suite (~148 passed, integration tests need mosquitto; auto-skip)
pytest tests/unit/ -v                    # unit only, no external deps
pytest tests/integration/ -v             # integration; needs `mosquitto` binary in PATH
pytest tests/unit/test_dashboard_live_state.py::test_partial_dock_osd_preserves_previous_fields -v   # single test
pytest tests/ -k dashboard               # filter by name

# CLI smoke
sim-dji --help                           # 9 subcommands: dashboard / inspect / list / play / record / repair / selfcheck / stop-record / validate-config
sim-dji <cmd> --help                     # per-command help

# Typical workflow
cp recorder.yaml.example recorder.yaml   # then edit mqtt.dock_sn + creds
export DJI_MQTT_USERNAME=... DJI_MQTT_PASSWORD=...
sim-dji validate-config --config recorder.yaml
sim-dji record --config recorder.yaml    # Ctrl-C or `sim-dji stop-record <task_id>` from another shell
sim-dji inspect ./recordings/<flight_dir>/
sim-dji play ./recordings/<flight_dir>/ --mqtt-url tcp://localhost:1883 --speed 1.0
sim-dji selfcheck ./recordings/<flight_dir>/ --tolerance-ms 50
sim-dji dashboard --port 8080            # browser: http://<server>:8080
```

## Architecture you can't infer from a single file

### Dock vs drone are separate data sources

DJI Dock 3 reports under `thing/product/<device_sn>/...`. **Two devices** publish under this same pattern:

- The **dock** publishes `osd / state / events / services / requests / drc`. Dock OSDs carry a `data.sub_device.device_sn` pointing at the paired drone.
- The **drone** publishes **only `osd`**. Nothing else.

`drone_sn` is **derived** at runtime from the first dock OSD that carries `sub_device.device_sn` — never hardcoded. Topic routing must use a learned identity (dock_sn / drone_sn) rather than payload heuristics, because partial OSDs sometimes omit `sub_device`. See `recorder/topic_router.py` and `dashboard/live_state.py` for the two independent implementations of this rule.

Multi-dock shared brokers exist — the Recorder applies a `device_sn` whitelist filter so it only records its own dock + paired drone.

### Recording is async-pipelined per topic

`Recorder` (`src/sim_dji_cloud/recorder/__init__.py`) is the orchestrator. Each topic gets its own `TopicWriteQueue` (`write_queue.py`) backed by a `RotatingJsonlWriter` (`storage/rotation.py`); records are buffered and flushed on `flush_max_records` OR `flush_interval_ms` (whichever first — both matter, low-frequency topics depend on the time fallback). `FlightDetector` (`flight_detector.py`) is a **sticky `flighttask_step_code` state machine**: it tracks the dock's last-known `data.flighttask_step_code` (the field is sent only on state change, so absence ≠ a state — it keeps the last value), transitions `WAITING_TASK → RECORDING` when that value ∈ `record_steps` (default `{0,1,2}`) and `RECORDING → FINALIZING` after it leaves the set for `idle_debounce_seconds`. It names the flight directory `<task_id>__<dock_sn>__<ts>/` and triggers a synchronous rename of pending JSONL files (purely sync — never await mid-rename; a previous bug had concurrent gmqtt callbacks corrupting state during async drain). `reset()` returns it to `WAITING_TASK` for the next task (multi-task per process); `tick(now_ms)` advances the idle debounce when no messages arrive.

`MqttRecorderClient` (`recorder/mqtt_client.py`) wraps gmqtt and reports connection gaps into the manifest.

Storage contract (frozen, do not break casually):

```
<flight_dir>/
├── manifest.json                # topics[].files[] with offsets, gaps[], duration_ms, dock_sn, drone_sn
├── topics/
│   └── thing__product__<sn>__<suffix>.NNNN.jsonl    # `/` → `__`, volume-rotated
└── video/main.mp4 + main.timing.json
```

Each JSONL line: `{recv_ts_ms, dji_ts_ms, direction, topic, payload}`.

### Playback uses virtual time, not wall clock

`Player` (`player/` package) opens all topic files, builds a merged `SeekIndex`, and schedules each record through `VirtTimeScheduler`:

```
virt_time_ms = virt_zero_ms + (wall_now - play_start) * speed * 1000
```

That formula is the contract for `--speed` and `--start-offset-ms`. SelfCheck (`selfcheck/`) reuses Player + a tmp Recorder in the **same event loop** (in-process loopback) and runs `Comparator` for payload + drift assertions. The `tests/fixtures/real_flight_basic/` is a 220s anonymized real flight kept as the regression baseline — `test_selfcheck_on_real_flight_fixture` must stay PASSED.

### Dashboard subtleties

- The WS push interval (`--ws-push-interval-ms`, default **2000ms**) matches the DJI dock OSD cadence. Pushing faster doesn't surface new data; it just burns CPU.
- `LiveState` (`dashboard/live_state.py`) does **incremental merge** on each OSD, not full replacement. DJI sometimes omits fields (e.g., `flighttask_step_code` is only sent on state change) — replacing would null them mid-flight and the UI would flicker. Tests `test_partial_dock_osd_preserves_previous_fields` / `test_partial_drone_osd_preserves_previous_fields` pin this behavior.
- Dock/drone routing uses learned device_sn (`_known_dock_sn` / `_known_drone_sn`), set the first time an OSD with `sub_device` arrives. After that, route purely by topic's device_sn.
- HTML enum labels (`mode_code`, `flighttask_step_code`, `drone_in_dock`, `drc_state`) live in `static/index.html` as a JS `ENUM_MAP`. Adding new enums means editing both `LiveState._update_dock` (extract the field) and `ENUM_MAP` (label it). DJI Cloud API docs define the integer → meaning mapping.
- `static/index.html` loads Leaflet + Alpine.js from CDN. The browser needs public-internet access; the server itself is just an HTTP server.
- 右侧栏顶部可选内嵌 HTTP-FLV 视频（mpegts.js，vendored 于 `static/mpegts.min.js`，经 `/static` 挂载发布）；`--video-url` 缺省则隐藏，不影响现有页面。

### Config & flight-detection rules

`config.py` resolves `${env:VAR}` placeholders at load. Flight detection is driven by the dock OSD's `data.flighttask_step_code` (NOT the old `mode_code` rules — those were removed). Config keys: `flight_detection.record_steps` (default `[0,1,2]` = 作业准备中/飞行作业中/作业后状态恢复) and `flight_detection.idle_debounce_seconds` (default 5). Record while the sticky last-known step ∈ `record_steps`; finalize the current task after it leaves the set (e.g. 5 任务空闲) sustained `idle_debounce_seconds`. **Edge-sent caveat:** `flighttask_step_code` is only present in the OSD on a state change, so the detector keeps the last-known value and never treats an absent field as a state — otherwise it would wrongly finalize mid-flight.

## Testing conventions

- TDD red-then-green is the norm — every behavior fix went in as a failing test first. Reviewers will look for both the test and the source change in the same patch.
- Integration tests live under `tests/integration/` and use the session-scoped `mosquitto_broker` fixture in `tests/integration/conftest.py`. They auto-skip if the `mosquitto` binary isn't in PATH — don't add unconditional integration assertions to unit files.
- `pyproject.toml` sets `asyncio_mode = "auto"` and a global 30s timeout. `pytest-timeout` is mandatory because gmqtt event loops can hang on broker misconfiguration.
- The 220s real-flight fixture (`tests/fixtures/real_flight_basic/`) is the canonical regression baseline. If a change makes its SelfCheck drift > 50ms, the change is wrong.

## Non-obvious operational notes

- `install.sh` uses `pip install -e ".[test]" --upgrade --upgrade-strategy eager` and then explicitly imports `fastapi/uvicorn/websockets/httpx` as a sanity check. This is intentional — `-e` alone doesn't resync dependencies on existing venvs, which has bitten us when pyproject grew new deps.
- Plain TCP (port 1883) vs TLS (port 8883) — the YAML `mqtt.tls` flag must match the actual broker. A mismatch surfaces as `ConnectionResetError` during handshake.
- The CLI `record` loop is **long-running and does NOT auto-exit**. Each 1s tick calls `rec._detector.tick(now_ms())`; on `FlightState.FINALIZING` it `finalize_and_close` + `reset_for_next_flight` and **keeps looping** for the next task (multi-task per process). Only Ctrl-C / SIGTERM / `stop-record` set `stop_event` to exit, and the `finally` finalizes any in-progress flight on the way out (`reason=manual_stop`). Video is default-off (`video.enabled: false`); `--video` opts in.
- `recordings/` and `*.yaml` are gitignored; only `*.yaml.example` is tracked.

## Reference documents

| File | Role |
|------|------|
| `../2026-05-18-sim-dji-cloud-design.md` | v1 design (data contract authority) |
| `../2026-05-19-sim-dji-cloud-design-revision-dock-drone.md` | dock/drone split rationale |
| `../2026-05-19-sim-dji-cloud-phase1-prp.md` | Phase 1 plan |
| `../2026-05-21-sim-dji-cloud-phase2-prp.md` | Phase 2 plan (Player + SelfCheck) |
| `../2026-05-22-sim-dji-cloud-phase2-ui-prp.md` | Phase 2 UI plan (dashboard) |
| `../2026-05-25-sim-dji-cloud-flight-area-overlay-design.md` | Flight-area XML + PNG overlay on dashboard map (design) |
| `../2026-05-25-sim-dji-cloud-recorder-video-design.md` | Recorder video: pull RTMP from SRS → video/main.mp4 (design) |
| `../2026-05-26-sim-dji-cloud-playback-video-design.md` | Playback video: play pushes video/main.mp4 → SRS RTMP (design) |
| `../2026-05-27-sim-dji-cloud-continuous-record-design.md` | Continuous record: dock flighttask_step-driven multi-task, no auto-exit, video default-off (design) |
| `../2026-05-27-sim-dji-cloud-dashboard-video-design.md` | Dashboard embedded video: mpegts.js HTTP-FLV player in sidebar (design) |
| `../2026-05-21-sim-dji-cloud-operations-manual.md` | **End-user operations manual — first stop for "how do I run X"** |
