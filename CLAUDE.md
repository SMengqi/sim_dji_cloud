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

`Recorder` (`src/sim_dji_cloud/recorder/__init__.py`) is the orchestrator. Each topic gets its own `TopicWriteQueue` (`write_queue.py`) backed by a `RotatingJsonlWriter` (`storage/rotation.py`); records are buffered and flushed on `flush_max_records` OR `flush_interval_ms` (whichever first — both matter, low-frequency topics depend on the time fallback). `FlightDetector` (`flight_detector.py`) is a **sticky `flighttask_step_code` state machine**: it tracks the dock's last-known `data.flighttask_step_code` (the field is sent only on state change, so absence ≠ a state — it keeps the last value), transitions `WAITING_TASK → RECORDING` when that value ∈ `record_steps` (default `{0,1,2}`) and `RECORDING → FINALIZING` after it leaves the set for `idle_debounce_seconds`. It names the flight directory `<dock_sn>_<ts>/` (`_<ms3>` suffix on rare collision; `task_id` is stored inside `manifest.json`) and triggers a synchronous rename of pending JSONL files (purely sync — never await mid-rename; a previous bug had concurrent gmqtt callbacks corrupting state during async drain). `reset()` returns it to `WAITING_TASK` for the next task (multi-task per process); `tick(now_ms)` advances the idle debounce when no messages arrive.

`MqttRecorderClient` (`recorder/mqtt_client.py`) wraps gmqtt and reports connection gaps into the manifest.

Storage contract (frozen, do not break casually):

```
<flight_dir>/
├── manifest.json                # topics[].files[] with offsets, gaps[], duration_ms, dock_sn, drone_sn
├── topics/
│   └── thing__product__<sn>__<suffix>.NNNN.jsonl    # `/` → `__`, volume-rotated
└── video/main_<epoch_ms>.mp4 + main.timing.json + main.ffmpeg.log
```

Each JSONL line: `{recv_ts_ms, dji_ts_ms, direction, topic, payload}`.

`VideoWriter` (`recorder/video_writer.py`) is **eager-launch + restart-on-fast-exit + first-frame-rename (with popen-fallback)**, no probe phase. On `start()` the supervisor thread `Popen`s ffmpeg immediately writing to a placeholder `video/main_<launch_ms>.mp4` (epoch ms at Popen) with `-rw_timeout 5000000` so an unreachable / no-publisher RTMP endpoint makes ffmpeg exit non-zero within ~5s. ffmpeg is also given `-progress pipe:1 -stats_period 0.1`; a daemon thread drains stdout, tracking BOTH the first and latest non-zero `out_time_us=…` values plus the wall ms when each arrived, then computes a CANDIDATE first-frame wall ms via `last_observed_at_ms − (last_us − first_us)/1000`. The candidate is then **sanity-checked**: it must lie in `[popen_wall_ms, first_observed_at_ms]` (lower = physical: first frame can't predate Popen; upper = observational: by the time we read the first `out_time_us` line, the first frame is already muxed). If the candidate passes, the file is **atomically renamed** to `main_<first_frame_wall_ms>.mp4` (POSIX rename — ffmpeg's open fd keeps writing through the inode) and `_segments[-1]["ffmpeg_start_wall_ms"]` is patched. If it fails, the placeholder popen-time filename and `ffmpeg_start_wall_ms == ffmpeg_popen_wall_ms` stay put and a `WARNING` is logged with full diagnostics (`first_us / last_us / duration_implied / candidate / popen`). The PTS-delta form is required because `-c copy` forwards source RTMP PTS into the progress stream directly; live encoders (DJI Dock 3 included) emit PTS that is their monotonic uptime, not 0-relative — the older `now_ms − out_time_us/1000` form silently assumed `first_frame_pts == 0` and produced wall times **days** before `popen_wall_ms` (2026-05-29 production log: 6.8 days early; regression pinned by `test_pts_not_starting_at_zero_uses_delta_against_first_observation`). The sanity check is required because some sources/ffmpeg versions report `out_time_us` that is non-monotonic at wall rate altogether — e.g. 2026-06-01 production log saw `first_us=5293000` → `last_us=93270000000` (a 25.9-hour jump) across only ~98s of wall time; `ffprobe` on the same mp4 showed actual frame PTS 0..92.981s (the muxer wrote sane PTS — only `-progress` was lying, likely reporting source DTS or a multi-stream synthetic time). Without the sanity check the formula gave wall = popen − 25.9 hr; with it we degrade to popen-time naming (= "保留开始拉流的时间戳" — exactly the user's original request); regression pinned by `test_implausible_back_calc_falls_back_to_popen`. Downstream tools rely on `frame_at_PTS_X.recv_ms == ffmpeg_start_wall_ms + X`; under sanity-rejection the anchor is RTMP-handshake-early (~1–5 s, consistent and player-correctable) instead of arbitrarily wrong. `ffmpeg_popen_wall_ms` is preserved in `timing.json` and exposed as `manifest.video.popen_at_recv_ms` (= "开始拉流" 时刻), unaffected by either path. On top of all that, `stop()` runs an **ffprobe-based authoritative override** (`_finalize_segment_via_ffprobe`): after ffmpeg exits we read the on-disk mp4's actual container duration via `ffprobe -show_entries format=duration` and compute `first_frame_wall_ms = exit_wall_ms − duration_ms`. ffprobe reads the muxer's real PTS so it sidesteps `-progress out_time_us` entirely; this is the authoritative answer when ffprobe is in PATH (sanity-checked the same way — rejection keeps the in-band anchor). The three-tier chain (popen-time → in-band PTS-delta → ffprobe override) is best-effort upgrade, never downgrade. If the run lasted < `success_min_seconds` (default **15s**, ≥ 3× rw_timeout) the supervisor treats it as a failed start, deletes the partial mp4 (under whichever name it currently has), pops the segment entry, and retries after `retry_interval_s` (default 2s). If ≥ 15s, supervisor treats it as completed and exits the loop. `main.ffmpeg.log` is opened in **append mode** every attempt with a `=== ffmpeg attempt at wall_ms=… ===` separator so failure traces accumulate. Why eager: the old probe-first design consumed ~5s of source data per probe (RTMP doesn't replay history to new subscribers).

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
- `EventsArchive` (`dashboard/events_archive.py`) 是与 `LiveState` 并列的内存层。`MqttSubscriber` 收到消息时先调 `state.update(...)` 再调 `archive.append(...)`。Archive 按 topic suffix 路由：`events` → kind="event"，`drc/down` / `services` → kind="control"，其它 (`osd` 等) 跳过。两 deque maxlen=5000 软上限 LRU 兜底；`LiveState.on_flight_idle` listener 在 `flighttask_step_code` 转 idle 时调 `archive.reset()`，跟 `_trail.clear()` 同步触发。Archive 内部 `threading.Lock` 保护 deque 操作，允许 asyncio loop（MqttSubscriber）与 FastAPI threadpool（API handler）跨线程访问。Offline 模式由 `read_archive_from_flight_dir(flight_dir)` 现读 JSONL 构造临时 archive；不缓存。REST `/api/timeline` 和 `/api/timeline/export.csv` 由 `_timeline_router` 挂；`archive=None` 时整段路由不挂（FastAPI 自然 404），保持旧测试 / selfcheck 零接触。`--recordings-root` 选项决定 offline 模式从哪个目录里找 flight_dir。
- `PlayController` (`dashboard/play_controller.py`) 是 dashboard 起 / 停 / 查 `sim-dji play` 子进程的封装。`subprocess.Popen` + `start_new_session=True` 把 play 脱离 dashboard 进程（dashboard 重启不杀 play）。pid 文件 + meta json 写在 `<log_dir>/` 跟 `run.sh launch()` 共享同一份 —— `./run.sh stop play` 和 `POST /api/play/stop` 操作同一把锁，双向互操作。`status()` 探活后自动清残留 pid（进程崩了把 pid 文件清掉）。`create_app(play_controller=None)` 时整套 `/api/play/*` 路由不挂；selfcheck / 老测试零接触。`require_token` (`dashboard/auth.py`) 是 secure-by-default 鉴权：`DASHBOARD_TOKEN` 环境变量未设 / 空 → POST 全 503；设了 → POST 需要 `Authorization: Bearer <token>`，`secrets.compare_digest` 比对。GET `/api/play/status` 公开。CLI 选项 `--log-dir` 控 PlayController 的 pid / log 目录（默认 `./logs`，跟 run.sh 同）。
- `LiveState.reset()` 是 dashboard 整体状态复位入口：清 dock / drone / events / controls / trail / topic_counts / known_*_sn 全部，触发所有 on_flight_idle listener。`_update_dock` 检到 `flighttask_step_code` 转 idle 时调 `self.reset()` 后会立即重写 `_known_dock_sn = sn`（防止下一条无 sub_device 的 OSD 误路由到 _update_drone）。HTTP `POST /api/state/reset` 写 API（需 token）触发同一逻辑 + `EventsArchive.reset()`。`GET /api/flights` 公开端点扫 `recordings_root` 下所有子目录、读 manifest 提取 5 字段 (id/started_at_ms/duration_ms/has_video/dock_sn) 按 started_at_ms 倒序返；缺 manifest / 损坏 / 非目录 / 隐藏目录都跳过 + warning。CLI `--default-video-push-url` 通过 `/` 路由 string-replace 注入 HTML `<meta name="default-video-push-url" content="...">`，前端切飞行时读这个 meta 决定是否带 video_push_url。前端 `index.html` 顶部下拉自动加载 `/api/flights`，选定后编排 stop → state reset → start 三步，URL 自动同步到 `?flight=<id>` 支持深链。
- `VirtTimeScheduler.pause/resume/set_virt` + `paused_event` 是阶段三 A pause-seek 的内核：pause 记录 `paused_at_wall` + clear event 让 `wait_until_virt` asyncio 卡住；resume 把暂停期间的 wall 时长补到 `_play_start_wall` 上避免 virt "跳"；set_virt 重置 zero + 锚定新 wall_start，paused 状态不变（同步把 _paused_at_wall 锚到新 wall_start 让 virt_now_ms 返回 target）。`Player.pause/resume/seek/progress` + 内嵌 `aiohttp` `ControlServer`（`player/control_server.py`，绑 127.0.0.1:0 拿系统端口，写 `<log_dir>/<name>.control.json` sidecar）暴露 4 端点给 dashboard `PlayController` 转发。CLI `sim-dji play --control-sidecar-path PATH` 启用 control server；不传则不起（selfcheck / 直跑 CLI 零接触）。`PlayController.pause/resume/seek` 读 sidecar 拿端口 + httpx forward；`start_progress_polling()` 1Hz 后台 task 缓存 `_last_progress` 到 status 响应的 `progress: {virt_ms, total_ms, paused, stale}` 字段（dashboard_cmd uvicorn startup 钩起，`start()` 时清空旧缓存避免跨飞行串味）。`ControlUnavailable` 异常（sidecar 缺 / HTTP 失败）路由层翻 503；`already paused` / `not paused` 翻 409；NotRunning 翻 404；virt_ms 非法 400。前端 `index.html` timeline drawer handle 栏加 ⏸▶ 按钮 + virt/total 读数 + ⚠ stale 标志；canvas click 任意位置 seek；`drawTimeline()` 末尾画红色播放头（暂停变黄；用 rect.* CSS 像素，不用 canvas.* 物理像素，避开 HiDPI 双倍位置 bug）。Seek 后 dashboard state **不清**——跳过的消息在 timeline 自然形成 gap，已收到的轨迹 / events 是真实历史保留。Seek 实现 `_replay_topic_from_virt` linear scan 跳到 target recv_ts（v1 够，~50-100ms / 10k records），未来 N > 100k 时可接 SeekIndex 二分。seek 越界自动截断到 `total - 1` + warning log。`_replay_topic_from_virt` 不加 `_start_offset_ms`（scheduler.set_virt 已经把 virt_zero 移到 seek 点，再加一遍会让所有 record 多延 S/speed 秒）。

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
