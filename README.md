# sim_dji_cloud — Phase 1 (Recorder MVP)

录制 DJI 机场 3 云 API（MQTT + RTMP），按 `2026-05-18-sim-dji-cloud-design.md` v1 §3 数据契约落盘成飞行目录，供后续离线分析与回放。

## 状态

阶段一交付：**Recorder + stop-record + inspect + validate-config**。
阶段二（Player / SelfCheck）、阶段三（UI）见父设计文档 §10.1。

## 安装

```bash
cd sim_dji_cloud
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
```

系统依赖：

- `ffmpeg` — 视频录制必需
- `mosquitto` — 仅运行集成测试需要，正常使用不需要

## 配置

```bash
cp recorder.yaml.example recorder.yaml
export DJI_MQTT_USERNAME=...
export DJI_MQTT_PASSWORD=...
# 在 recorder.yaml 把 mqtt.dock_sn 改成真实机场 SN
sim-dji validate-config --config recorder.yaml
```

## 录制

```bash
sim-dji record --config recorder.yaml
```

在另一个终端按 task_id 手动终止：

```bash
sim-dji stop-record T-2026-001
```

或者直接 Ctrl-C，会触发优雅 finalize。

产出目录：`./recordings/<task_id>__<dock_sn>__<YYYYMMDD-HHMMSS>/`

```
<flight_dir>/
├── manifest.json
├── topics/
│   ├── thing__product__<dock_sn>__osd.0001.jsonl
│   ├── thing__product__<drone_sn>__osd.0001.jsonl
│   ├── thing__product__<dock_sn>__services.0001.jsonl
│   ├── thing__product__<dock_sn>__drc__up.0001.jsonl
│   └── ...
└── video/
    ├── main.mp4
    └── main.timing.json
```

### 主镜头视频（SRS RTMP）

机场把主镜头推流到 **SRS**，recorder 飞行期间从 SRS 拉流转写到 `video/main.mp4`（ffmpeg `-c copy`）。在 `recorder.yaml` 配置真实 SRS 拉流地址：

```yaml
video:
  enabled: true
  source_url_override: "rtmp://<srs主机>/live/<stream>"   # v1：填真实 SRS 拉流地址
```

`sim-dji record` 即自动录制；Ctrl-C / `stop-record` / 自动结束都会优雅停 ffmpeg 并回填 `manifest.video`。`--no-video`（或 `video.enabled=false`）关闭，不建 `video/`。需要 `ffmpeg` 在 PATH。详见操作手册 §3.5。

## 检查录制

```bash
sim-dji inspect ./recordings/T-2026-001__SN_DOCK_001__20260520-093015/
```

输出 manifest 摘要：task_id、SN、时长、topic 列表（标注 dock/drone）、gaps、视频信息。

## 测试

```bash
pytest tests/unit/ -v                  # 单元测试，无外部依赖
pytest tests/integration/ -v           # 集成测试；mosquitto 未安装时 skip
```

## 真机首次录制清单

阶段一收尾时按这个清单做一次真实飞行的回收：

- [ ] DJI 云账号已申请，能拿到 MQTT 凭据 + TLS 证书（如需）
- [ ] 已在 `recorder.yaml` 填入 `mqtt.dock_sn` = 真实机场 SN
- [ ] `sim-dji validate-config --config recorder.yaml` 退出码 0
- [ ] 测试一次空跑：录制运行 30 秒，能看到至少 `dock_osd` 落盘
- [ ] 安排一次完整飞行（>= 5 分钟，含起飞/航点/降落）
- [ ] 飞行结束后 `sim-dji inspect <flight_dir>` 输出符合预期（两路 osd / DRC / video 都有数据）
- [ ] 把 `recordings/<flight>/` 整个目录打包带回离线分析

## 项目结构

```
sim_dji_cloud/
├── pyproject.toml
├── recorder.yaml.example
├── README.md
├── src/sim_dji_cloud/
│   ├── cli.py                      # click 入口
│   ├── config.py                   # YAML 加载 + ${env:VAR}
│   ├── logging_setup.py            # loguru 配置
│   ├── utils/time_ms.py
│   ├── storage/
│   │   ├── jsonl.py                # JsonlWriter / JsonlReader
│   │   ├── rotation.py             # RotatingJsonlWriter（分卷）
│   │   └── manifest.py             # ManifestBuilder + validator
│   ├── recorder/
│   │   ├── __init__.py             # Recorder 顶层编排
│   │   ├── topic_router.py         # topic → (device_sn, source)
│   │   ├── write_queue.py          # 每 topic 一个 asyncio queue
│   │   ├── mqtt_client.py          # gmqtt 封装 + 断连 gap 跟踪
│   │   ├── flight_detector.py      # 任务边界规则引擎
│   │   ├── video_writer.py         # ffmpeg 子进程
│   │   └── stop_signal.py          # 进程间停止信号
│   └── tools/
│       ├── inspect_cmd.py
│       ├── stop_record_cmd.py
│       └── validate_config_cmd.py
└── tests/
    ├── unit/                       # 单元测试
    ├── integration/                # mosquitto 集成测试
    └── fixtures/recorder_minimal.yaml
```

## Phase 2: Player + SelfCheck

阶段二交付：**`sim-dji play` + `sim-dji selfcheck` + `sim-dji list` + `sim-dji repair`**。
阶段三（UI / 视频回放）见父设计文档。

### 回放

```bash
sim-dji play ./recordings/<flight_dir>/ \
    --mqtt-url tcp://localhost:1883 --speed 1.0
```

把飞行目录的所有 topic 按原时序重发到指定 broker。被测业务系统作为订阅端连同一 broker 即可复现飞行。

回放时还可把主镜头视频推回 SRS（与数据同步），被测系统/播放器从 SRS 拉流即可看到"实时"画面：

```bash
sim-dji play ./recordings/<flight_dir>/ --mqtt-url tcp://localhost:1883 --speed 1.0 \
  --video-push-url "rtmp://<srs>/live/<stream>"
```

仅 `--speed 1.0` 生效；倍速/无视频/失败都跳过且不影响 MQTT 回放。详见操作手册 §5.2.1。

### 自检（录-放对称性回归）

```bash
sim-dji selfcheck ./recordings/<flight_dir>/ --tolerance-ms 50
```

In-process loopback：Player 发 → 临时 Recorder 收 → Comparator 比对。
退出码 0 = PASS / 非 0 = FAIL。报告写到 `<flight_dir>/selfcheck/<ts>/`。

### 列出 + 修复

```bash
sim-dji list --root ./recordings              # 列出所有飞行
sim-dji repair ./recordings/<flight_dir>/     # manifest 丢失/损坏时重建
```

### 真机 fixture

`tests/fixtures/real_flight_basic/` 是 220s 完整飞行的脱敏版本，作为 SelfCheck 回归基线：

```
test_selfcheck_on_real_flight_fixture PASSED
```

要重新生成（基于新真机数据）：

```bash
python3 scripts/anonymize_flight.py recordings/<flight_dir>/ tests/fixtures/real_flight_basic/
```

---

## Dashboard 可视化（阶段二 UI 补丁）

无图形界面 Linux 服务器友好——纯 HTTP，浏览器看：

```bash
# 在录制或回放的同台服务器上
sim-dji dashboard --port 8080
# 浏览器访问 http://<server-ip>:8080
```

订阅本地 broker（与 record/play 同一个），实时显示：

- 飞行器位置 + 轨迹（Leaflet 地图）
- 机场状态（温度、湿度、舱盖、入坞、网络）
- 飞行器状态（mode_code、电池、高度、速度、GPS/RTK、姿态）
- 最近 20 条 events
- 各 topic 实时消息计数

SSH 隧道（不开外网端口）：

```bash
ssh -L 8080:localhost:8080 <user>@<server>
# 浏览器访问 http://localhost:8080
```

可叠加站点**飞行区域**（XML 限制区/作业区多边形）+ PNG 离线底图，并用 `?calibrate=1` 实时对位校准：

```bash
sim-dji dashboard --port 8080 \
  --flight-area-xml <area.xml> --flight-area-png <bg.png>
```

详见操作手册 §6.5 / §6.6。

右侧栏可内嵌 SRS 主镜头 HTTP-FLV 实时画面（mpegts.js，库随包分发无需外网）：

```bash
sim-dji dashboard --port 8080 --video-url http://<srs>:8080/live/livestream.flv
```

详见操作手册 §6.5「仪表盘内嵌视频」。

---

## 📖 完整操作手册

所有 8 个 CLI 子命令的详细用法、配置说明、完整工作流、故障排查矩阵、数据契约速查见：

**[../2026-05-21-sim-dji-cloud-operations-manual.md](../2026-05-21-sim-dji-cloud-operations-manual.md)**

涵盖章节：

- 0. 快速速查表（8 个命令一览）
- 1-2. 安装 + 配置
- 3. 录制（启停三种方式 / 实时验证）
- 4. 检查（inspect / list / repair）
- 5. 回放（broker 启动 / play 选项 / 验证）
- 6. 自检（in-process loopback / 报告解读 / 失败排查）
- 7. 真机 fixture 脱敏复用
- 8. 跑测试
- 9. 完整工作流示例
- 10. 故障排查（录制 / 回放 / 自检 三大类常见现象）
- 11-12. 文件结构 + 数据契约速查
- 13-15. 阶段状态 + 诊断顺序 + 参考文档

## 设计文档与实施计划

| 文件 | 内容 |
|------|------|
| `../2026-05-18-sim-dji-cloud-design.md` | v1 设计文档（数据契约权威） |
| `../2026-05-19-sim-dji-cloud-design-revision-dock-drone.md` | dock/drone 数据源拆分修订记录 |
| `../2026-05-19-sim-dji-cloud-phase1-prp.md` | 阶段一实施计划（Recorder MVP） |
| `../2026-05-21-sim-dji-cloud-phase2-prp.md` | 阶段二实施计划（Player + SelfCheck） |
| `../2026-05-22-sim-dji-cloud-phase2-ui-prp.md` | 阶段二 UI 实施计划（dashboard） |
| `../2026-05-25-sim-dji-cloud-flight-area-overlay-design.md` | 飞行区域叠加设计 |
| `../2026-05-25-sim-dji-cloud-flight-area-overlay-prp.md` | 飞行区域叠加实施计划 |
| `../2026-05-25-sim-dji-cloud-recorder-video-design.md` | 录制端视频（SRS RTMP）设计 |
| `../2026-05-25-sim-dji-cloud-recorder-video-prp.md` | 录制端视频实施计划 |
| `../2026-05-26-sim-dji-cloud-playback-video-design.md` | 回放端视频推流（play→SRS）设计 |
| `../2026-05-26-sim-dji-cloud-playback-video-prp.md` | 回放端视频推流实施计划 |
| `../2026-05-21-sim-dji-cloud-operations-manual.md` | **操作手册（常用入口）** |

