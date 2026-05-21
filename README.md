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
