# sim-dji-cloud Docker 部署手册

**适用场景**：生产 / 测试机上用 Docker Compose 跑 `record`、`dashboard`（以及按需的 `record-pilot` / `play` / 工具命令），
数据和视频落在宿主机目录。镜像在部署机本地 `docker compose build`，不需要 registry，开发机不参与打包。

> 要把镜像打成 tar 拷到**另一台**全新机器部署（新机无源码 / 无外网），看 [`docker-offline-deploy.md`](docker-offline-deploy.md)：`bash deploy/package.sh <tag>` 一条命令产出镜像 tar + 部署包。

相关文件（都在仓库里）：

| 文件 | 作用 |
|---|---|
| `Dockerfile` | `python:3.12-slim` + ffmpeg/tzdata + `pip install .`；入口 `sim-dji` |
| `.dockerignore` | 只把 `pyproject.toml` / `src/` / `deploy/` 送进构建上下文（recordings、.venv、yaml 等不进镜像） |
| `docker-compose.yml` | 服务定义：`record`（默认，`docker compose up -d` 只起它）、`dashboard` `record-pilot` `tools` `mosquitto`（profile，按需起） |
| `.env.example` | 所有可调变量的模板；`cp .env.example .env` 后填写 |
| `deploy/record-entrypoint.sh` | 容器内 record / record-pilot 包装：`sim-dji-record [配置文件名] [其它参数]`，配置按 `/config/<名>` 解析 |
| `deploy/dashboard-entrypoint.sh` | 容器内按环境变量拼 `sim-dji dashboard` 参数（对应 run.sh 的 dashboard / dashboard-area） |
| `deploy/mosquitto.conf` | 可选本地 broker 配置 |
| `deploy/area/` | 飞行区域叠加文件（XML / PNG / `.bounds`）放这里，容器内挂到 `/area` |

---

## 1. 设计要点（看懂这几条就能排障）

- **host 网络**：所有服务 `network_mode: host`，和原来 `run.sh` 直跑一样：record 出站连 DJI 云 broker 与局域网 SRS；dashboard / play 连宿主机 `127.0.0.1:1883`；dashboard 的 8080 直接对外，不做端口映射。
- **数据在宿主机**：`recordings/`、`recordings_pilot/`、`logs/` 以 bind mount 挂进容器（默认就是项目目录下现有的目录，零搬迁）。视频 `video/*.mp4` 在飞行目录里，一并落在宿主机。`tar` / `du` / 清老飞行都在宿主机直接做。
- **容器以宿主机用户身份运行**：`.env` 的 `PUID`/`PGID` 必须等于 `recordings/` 目录属主的 `id -u`/`id -g`，否则容器写不进去。
- **时区**：飞行目录名 `<dock_sn>_<YYYYMMDD-HHMMSS>` 用本地时间；容器 `TZ` 默认 `Asia/Shanghai`，与现有目录命名一致。
- **优雅停止**：`docker compose stop record` 发 SIGTERM，record 会 finalize 当前飞行并停 ffmpeg；`stop_grace_period` 给了 40s。不要 `kill -9`。
- **配置文件按文件名指定**：宿主机项目目录（`CONFIG_DIR`，默认 `.`）只读挂到容器 `/config`，所以项目里任意一份 yaml 都能用：`docker compose up -d` 用 `.env` 的 `RECORDER_CONFIG`（默认 `recorder.yaml`）；`docker compose run --rm record recorder_125.yaml` 临时换一份。yaml 里 `storage.root: ./recordings` 不用改，包装脚本会强制 `--storage-root /data/recordings`。凭据明文写在 yaml 里就不需要 `.env` 的 DJI 变量；写 `${env:...}` 引用时才填。
- **容器内路径固定**：`/data/recordings`、`/data/recordings_pilot`、`/data/logs`、`/config/<yaml>`。所有命令里写路径都用容器内路径。
- **互斥**：同一时间只能有一个进程写 `recordings/`。不要同时跑 compose 的 `record` 和 systemd / `run.sh record`；`docker compose run --rm record xxx.yaml` 之前也要先 `docker compose stop record`，否则两个录制并发写会撕裂 JSONL / manifest。

---

## 2. 首次部署

```bash
cd ~/projects/docker3/sim_dji_cloud        # 项目目录（recordings/ recorder.yaml 已在这里）
git pull

# 1) 变量文件
cp .env.example .env
vim .env
#   PUID / PGID  → 填 id -u / id -g 的输出（其余先保持默认即可）
#   RECORDER_CONFIG → `docker compose up -d` 默认用的 yaml 文件名（默认 recorder.yaml）
#   DJI_MQTT_USERNAME / DJI_MQTT_PASSWORD → 仅当 yaml 里写 ${env:...} 引用时才填
#   DASHBOARD_TOKEN / VIDEO_URL / DEFAULT_VIDEO_PUSH_URL → 起 dashboard 时才用

# 2) 录制配置（和原来一样，就是项目目录里的 recorder.yaml；视频开关在 video.enabled）
ls recorder.yaml || cp recorder.yaml.example recorder.yaml

# 3) 停掉旧的直跑进程（如有）
./run.sh status; ./run.sh stop record; ./run.sh stop dashboard
sudo systemctl stop sim-dji-record 2>/dev/null; sudo systemctl disable sim-dji-record 2>/dev/null

# 4) 构建镜像（在本机，几分钟）
docker compose build

# 5) 校验配置（容器内跑 validate-config）
docker compose run --rm tools validate-config --config /config/recorder.yaml

# 6) 启动录制（默认只起 record）
docker compose up -d
docker compose ps
docker compose logs -f record          # 看到 mqtt connected 即正常

# 7) 需要仪表盘时再起（profile）
docker compose --profile dashboard up -d dashboard     # 浏览器 http://<服务器IP>:8080
```

### 用另一份配置文件录制（一次性）

项目目录整体只读挂在容器 `/config`，所以直接把文件名当参数传：

```bash
docker compose stop record                                   # 先停常驻的，避免并发写 recordings/
docker compose run --rm record recorder_125.yaml             # 前台跑，Ctrl-C 优雅 finalize
docker compose run --rm -d record recorder_125.yaml          # 后台跑；停：docker stop <容器名>（见 docker ps）
docker compose run --rm record recorder_125.yaml --no-video  # 配置名后面可以跟 sim-dji record 的其它参数
docker compose run --rm record /config/sub/dir/x.yaml        # 绝对路径也行
```

要长期换默认配置：改 `.env` 的 `RECORDER_CONFIG=recorder_125.yaml`，然后 `docker compose up -d`。

---

## 3. 日常操作（run.sh 模式 → compose 命令对照）

| 原来 | 现在 |
|---|---|
| `./run.sh record` | `docker compose up -d`（默认只起 record） |
| `./run.sh record <别的配置>` | `docker compose run --rm record <文件名>.yaml`（先 `stop record`） |
| `./run.sh record-video` | yaml 里 `video.enabled: true`，然后 `docker compose up -d`（或 `restart record`） |
| `./run.sh record-pilot` | `docker compose --profile pilot up -d record-pilot` |
| `./run.sh dashboard` / `dashboard-area` | `docker compose --profile dashboard up -d dashboard`（叠加见 §5） |
| `./run.sh play <目录> [倍速]` | `docker compose run --rm tools play /data/recordings/<目录> --speed 1.0` |
| `./run.sh play-video <目录>` | 上一条再加 `--video-push-url rtmp://<srs>/live/livestream --video-anchor-offset-ms -4000` |
| `./run.sh selfcheck <目录>` | `docker compose run --rm tools selfcheck /data/recordings/<目录> --tolerance-ms 200` |
| `./run.sh list` | `docker compose run --rm tools list --root /data/recordings` |
| `sim-dji inspect <目录>` | `docker compose run --rm tools inspect /data/recordings/<目录>` |
| `sim-dji repair <目录> --force` | `docker compose run --rm tools repair /data/recordings/<目录> --force` |
| `./run.sh status` | `docker compose ps` |
| `./run.sh logs record` | `docker compose logs -f record` |
| `./run.sh stop record` | `docker compose stop record`（优雅 finalize） |
| `sim-dji stop-record <task_id>` | `docker compose exec record sim-dji stop-record <task_id> --state-dir /data/logs/state` |

dashboard 里"选飞行 → 回放"的按钮仍然可用：dashboard 容器内自带 `sim-dji`，会在容器内拉起 play 子进程，pid / 日志写在 `logs/`（宿主机可见）。

---

## 4. 更新代码

```bash
git pull
docker compose build                   # 只改了 src/ 时依赖层走缓存，很快
docker compose up -d                   # compose 检测到镜像变了会重建容器（record 先优雅停）
docker compose --profile dashboard up -d dashboard   # 有跑 dashboard 的话
```

改了 yaml：`docker compose restart record`（yaml 是挂载进去的，不用重建）。改了 `.env`：`docker compose up -d`。

---

## 5. 飞行区域叠加（dashboard-area）

```bash
cp 东方理工新区_2026-04-24_V0.0.1.xml 东方理工新区背景.png deploy/area/
# 如有校准产出的 sidecar：cp 东方理工新区背景.png.bounds deploy/area/
```

`.env`：

```
FLIGHT_AREA_XML=/area/东方理工新区_2026-04-24_V0.0.1.xml
FLIGHT_AREA_PNG=/area/东方理工新区背景.png
FLIGHT_AREA_BOUNDS_LATLON=29.919465,121.655345,29.928699,121.675011   # 有 .bounds sidecar 可留空
```

然后 `docker compose --profile dashboard up -d dashboard`。`deploy/area/` 里的大文件已被 `.gitignore` 的 XML/PNG 规则忽略，不会误提交。

---

## 6. 宿主机没有本地 broker 时

play / dashboard 需要一个本地 MQTT broker。宿主机已有 mosquitto 就什么都不用做；没有则：

```bash
docker compose --profile broker up -d mosquitto     # 只监听 127.0.0.1:1883，匿名
```

它只绑本机回环地址：容器都是 host 网络，dashboard / play 通过 `127.0.0.1` 就能连上。要让**另一台机器**上的被测系统订阅回放流，改 `deploy/mosquitto.conf` 为 `listener 1883 0.0.0.0`，并且把 `allow_anonymous` 关掉配 `password_file`，别把匿名 broker 开到局域网。

dashboard 的 8080 默认绑 `0.0.0.0`（浏览器要从别的机器访问），写操作由 `DASHBOARD_TOKEN` 守护、读是公开的，和 run.sh 直跑时一致；对外网段建议用防火墙限制来源。

---

## 7. 数据备份 / 清理

数据就在宿主机 `recordings/`，和以前完全一样：

```bash
du -sh recordings/* | sort -h | tail
tar czf /backup/flight_X.tar.gz recordings/<flight_dir>
rm -r recordings/<old_flight_dir>         # 录制进行中也可以删别的目录
```

容器日志（stdout）由 docker 轮转（每服务 5×50MB），用 `docker compose logs` 看；
dashboard 拉起的 play 子进程日志在 `logs/`。

---

## 8. 排障

| 现象 | 处理 |
|---|---|
| `Permission denied` 写 recordings | `.env` 的 `PUID/PGID` 与 `ls -ld recordings` 属主不一致；改完 `docker compose up -d` |
| 新飞行目录时间差 8 小时 | `.env` 的 `TZ` 不对（默认 `Asia/Shanghai`） |
| `env var not set: DJI_MQTT_USERNAME` | yaml 里写的是 `${env:...}` 引用但 `.env` 没填；填完 `docker compose up -d` |
| `[sim-dji-record] 配置文件不存在: /config/xxx.yaml` | 文件名拼错，或文件不在 `CONFIG_DIR`（默认项目目录）下；`ls *.yaml` 核对 |
| 两份录制同时在跑 | `docker ps` 看到 `sim-dji-record` 和 `sim-dji-cloud-record-run-*` 并存：立刻 `docker stop` 其中一个，之后 `sim-dji repair` 检查受影响的飞行目录 |
| dashboard 8080 起不来 | `ss -ltnp \| grep 8080` 看谁占着（多半是没停的 `run.sh dashboard`）；或改 `.env` 的 `DASH_PORT` |
| dashboard 一直 unhealthy | `docker compose logs dashboard`；host 网络下 healthcheck 探的是 `127.0.0.1:$DASH_PORT` |
| 回放没消息 / dashboard 空 | 宿主机 1883 没 broker：`ss -ltnp \| grep 1883`；没有就 §6 |
| 视频面板黑屏 / 录不到视频 | `docker compose logs record` 找 `ffmpeg exited`；检查 SRS 地址（容器 host 网络，地址和以前一样写） |
| 想进容器看看 | `docker compose exec record sh`（或 `docker compose run --rm --entrypoint sh tools`） |
| 改了 Dockerfile 想干净重建 | `docker compose build --no-cache` |

更多录制 / 回放本身的排障看 `../2026-05-21-sim-dji-cloud-operations-manual.md` §10。
