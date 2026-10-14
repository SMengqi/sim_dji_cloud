# sim-dji-cloud 镜像构建 → 打包 tar → 新环境部署（完整指南）

**场景**：在有源码和 Docker 的"构建机"（测试环境）上构建镜像并导出成 tar，拷到一台全新的服务器（可能没有外网、没有源码）用 `docker load` + `docker compose` 跑起来。
日常在同一台机器上 `git pull` + `docker compose build` 的流程见 `docker-deploy.md`。

三个阶段：

| 阶段 | 在哪台机 | 产物 |
|---|---|---|
| A. 构建 + 打包 | 构建机（测试环境） | `sim-dji-cloud-image-<TAG>.tar.gz`、`sim-dji-cloud-deploy-<TAG>.tar.gz`、`SHA256SUMS` |
| B. 传输 | — | 三个文件拷到新机 |
| C. 新环境部署 | 新服务器 | 容器 `sim-dji-record` 常驻录制 |

---

## A. 构建机：构建镜像并打包

### A.1 前置

- Docker Engine ≥ 20.10 + Compose 插件 v2（`docker compose version` 能输出）。
- 构建机能访问 Docker Hub（拉 `python:3.12-slim`）和 Debian / PyPI 源。只在构建机需要外网，新环境不需要。
- **架构要一致**：在 x86_64 机器上 build 出的镜像只能在 x86_64 上跑；目标是 arm64 就在 arm64 机器上 build（或 `docker buildx build --platform linux/arm64`，较慢）。`uname -m` 两边对一下。

### A.2 一条命令

```bash
cd ~/projects/docker3/sim_dji_cloud
git pull
bash deploy/package.sh v0.1.0          # TAG 自定；缺省用 git describe
```

脚本做的事（也可以手动逐条执行，见 A.3）：

1. `IMAGE=sim-dji-cloud:v0.1.0 docker compose build record` 构建并打 tag
2. `docker save sim-dji-cloud:v0.1.0 | gzip > dist/sim-dji-cloud-image-v0.1.0.tar.gz`
3. 把 `docker-compose.yml`、`.env.example`（`IMAGE=` 已改成本次 tag）、`deploy/`、`docs/`、`*.yaml.example` 打成 `dist/sim-dji-cloud-deploy-v0.1.0.tar.gz`
4. `dist/SHA256SUMS`

产物在 `./dist/`（已 gitignore）。镜像 tar 约 400–500 MB（python slim + ffmpeg + 依赖）。

### A.3 手动等价命令（想自己控制时）

```bash
export IMAGE=sim-dji-cloud:v0.1.0
docker compose build record                      # 产出 $IMAGE
docker images sim-dji-cloud                      # 确认 tag 和大小
docker save "$IMAGE" | gzip > sim-dji-cloud-image-v0.1.0.tar.gz

mkdir -p bundle && cp docker-compose.yml .env.example recorder.yaml.example pilot_recorder.yaml.example bundle/
cp -r deploy docs bundle/
sed -i "s|^IMAGE=.*|IMAGE=$IMAGE|" bundle/.env.example
tar czf sim-dji-cloud-deploy-v0.1.0.tar.gz -C bundle .
sha256sum sim-dji-cloud-*.tar.gz > SHA256SUMS
```

### A.4 构建后在构建机上冒烟（可选但推荐）

```bash
docker run --rm sim-dji-cloud:v0.1.0 --help                                   # 列出 10 个子命令
docker run --rm --entrypoint ffmpeg sim-dji-cloud:v0.1.0 -version | head -1    # ffmpeg 在
docker run --rm --entrypoint sh sim-dji-cloud:v0.1.0 -c 'date; echo $TZ'       # 时区应是 CST / Asia/Shanghai
```

---

## B. 传输

```bash
# 构建机 → 新机
scp dist/sim-dji-cloud-image-v0.1.0.tar.gz dist/sim-dji-cloud-deploy-v0.1.0.tar.gz dist/SHA256SUMS user@new-host:/tmp/

# 新机上校验
cd /tmp && sha256sum -c SHA256SUMS          # 两行都应为 OK
```

U 盘 / 内网文件服务器同理，核对 `SHA256SUMS` 即可。

---

## C. 新环境部署

### C.1 前置

- Linux，Docker Engine ≥ 20.10，Compose 插件 v2。离线安装 Docker 的方法以各发行版文档为准（Ubuntu 可下载 `docker-ce` / `docker-ce-cli` / `containerd.io` / `docker-compose-plugin` 的 deb 离线装）。
- 当前用户能跑 `docker`（在 `docker` 组，或用 sudo）。
- **不需要** Python、ffmpeg、venv，这些都在镜像里。
- 只在要用 `play` / `dashboard` 回放时才需要本地 MQTT broker（宿主机 mosquitto 或 compose 的 `broker` profile）；纯录制不需要。
- 网络：新机要能连到 DJI 云 MQTT broker（你的 yaml 里的 `mqtt.host:port`）和 SRS 拉流地址（`video.source_url_override`）。容器是 host 网络，和宿主机 `curl` / `nc` 测通就行。
- 磁盘：录制数据写在宿主机目录，按飞行量预留（参考测试机 `recordings/` 的大小）。

### C.2 导入镜像

```bash
docker load -i /tmp/sim-dji-cloud-image-v0.1.0.tar.gz
docker images sim-dji-cloud                 # 应看到 sim-dji-cloud  v0.1.0
```

### C.3 展开部署目录

```bash
sudo mkdir -p /opt/sim-dji-cloud && sudo chown "$(id -u):$(id -g)" /opt/sim-dji-cloud
cd /opt/sim-dji-cloud
tar xzf /tmp/sim-dji-cloud-deploy-v0.1.0.tar.gz --strip-components=1
ls -a
#   docker-compose.yml  .env.example  deploy/  docs/  recorder.yaml.example  pilot_recorder.yaml.example  IMAGE_TAG
mkdir -p recordings recordings_pilot logs
```

目录放哪都行，关键是 `docker-compose.yml`、`.env`、`recorder.yaml`、`recordings/` 在同一层（compose 里的相对路径都以此为准）。

### C.4 配置

```bash
cp .env.example .env
vim .env
#   IMAGE=sim-dji-cloud:v0.1.0      ← 部署包里已填好，和 docker images 里的 tag 一致即可
#   PUID=<id -u>  PGID=<id -g>      ← 必填，等于 recordings/ 目录属主
#   TZ=Asia/Shanghai                ← 保持
#   其余留默认；DJI_MQTT_* 只在 yaml 用 ${env:...} 引用时才填

cp recorder.yaml.example recorder.yaml
vim recorder.yaml
#   mqtt.host / port / tls / client_id / dock_sn / username / password
#   video.enabled + video.source_url_override
#   storage.root 保持 ./recordings（容器内会被强制为 /data/recordings，即宿主机 ./recordings）
```

`client_id` 建议每台机不同（DJI broker 对重复 client_id 会互踢）。

### C.5 校验配置（不连 broker，只检查 yaml）

```bash
docker compose run --rm tools validate-config --config /config/recorder.yaml
```

退出码 0 才往下走。`tools` 是 profile 服务，`run` 可以直接用。

### C.6 启动

```bash
docker compose up -d                        # 默认只起 record
docker compose ps                           # STATUS 应为 Up
docker compose logs -f record               # 看到 mqtt connected / subscribed 即正常；Ctrl-C 只退出 logs，不停容器
```

`docker compose up` 发现本地已有 `IMAGE` 指定的镜像就直接用，不会尝试 build（部署目录里也没有源码）。**不要**加 `--build`。

### C.7 验证一次录制

飞一次（或等机场跑一次任务）后：

```bash
ls -lt recordings | head                                                # 新目录 <dock_sn>_<YYYYMMDD-HHMMSS>
docker compose run --rm tools inspect /data/recordings/<flight_dir>     # topics / 时长 / video
docker compose run --rm tools selfcheck /data/recordings/<flight_dir> --tolerance-ms 200
```

目录时间戳应是本地时间；差 8 小时就是 `.env` 的 `TZ` 没生效。

### C.8 设为开机自启

compose 里 record 是 `restart: unless-stopped`，只要 Docker 服务开机自启（`systemctl enable docker`，通常默认就是），宿主机重启后容器自动拉起。手动 `docker compose stop record` 过的容器不会自动起，需要再 `up -d`。

### C.9 日常操作

| 操作 | 命令 |
|---|---|
| 看状态 / 日志 | `docker compose ps` / `docker compose logs -f record` |
| 优雅停录制 | `docker compose stop record`（SIGTERM，finalize 当前飞行 + 停 ffmpeg，最多等 40s） |
| 再启动 | `docker compose up -d` |
| 改了 recorder.yaml | `docker compose restart record` |
| 临时用另一份配置 | `docker compose stop record && docker compose run --rm record other.yaml` |
| 列飞行 / 看摘要 | `docker compose run --rm tools list --root /data/recordings` / `... inspect /data/recordings/<dir>` |
| 回放 | `docker compose run --rm tools play /data/recordings/<dir> --speed 1.0`（需要本地 1883 broker） |
| 仪表盘 | `docker compose --profile dashboard up -d dashboard`，浏览器 `http://<ip>:8080` |
| 进容器排查 | `docker compose exec record sh` |

更多（Pilot 录制、飞行区域叠加、排障矩阵）见 `docs/docker-deploy.md`。

---

## D. 升级到新版本

构建机重新 `bash deploy/package.sh v0.1.1`，把新的 image tar（部署包只有文件变了才需要）拷过去：

```bash
docker load -i /tmp/sim-dji-cloud-image-v0.1.1.tar.gz
cd /opt/sim-dji-cloud
sed -i 's|^IMAGE=.*|IMAGE=sim-dji-cloud:v0.1.1|' .env
docker compose up -d                     # 检测到镜像变化 → 优雅停旧容器 → 起新容器
docker compose logs -f record
```

若 `docker-compose.yml` / `deploy/` 也有更新，先备份 `.env` 和 `recorder.yaml`，再 `tar xzf` 新部署包覆盖；`.env` / `recorder.yaml` 不在包里，不会被覆盖。

**回滚**：把 `.env` 的 `IMAGE` 改回旧 tag，`docker compose up -d`。旧镜像只要没 `docker rmi` 就还在。

**清理旧镜像**（确认回滚不再需要后）：

```bash
docker images sim-dji-cloud
docker rmi sim-dji-cloud:v0.1.0
```

---

## E. 常见问题

| 现象 | 原因 / 处理 |
|---|---|
| `docker load` 后 `docker images` 没有 | tar 传坏了：`sha256sum -c SHA256SUMS` |
| `exec format error` | 镜像架构和新机不一致（`uname -m`），回构建机用同架构机器重新 build |
| `up -d` 报 `pull access denied` / 尝试拉镜像 | `.env` 的 `IMAGE` 和 `docker images` 里的 tag 不一致 |
| `Permission denied` 写 recordings | `.env` 的 `PUID/PGID` ≠ `ls -ld recordings` 的属主 |
| `配置文件不存在: /config/recorder.yaml` | 没 `cp recorder.yaml.example recorder.yaml`，或不在 compose 同级目录 |
| 日志里一直 `reconnecting` | 新机到 DJI broker 网络不通 / 账号错 / `tls` 与端口不匹配（1883 明文，8883 TLS） |
| 录不到视频，`ffmpeg exited` | 新机到 SRS 地址不通：宿主机 `curl -I <flv地址>` 或 `nc -vz <srs> 1935` 先测 |
| 新目录时间差 8 小时 | `.env` 的 `TZ` |
