# sim-dji-cloud 生产镜像
# 在部署机上构建：docker compose build   （见 docs/docker-deploy.md）
#
# 分两步 pip：先只按 pyproject 装依赖（改代码不会重装依赖），再装本包本身。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai

# 构建期软件源（国内 / 内网机器连不上 deb.debian.org / pypi.org 时用，见 .env.example）：
#   APT_MIRROR     留空 = 官方 deb.debian.org；例：mirrors.aliyun.com / mirrors.tuna.tsinghua.edu.cn
#   PIP_INDEX_URL  例：https://mirrors.aliyun.com/pypi/simple/ 或 https://pypi.tuna.tsinghua.edu.cn/simple
ARG APT_MIRROR=""
ARG PIP_INDEX_URL="https://pypi.org/simple"

# ffmpeg/ffprobe：录视频 (record --video) + 回放推流 (play --video-push-url) 必需
# tzdata：飞行目录名用本地时间 (time.localtime)，容器必须有时区数据
RUN if [ -n "$APT_MIRROR" ]; then \
      sed -i "s|deb.debian.org|$APT_MIRROR|g" /etc/apt/sources.list.d/debian.sources 2>/dev/null \
      || sed -i "s|deb.debian.org|$APT_MIRROR|g" /etc/apt/sources.list; \
    fi \
 && apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" \
 | pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -r /dev/stdin

COPY src ./src
RUN pip install --no-cache-dir --no-deps --index-url "$PIP_INDEX_URL" .

# 启动包装：
#   sim-dji-record    [配置文件] [...]   record / record-pilot，配置按 /config/<name> 解析
#   sim-dji-dashboard                    按环境变量拼 dashboard 参数（对应 run.sh 的 dashboard / dashboard-area）
COPY deploy/record-entrypoint.sh    /usr/local/bin/sim-dji-record
COPY deploy/dashboard-entrypoint.sh /usr/local/bin/sim-dji-dashboard
RUN chmod 0755 /usr/local/bin/sim-dji-record /usr/local/bin/sim-dji-dashboard

# 运行时工作目录；recordings / logs / config 由 compose 挂载进来
WORKDIR /data
RUN mkdir -p /data/recordings /data/recordings_pilot /data/logs && chmod 0777 /data /data/recordings /data/recordings_pilot /data/logs

ENTRYPOINT ["sim-dji"]
CMD ["--help"]
