# sim-dji-cloud 生产镜像
# 在部署机上构建：docker compose build   （见 docs/docker-deploy.md）
#
# 分两步 pip：先只按 pyproject 装依赖（改代码不会重装依赖），再装本包本身。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai

# ffmpeg/ffprobe：录视频 (record --video) + 回放推流 (play --video-push-url) 必需
# tzdata：飞行目录名用本地时间 (time.localtime)，容器必须有时区数据
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" \
 | pip install --no-cache-dir -r /dev/stdin

COPY src ./src
RUN pip install --no-cache-dir --no-deps .

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
