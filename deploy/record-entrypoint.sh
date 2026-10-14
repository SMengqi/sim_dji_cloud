#!/bin/sh
# 容器内 record / record-pilot 启动包装。
#
#   sim-dji-record [配置文件] [sim-dji record 的其它参数...]
#
# 配置文件：相对名按 /config/<name> 解析（/config 是宿主机项目目录的只读挂载），
#           绝对路径原样使用；不传则用 $RECORDER_CONFIG（默认 recorder.yaml）。
# 例：
#   docker compose up -d                                   # /config/recorder.yaml
#   docker compose run --rm record recorder_125.yaml       # 指定另一份配置
#   docker compose run --rm record recorder_125.yaml --no-video
set -eu

cmd="${RECORD_CMD:-record}"                     # record | record-pilot
storage_root="${STORAGE_ROOT:-/data/recordings}"
cfg="${RECORDER_CONFIG:-recorder.yaml}"

# 第一个参数不以 - 开头就当作配置文件
if [ "$#" -gt 0 ]; then
  case "$1" in
    -*) ;;
    *)  cfg="$1"; shift ;;
  esac
fi
case "$cfg" in
  /*) ;;
  *)  cfg="/config/$cfg" ;;
esac

if [ ! -f "$cfg" ]; then
  echo "[sim-dji-record] 配置文件不存在: $cfg（/config 对应宿主机项目目录，请检查文件名）" >&2
  exit 2
fi

echo "[sim-dji-record] sim-dji $cmd --config $cfg --storage-root $storage_root $*"
exec sim-dji "$cmd" \
  --config "$cfg" \
  --storage-root "$storage_root" \
  --state-dir /data/logs/state \
  "$@"
