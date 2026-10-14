#!/bin/sh
# 容器内 dashboard 启动包装：按环境变量拼 `sim-dji dashboard` 参数。
# 对应 run.sh 的 dashboard / dashboard-area 两种模式；变量含义见 .env.example。
set -eu

set -- dashboard \
  --host "${DASH_HOST:-0.0.0.0}" \
  --port "${DASH_PORT:-8080}" \
  --mqtt-url "${MQTT_URL:-tcp://127.0.0.1:1883}" \
  --ws-push-interval-ms "${DASH_WS_INTERVAL:-2000}" \
  --recordings-root /data/recordings \
  --log-dir /data/logs

[ -n "${VIDEO_URL:-}" ]                 && set -- "$@" --video-url "$VIDEO_URL"
[ -n "${DEFAULT_VIDEO_PUSH_URL:-}" ]    && set -- "$@" --default-video-push-url "$DEFAULT_VIDEO_PUSH_URL"
[ -n "${FLIGHT_AREA_XML:-}" ]           && set -- "$@" --flight-area-xml "$FLIGHT_AREA_XML"
[ -n "${FLIGHT_AREA_PNG:-}" ]           && set -- "$@" --flight-area-png "$FLIGHT_AREA_PNG"
[ -n "${FLIGHT_AREA_BOUNDS_LATLON:-}" ] && set -- "$@" --flight-area-png-bounds-latlon "$FLIGHT_AREA_BOUNDS_LATLON"

# DASH_EXTRA_ARGS 故意不加引号：允许一次传多个额外参数
# shellcheck disable=SC2086
exec sim-dji "$@" ${DASH_EXTRA_ARGS:-}
