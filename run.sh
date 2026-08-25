#!/usr/bin/env bash
# sim-dji 启动脚本：选模式即可，参数在下面"可编辑配置"区改。
# 长跑模式用 nohup 脱离终端后台启动（终端关闭/崩溃也不停），日志写到 ./logs/。
#
# 用法:
#   ./run.sh <模式>          后台启动一个模式
#   ./run.sh stop <模式>     优雅停止该模式（SIGTERM → finalize + 停 ffmpeg）
#   ./run.sh status          查看各模式运行状态
#   ./run.sh logs <模式>     tail -f 该模式最新日志
#   ./run.sh list            列出所有飞行（前台直接输出）
set -euo pipefail
cd "$(dirname "$0")"
[ -f .venv/bin/activate ] && source .venv/bin/activate

# ================= 可编辑配置（按机器/场景改这里） =================
CONFIG="recorder.yaml"
PILOT_CONFIG="pilot_recorder.yaml"

# 回放视频推流目标（play-video 用；录制端 SRS 地址在 recorder.yaml 的 source_url_override）
SRS_PUSH_URL="rtmp://172.16.8.89/live/livestream"

# dashboard 多飞行切换时默认带的视频推流 URL（一般跟 SRS_PUSH_URL 同）
# 留空 = 切飞行只回放数据不推视频
DEFAULT_VIDEO_PUSH_URL="$SRS_PUSH_URL"

# dashboard 飞行区域叠加
DASH_PORT="8080"
AREA_XML="东方理工新区_2026-04-24_V0.0.1.xml"
AREA_PNG="东方理工新区背景.png"
AREA_BOUNDS_LATLON="29.919465,121.655345,29.928699,121.675011"   # 留空则用 XML 默认 / sidecar

# dashboard 内嵌视频（SRS HTTP-FLV 地址；留空则 dashboard 不带视频）
VIDEO_URL="http://172.16.8.89:8080/live/livestream.flv"

# dashboard WS 推送间隔毫秒；OSD 源头是 2s/帧，但推快点能减小"轨迹比视频慢"的观感
# （留空则用 sim-dji dashboard 的默认 2000）
DASH_WS_INTERVAL="200"

# events timeline 的 flight_dir 根（dashboard offline 模式读这里）
RECORDINGS_ROOT="./recordings"

# HTTP 控制 play 的 token（dashboard 启动时 export 这个 → 启用 POST API）
# 留空 = 不启用 HTTP 控制（POST 端点返 503，只读 dashboard 正常）
# 生成方式: openssl rand -hex 16
# 注意：run.sh 是本机配置（已 gitignore），直接写死 token 即可；
#      如果想从 shell 环境继承，改成 DASHBOARD_TOKEN="${DASHBOARD_TOKEN:-}"
DASHBOARD_TOKEN="03c0e6a194460b4817471bd5460623cb"

# 回放 / 自检
PLAY_DIR="recordings/<flight_dir>"
PLAY_MQTT="tcp://localhost:1883"
PLAY_SPEED="1.0"
# play-video 视频锚偏移（ms，可正可负）：负数让视频比 first-frame 时刻
# 提前 |N| ms 开始推，用来补偿 RTMP push → SRS GOP cache → mpegts.js
# jitter buffer 这段不可避免的管道延迟。建议：先用 0 跑一次看视频比
# 轨迹慢多少秒（比如慢 4s），就把这里设成 -4000；再细调到视觉同步。
# 留空 / "0" 则不偏移。
VIDEO_ANCHOR_OFFSET_MS="-4000"
SELFCHECK_DIR="recordings/<flight_dir>"
SELFCHECK_TOL_MS="200"
# ===================================================================

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# 后台启动：nohup 脱离 SIGHUP + 重定向日志 + 脱离终端 stdin；记录 pid 与 latest 软链。
launch() {
  local name="$1"; shift
  local pidf="$LOG_DIR/${name}.pid"
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    echo "[run] $name 已在运行 (pid=$(cat "$pidf"))；要重启先：./run.sh stop $name"
    exit 1
  fi
  local ts log; ts="$(date +%Y%m%d-%H%M%S)"; log="$LOG_DIR/${name}-${ts}.log"
  nohup "$@" >"$log" 2>&1 < /dev/null &
  local pid=$!
  disown 2>/dev/null || true
  echo "$pid" > "$pidf"
  ln -sf "$(basename "$log")" "$LOG_DIR/${name}-latest.log"
  echo "[run] $name 已后台启动  pid=$pid"
  echo "[run] 日志: $log"
  echo "[run] 看日志: ./run.sh logs $name   停止: ./run.sh stop $name"
}

stop_mode() {
  local name="$1"; local pidf="$LOG_DIR/${name}.pid"
  [ -f "$pidf" ] || { echo "[run] $name 未在运行（无 pid 文件）"; return 0; }
  local pid; pid="$(cat "$pidf")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"            # SIGTERM → record/play 优雅 finalize + 停 ffmpeg
    echo "[run] 已向 $name (pid=$pid) 发送停止信号（优雅收尾，最多等 30s）"
    # 等待优雅退出再删 pid 文件；否则 ./run.sh stop record && ./run.sh record
    # 之间会有两进程并发写同一 recordings/ → JSONL 损坏 + manifest 撕裂。
    local i
    for i in $(seq 1 60); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "[run] $name (pid=$pid) 30s 内未退出，发送 SIGKILL"
      kill -9 "$pid" 2>/dev/null || true
      sleep 0.5
    fi
  else
    echo "[run] $name 进程不存在 (pid=$pid)，清理 pid 文件"
  fi
  rm -f "$pidf"
}

status() {
  printf "%-16s %-8s %s\n" "模式" "PID" "状态"
  shopt -s nullglob
  for pidf in "$LOG_DIR"/*.pid; do
    local name pid; name="$(basename "$pidf" .pid)"; pid="$(cat "$pidf")"
    if kill -0 "$pid" 2>/dev/null; then
      printf "%-16s %-8s %s\n" "$name" "$pid" "运行中"
    else
      printf "%-16s %-8s %s\n" "$name" "$pid" "已停止(残留 pid 文件)"
    fi
  done
}

usage() {
  cat <<EOF
用法: ./run.sh <模式|命令>

后台启动模式（日志在 ./logs/，终端关掉也不停；[] 内为可选临时参数，省略用配置默认）:
  record [配置]              连续录制（仅 MQTT，视频默认关）
  record-video [配置]        录制 + 主镜头视频（SRS 地址在 $CONFIG 的 source_url_override）
  record-pilot [配置]        Pilot-to-Cloud 录制（RC Plus 2 + M400，遥控器为网关，无视频）
  dashboard [端口]           可视化仪表盘 (默认 :$DASH_PORT)
  dashboard-area [端口]      仪表盘 + 飞行区域叠加（XML+PNG+经纬度校准）
  play [目录] [倍速]         回放到本地 broker
  play-video [目录] [倍速]   回放 + 视频推流到 SRS
  selfcheck [目录] [容差ms]  录-放对称性自检

管理命令:
  status          查看各模式运行状态
  stop <模式>     优雅停止某模式
  logs <模式>     tail -f 某模式最新日志
  list            列出所有飞行（前台输出）

例: ./run.sh play recordings/<flight>          # 临时换回放目录
    ./run.sh play recordings/<flight> 5.0      # 再换倍速
    ./run.sh dashboard 9000                    # 临时换端口
默认参数在脚本顶部"可编辑配置"区修改。
EOF
}

mode="${1:-}"
shift || true   # 其余位置参数作为该模式的临时覆盖；省略则用上面配置默认

case "$mode" in
  record)
    launch record sim-dji record --config "${1:-$CONFIG}" ;;
  record-video)
    launch record-video sim-dji record --config "${1:-$CONFIG}" --video ;;
  record-pilot)
    launch record-pilot sim-dji record-pilot --config "${1:-$PILOT_CONFIG}" ;;
  dashboard)
    vid_opts=()
    [ -n "$VIDEO_URL" ] && vid_opts=(--video-url "$VIDEO_URL")
    [ -n "${DASH_WS_INTERVAL:-}" ] && vid_opts+=(--ws-push-interval-ms "$DASH_WS_INTERVAL")
    vid_opts+=(--recordings-root "$RECORDINGS_ROOT" --log-dir "$LOG_DIR")
    [ -n "$DASHBOARD_TOKEN" ] && export DASHBOARD_TOKEN
    [ -n "$DEFAULT_VIDEO_PUSH_URL" ] && vid_opts+=(--default-video-push-url "$DEFAULT_VIDEO_PUSH_URL")
    launch dashboard sim-dji dashboard --port "${1:-$DASH_PORT}" "${vid_opts[@]}" ;;
  dashboard-area)
    port="${1:-$DASH_PORT}"
    area_opts=(--flight-area-xml "$AREA_XML" --flight-area-png "$AREA_PNG")
    [ -n "$AREA_BOUNDS_LATLON" ] && area_opts+=(--flight-area-png-bounds-latlon "$AREA_BOUNDS_LATLON")
    [ -n "$VIDEO_URL" ] && area_opts+=(--video-url "$VIDEO_URL")
    [ -n "${DASH_WS_INTERVAL:-}" ] && area_opts+=(--ws-push-interval-ms "$DASH_WS_INTERVAL")
    area_opts+=(--recordings-root "$RECORDINGS_ROOT" --log-dir "$LOG_DIR")
    [ -n "$DASHBOARD_TOKEN" ] && export DASHBOARD_TOKEN
    [ -n "$DEFAULT_VIDEO_PUSH_URL" ] && area_opts+=(--default-video-push-url "$DEFAULT_VIDEO_PUSH_URL")
    launch dashboard-area sim-dji dashboard --port "$port" "${area_opts[@]}" ;;
  play)
    launch play sim-dji play "${1:-$PLAY_DIR}" --mqtt-url "$PLAY_MQTT" --speed "${2:-$PLAY_SPEED}" ;;
  play-video)
    pv_opts=(--mqtt-url "$PLAY_MQTT" --speed "${2:-$PLAY_SPEED}"
             --video-push-url "$SRS_PUSH_URL")
    [ -n "${VIDEO_ANCHOR_OFFSET_MS:-}" ] && pv_opts+=(--video-anchor-offset-ms "$VIDEO_ANCHOR_OFFSET_MS")
    launch play-video sim-dji play "${1:-$PLAY_DIR}" "${pv_opts[@]}" ;;
  selfcheck)
    launch selfcheck sim-dji selfcheck "${1:-$SELFCHECK_DIR}" --tolerance-ms "${2:-$SELFCHECK_TOL_MS}" ;;
  list)
    sim-dji list --root recordings ;;
  status)
    status ;;
  stop)
    stop_mode "${1:?用法: ./run.sh stop <模式>}" ;;
  logs)
    tail -f "$LOG_DIR/${1:?用法: ./run.sh logs <模式>}-latest.log" ;;
  ""|-h|--help|help)
    usage ;;
  *)
    echo "未知模式: $mode"; echo; usage; exit 1 ;;
esac
