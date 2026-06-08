#!/usr/bin/env bash
# sim_dji_cloud — 一键安装脚本（阶段一 Recorder + 阶段二 Player/SelfCheck + Dashboard UI）
# 用法：bash install.sh   或   ./install.sh
#
# 行为：
# 1) 检查 Python 3.10+ / ffmpeg / mosquitto
# 2) 创建 .venv（已存在则复用）
# 3) pip install -e ".[test]" --upgrade（关键：升级以同步新增依赖如 fastapi/uvicorn）
# 4) 验证 sim-dji CLI 可调用（9 个子命令）
# 5) 跑单元测试（集成测试需 mosquitto，跳过 OK）
# 6) 如存在 recorder.yaml，顺手 validate-config

set -euo pipefail

# ---------- color ----------
if [[ -t 1 ]]; then
    G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; B='\033[0;34m'; N='\033[0m'
else
    G=''; Y=''; R=''; B=''; N=''
fi
say()  { echo -e "${G}[install]${N} $1"; }
warn() { echo -e "${Y}[install]${N} $1"; }
fail() { echo -e "${R}[install]${N} $1"; exit 1; }
hr()   { echo -e "${B}----------------------------------------${N}"; }

cd "$(dirname "$0")"
PROJ_ROOT="$(pwd)"
say "项目根目录：$PROJ_ROOT"
hr

# ---------- 1. Python 版本 ----------
say "检查 Python ≥ 3.10 ..."
if ! command -v python3 >/dev/null 2>&1; then
    fail "python3 未安装；先：sudo apt install -y python3"
fi
PYVER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PYMAJOR=$(echo "$PYVER" | cut -d. -f1)
PYMINOR=$(echo "$PYVER" | cut -d. -f2)
if [[ $PYMAJOR -lt 3 || ($PYMAJOR -eq 3 && $PYMINOR -lt 10) ]]; then
    fail "Python 3.10+ 必需，当前 $PYVER。安装：sudo apt install -y python3.10 python3.10-venv"
fi
say "  Python $PYVER OK"

if ! python3 -c 'import venv' >/dev/null 2>&1; then
    fail "缺少 venv 模块。安装：sudo apt install -y python3-venv"
fi
hr

# ---------- 2. ffmpeg（视频录制必需，仅当 video.enabled=true 时）----------
say "检查 ffmpeg（视频录制需要；只录 MQTT 不需要）..."
if command -v ffmpeg >/dev/null 2>&1; then
    FFMPEG_VER=$(ffmpeg -version 2>&1 | head -1)
    say "  $FFMPEG_VER"
else
    warn "  ffmpeg 未安装。如不录视频可忽略；要录视频请：sudo apt install -y ffmpeg"
fi
hr

# ---------- 3. mosquitto（仅集成测试需要）----------
say "检查 mosquitto（仅 tests/integration/ 需要；正常使用不需要）..."
if command -v mosquitto >/dev/null 2>&1; then
    say "  mosquitto 已安装"
else
    warn "  mosquitto 未安装。集成测试会自动 skip。安装：sudo apt install -y mosquitto"
fi
hr

# ---------- 4. venv ----------
VENV_DIR="$PROJ_ROOT/.venv"
if [[ -d "$VENV_DIR" ]]; then
    say "venv 已存在：$VENV_DIR（复用）"
else
    say "创建 venv：$VENV_DIR"
    python3 -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
say "  已激活：$(which python3)"
hr

# ---------- 5. pip install ----------
say "升级 pip + 安装依赖（editable + test extras，--upgrade 以同步新增依赖）..."
python3 -m pip install --upgrade pip --quiet
python3 -m pip install -e ".[test]" --upgrade --upgrade-strategy eager --quiet
# 兜底：显式校验新增依赖在位（防止极老缓存 / 修改 pyproject 后未同步）
# pyproj：飞行区域叠加用，UTM→WGS84 坐标换算
MISSING=()
for mod in fastapi uvicorn websockets httpx aiohttp pyproj; do
    python3 -c "import $mod" 2>/dev/null || MISSING+=("$mod")
done
if [[ ${#MISSING[@]} -gt 0 ]]; then
    warn "  以下依赖未装好，强制重装：${MISSING[*]}"
    python3 -m pip install --force-reinstall "${MISSING[@]}" --quiet
fi
say "  依赖安装完成"
hr

# ---------- 6. 验证 CLI ----------
say "验证 sim-dji CLI ..."
if ! command -v sim-dji >/dev/null 2>&1; then
    fail "sim-dji 未在 PATH 中；pip install 可能失败，请回看上面输出"
fi
sim-dji --help >/dev/null 2>&1 || fail "sim-dji --help 执行失败"
say "  $(which sim-dji)"
say "  sim-dji 可用，子命令："
sim-dji --help 2>&1 | grep -E "^  (record|stop-record|inspect|validate-config|play|list|repair|selfcheck|dashboard)" | sed 's/^/    /'
hr

# ---------- 7. 单元测试 ----------
# Regression (review MAJOR): 旧写法 `if pytest ... | tail -3; then` 检的是
# tail 的退出码（永远 0），pytest 红了也"成功"。用 ${PIPESTATUS[0]} 拿 pytest
# 真实退出码。
say "跑单元测试（跳过集成测试，需要 mosquitto）..."
pytest tests/unit/ -q --tb=line 2>&1 | tail -3
pytest_rc="${PIPESTATUS[0]}"
if [[ "$pytest_rc" -eq 0 ]]; then
    say "  单元测试通过"
else
    warn "  单元测试有失败（退出码 $pytest_rc；看上面输出）"
fi
hr

# ---------- 8. 配置检查 ----------
if [[ -f "$PROJ_ROOT/recorder.yaml" ]]; then
    say "发现 recorder.yaml，校验配置 ..."
    if sim-dji validate-config --config "$PROJ_ROOT/recorder.yaml"; then
        say "  配置 OK"
    else
        warn "  配置校验失败，录制前请修正"
    fi
else
    warn "recorder.yaml 不存在；先：cp recorder.yaml.example recorder.yaml 并填入凭据"
fi
hr

# ---------- 9. 收尾提示 ----------
cat <<EOF

${G}=== 安装完成 ===${N}

下次使用：
  source .venv/bin/activate
  sim-dji --help

快速开始：
  cp recorder.yaml.example recorder.yaml
  vim recorder.yaml                          # 填 mqtt.dock_sn 和凭据
  sim-dji validate-config --config recorder.yaml
  sim-dji record --config recorder.yaml --no-video    # 只录 MQTT 不录视频

停止录制：
  另一个终端：sim-dji stop-record <task_id>
  或 Ctrl-C 触发优雅 finalize

检查录制结果：
  sim-dji inspect ./recordings/<flight_dir>/

回放：
  sim-dji play ./recordings/<flight_dir>/ --mqtt-url tcp://localhost:1883 --speed 1.0

自检（录-放对称性回归）：
  sim-dji selfcheck ./recordings/<flight_dir>/ --tolerance-ms 50

可视化仪表盘（headless 服务器友好，浏览器看）：
  sim-dji dashboard --port 8080
  # 浏览器访问 http://<server-ip>:8080
  # 或 SSH 隧道：ssh -L 8080:localhost:8080 <user>@<server>

EOF
