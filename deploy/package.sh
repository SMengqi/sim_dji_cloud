#!/usr/bin/env bash
# 在"构建机"（测试环境）上一条命令完成：构建镜像 → docker save 成 tar.gz → 部署文件打成离线包。
#
# 用法：
#   bash deploy/package.sh [TAG]        # TAG 缺省 = git describe（无 git 则 YYYYMMDD）
#   OUT_DIR=/some/dir bash deploy/package.sh v0.1.0
#
# 产出（默认 ./dist/）：
#   sim-dji-cloud-image-<TAG>.tar.gz    镜像（docker load 用）
#   sim-dji-cloud-deploy-<TAG>.tar.gz   部署包：docker-compose.yml / .env.example / deploy/ / docs/ / *.yaml.example
#   SHA256SUMS                          校验和
#
# 新环境部署步骤见 docs/docker-offline-deploy.md
set -euo pipefail
cd "$(dirname "$0")/.."

# 前置：能连上 Docker daemon（常见失败：当前用户不在 docker 组）
if ! docker info >/dev/null 2>&1; then
  cat >&2 <<'MSG'
[package] 连不上 Docker daemon（permission denied / daemon 未启动）。
  方法 1（推荐）：sudo usermod -aG docker "$USER" && newgrp docker   # 或退出重新登录
  方法 2：sudo -E bash deploy/package.sh <TAG>   # 之后 sudo chown -R "$(id -u):$(id -g)" dist
MSG
  exit 1
fi

TAG="${1:-$(git describe --tags --always --dirty 2>/dev/null || date +%Y%m%d)}"
IMAGE="sim-dji-cloud:${TAG}"
OUT_DIR="${OUT_DIR:-./dist}"
mkdir -p "$OUT_DIR"

echo "[package] 镜像: $IMAGE"
echo "[package] 架构: $(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')  ← 目标机必须同架构"

# 1) 构建（compose 用 IMAGE 变量给镜像打 tag；只 build record，其它服务共用同一镜像）
IMAGE="$IMAGE" docker compose build record

# 2) 镜像导出
IMG_TAR="$OUT_DIR/sim-dji-cloud-image-${TAG}.tar.gz"
echo "[package] docker save → $IMG_TAR"
docker save "$IMAGE" | gzip > "$IMG_TAR"

# 3) 部署包（新环境不需要源码，只要这些）
BUNDLE_NAME="sim-dji-cloud-deploy-${TAG}"
STAGE="$(mktemp -d)"
trap 'rm -r "$STAGE"' EXIT
mkdir -p "$STAGE/$BUNDLE_NAME"
cp docker-compose.yml .env.example recorder.yaml.example pilot_recorder.yaml.example "$STAGE/$BUNDLE_NAME/"
cp -r deploy docs "$STAGE/$BUNDLE_NAME/"
# 部署包里的 .env.example 直接指向这次打出来的镜像 tag
sed -i "s|^IMAGE=.*|IMAGE=${IMAGE}|" "$STAGE/$BUNDLE_NAME/.env.example"
printf '%s\n' "$IMAGE" > "$STAGE/$BUNDLE_NAME/IMAGE_TAG"
DEPLOY_TAR="$OUT_DIR/${BUNDLE_NAME}.tar.gz"
tar czf "$DEPLOY_TAR" -C "$STAGE" "$BUNDLE_NAME"

# 4) 校验和
( cd "$OUT_DIR" && sha256sum "$(basename "$IMG_TAR")" "$(basename "$DEPLOY_TAR")" > SHA256SUMS )

echo
echo "[package] 完成："
ls -lh "$IMG_TAR" "$DEPLOY_TAR" "$OUT_DIR/SHA256SUMS"
echo
echo "拷到新环境后：  docker load -i $(basename "$IMG_TAR")   &&   tar xzf $(basename "$DEPLOY_TAR")"
