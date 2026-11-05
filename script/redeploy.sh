#!/bin/bash

# 进入部署目录
cd /opt/orderhisclean || { echo "进入目录失败，中止执行"; exit 1; }

# 可选参数：镜像 tag（默认 latest；tag 发布构建的镜像 tag 是版本号，如 0.0.6）
IMAGE_TAG="${1:-latest}"
IMAGE="registry.cn-hangzhou.aliyuncs.com/redgreat/orderhisclean:${IMAGE_TAG}"

# 优先使用 docker compose（v2），其次 docker-compose（v1），都没有则用原生 docker 命令
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  COMPOSE=""
  echo "提示：未找到 docker compose / docker-compose，将使用原生 docker 命令部署"
fi

echo "=========================================="
echo "    开始重新部署 orderhisclean 容器环境"
echo "    镜像: $IMAGE"
echo "=========================================="

echo "➤ 1. 停止并移除旧容器: orderhisclean ..."
if [ -n "$COMPOSE" ]; then
  $COMPOSE stop orderhisclean
  $COMPOSE rm -f orderhisclean
else
  docker stop orderhisclean 2>/dev/null || true
  docker rm orderhisclean 2>/dev/null || true
fi

echo "➤ 2. 拉取最新镜像..."
docker pull "$IMAGE" || { echo "拉取镜像失败，中止执行"; exit 1; }

echo "➤ 3. 删除其他 orderhisclean 旧镜像..."
docker images --format '{{.Repository}}:{{.Tag}}' | grep 'orderhisclean' | grep -v "$IMAGE" | xargs -r docker rmi -f 2>/dev/null || true

echo "➤ 4. 启动新容器..."
if [ -n "$COMPOSE" ]; then
  $COMPOSE up -d orderhisclean
else
  docker run -d --name orderhisclean --hostname orderhisclean \
    --restart unless-stopped \
    -v /opt/orderhisclean/conf:/app/conf \
    -v /opt/orderhisclean/log:/app/log \
    -e DISABLE_IPV6=true \
    -e TZ=Asia/Shanghai \
    -e LANG=zh_CN.UTF-8 \
    "$IMAGE"
fi

echo "等待 2 秒检查容器状态..."
sleep 2

echo "➤ 5. 当前 orderhisclean 容器运行状态:"
docker ps -a --filter "name=orderhisclean"

echo "=========================================="
echo "    容器已更新并重启完成！"
echo "=========================================="
