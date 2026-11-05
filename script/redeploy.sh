#!/bin/bash

# 进入部署目录
cd /opt/orderhisclean || { echo "进入目录失败，中止执行"; exit 1; }

# 优先使用 docker compose（v2），不存在则使用 docker-compose（v1）
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "错误：未找到 docker compose 或 docker-compose 命令"
  exit 1
fi

echo "=========================================="
echo "    开始重新部署 orderhisclean 容器环境"
echo "=========================================="

echo "➤ 1. 停止并移除旧容器: orderhisclean ..."
$COMPOSE stop orderhisclean
$COMPOSE rm -f orderhisclean

echo "➤ 2. 删除本地所有的 orderhisclean 镜像记录..."
docker images | grep 'orderhisclean' | awk '{print $2}' | xargs -r docker rmi -f

echo "➤ 3. 重新拉取最新镜像并后台启动..."
$COMPOSE pull orderhisclean
$COMPOSE up -d orderhisclean

echo "等待 2 秒检查容器状态..."
sleep 2

echo "➤ 4. 当前 orderhisclean 容器运行状态:"
docker ps -a --filter "name=orderhisclean"

echo "=========================================="
echo "    容器已更新并重启完成！"
echo "=========================================="
