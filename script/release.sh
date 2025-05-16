#!/bin/bash

git tag v0.0.1
git push origin v0.0.1

cd /opt/orderhisclean
docker stop orderhisclean
docker rm orderhisclean
docker rmi registry.cn-hangzhou.aliyuncs.com/redgreat/orderhisclean
docker pull registry.cn-hangzhou.aliyuncs.com/redgreat/orderhisclean:latest
docker-compose up -d
docker logs orderhisclean


cd /opt/orderhisclean
docker stop orderhisclean
docker rm orderhisclean
docker rmi registry.cn-hangzhou.aliyuncs.com/redgreat/orderhisclean:main
docker pull registry.cn-hangzhou.aliyuncs.com/redgreat/orderhisclean:main
docker-compose up -d
docker logs orderhisclean


# 手动执行
docker exec orderhisclean python src/job_scheduler.py --run-now
