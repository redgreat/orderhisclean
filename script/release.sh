#!/bin/bash

git tag v0.0.1
git push origin v0.0.1

docker stop orderhisclean
docker rm orderhisclean
docker rmi registry.cn-hangzhou.aliyuncs.com/redgreat/orderhisclean
docker-compose up -d