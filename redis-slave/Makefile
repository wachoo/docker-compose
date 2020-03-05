IMAGE_NAME = registry.cn-beijing.aliyuncs.com/docker_wachoo_1/redis
IMAGE_VERSION = 1.0.0
CONTAINER_NAMES = redis-${IMAGE_VERSION}-0

.PHONY: build start push

build: build-version

build-version:
	docker build -t ${IMAGE_NAME}:${IMAGE_VERSION} -f Dockerfile  .

tag-latest:
	docker tag ${IMAGE_NAME}:${IMAGE_VERSION} ${IMAGE_NAME}:latest

start-celeryExecutor:
	docker-compose -f docker-compose.yml up -d

