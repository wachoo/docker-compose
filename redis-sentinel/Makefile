IMAGE_NAME = registry.cn-beijing.aliyuncs.com/docker_wachoo_1/redis
IMAGE_VERSION = 1.0.0
CONTAINER_NAMES = redis-${IMAGE_VERSION}-0

.PHONY: start-celeryExecutor

start-celeryExecutor:
	docker-compose -f docker-compose.yml up -d

