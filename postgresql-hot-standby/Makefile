IMAGE_NAME = registry.cn-beijing.aliyuncs.com/docker_wachoo_1/redis
IMAGE_VERSION = 1.0.0
CONTAINER_NAMES = redis-${IMAGE_VERSION}-0

.PHONY: build start push

build: build-version


docker-run-master:
	docker run -d --name pgmaster \
    -p 5500:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -v ${HOME}/postgresql-cluster/pgmaster:/var/lib/postgresql/data \
    -d postgres:9.6

docker-run-slave1:
	docker run -d --name pgslave1 \
    -p 5501:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -v ${HOME}/postgresql-cluster/pgslave1:/var/lib/postgresql/data \
    -d postgres:9.6

docker-stop:
	docker stop pgmaster
	docker stop pgslave1

docker-start-pgslave1:
	docker start pgslave1

docker-start-pgmaster:
	docker start pgmaster

docker-slave-data-rm:
	docker exec -i pgslave1 rm -rf  /var/lib/postgresql/data/**

	# docker exec -it pgslave1 bash
	# rm -rf  /var/lib/postgresql/data/*
	# pg_basebackup -h 172.19.6.154 -p 5500 -U replica -F p -x -P -R -D /var/lib/postgresql/data/ -l postgresbackup20200306



build-version:
	docker build -t ${IMAGE_NAME}:${IMAGE_VERSION} -f Dockerfile  .

tag-latest:
	docker tag ${IMAGE_NAME}:${IMAGE_VERSION} ${IMAGE_NAME}:latest

start-celeryExecutor:
	docker-compose -f docker-compose.yml up -d

