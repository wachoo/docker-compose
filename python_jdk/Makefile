IMAGE_NAME = wachoo/jdk8_py3
IMAGE_VERSION = latest
CONTAINER_NAMES = jdk8_py3-${IMAGE_VERSION}-0

.PHONY: build start push

build: build-version

build-version:
	docker build -t ${IMAGE_NAME}:${IMAGE_VERSION} -f Dockerfile  .

tag-latest:
	docker tag ${IMAGE_NAME}:${IMAGE_VERSION} ${IMAGE_NAME}:latest

