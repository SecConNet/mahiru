.PHONY: docker_images
docker_images: registry_docker_image

.PHONY: docker_clean
docker_clean:
	docker rmi -f mahiru-registry:latest

.PHONY: registry_docker_image
registry_docker_image:
	docker build . -f docker/registry.Dockerfile -t mahiru-registry:latest
