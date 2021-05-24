.PHONY: docker_images
docker_images: base_docker_image registry_docker_image site_docker_image

.PHONY: docker_clean
docker_clean:
	docker rmi -f mahiru-site:latest
	docker rmi -f mahiru-registry:latest
	docker rmi -f mahiru-base:latest


.PHONY: base_docker_image
base_docker_image:
	docker build . -f src/docker/mahiru/base.Dockerfile -t mahiru-base:latest

.PHONY: registry_docker_image
registry_docker_image: base_docker_image
	docker build . -f src/docker/mahiru/registry.Dockerfile -t mahiru-registry:latest

.PHONY: site_docker_image
site_docker_image: base_docker_image
	docker build . -f src/docker/mahiru/site.Dockerfile -t mahiru-site:latest
