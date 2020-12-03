.PHONY: docker
docker: registry_docker

.PHONY: docker_clean
docker_clean:
	docker rmi mahiru-registry:latest

.PHONY: registry_docker
registry_docker:
	docker build . -f docker/registry.Dockerfile -t mahiru-registry:latest
