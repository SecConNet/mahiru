.PHONY: docker_images
docker_images: base_docker_image registry_docker_image site_docker_image

# Extra docker_images makes them both build before exporting, which is
# faster because the tarball of the first doesn't end up in the build
# context of the second.
.PHONY: docker_tars
docker_tars: docker_images registry_docker_tar site_docker_tar


.PHONY: docker_clean
docker_clean:
	docker rmi -f mahiru-site:latest
	docker rmi -f mahiru-registry:latest
	docker rmi -f mahiru-base:latest


.PHONY: base_docker_image
base_docker_image:
	docker build . -f docker/mahiru/base.Dockerfile -t mahiru-base:latest

.PHONY: registry_docker_image
registry_docker_image: base_docker_image
	docker build . -f docker/mahiru/registry.Dockerfile -t mahiru-registry:latest

.PHONY: site_docker_image
site_docker_image: base_docker_image
	docker build . -f docker/mahiru/site.Dockerfile -t mahiru-site:latest


.PHONY: registry_docker_tar
registry_docker_tar: registry_docker_image
	docker save -o build/mahiru-registry-latest.tar mahiru-registry:latest

.PHONY: site_docker_tar
site_docker_tar: site_docker_image
	docker save -o build/mahiru-site-latest.tar mahiru-site:latest
