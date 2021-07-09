export DOCKER_BUILDKIT = 1

.PHONY: docs
docs:
	PYTHONPATH=${PYTHONPATH}:${PWD} make -C docs html

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


.PHONY: assets_clean
assets_clean:
	docker rmi -f mahiru-test/data-asset-base:latest
	docker rmi -f mahiru-test/data-asset-input:latest
	docker rmi -f mahiru-test/compute-asset-base:latest
	docker rmi -f mahiru-test/compute-asset:latest


.PHONY: assets
assets: data_asset_base_tar data_asset_input_tar compute_asset_tar
	# docker rmi images here?

.PHONY: data_asset_base_tar
data_asset_base_tar: data_asset_base
	docker save mahiru-test/data-asset-base:latest | gzip -1 -c >build/data-asset-base.tar.gz

.PHONY: data_asset_input_tar
data_asset_input_tar: data_asset_input
	docker save mahiru-test/data-asset-input:latest | gzip -1 -c >build/data-asset-input.tar.gz

.PHONY: compute_asset_tar
compute_asset_tar: compute_asset
	docker save mahiru-test/compute-asset:latest | gzip -1 -c >build/compute-asset.tar.gz


.PHONY: data_asset_base
data_asset_base:
	docker build docker/assets -f docker/assets/data-asset-base.Dockerfile -t mahiru-test/data-asset-base:latest

.PHONY: data_asset_input
data_asset_input: data_asset_base
	docker build docker/assets -f docker/assets/data-asset-input.Dockerfile -t mahiru-test/data-asset-input:latest

.PHONY: compute_asset_base
compute_asset_base:
	docker build docker/assets -f docker/assets/compute-asset-base.Dockerfile -t mahiru-test/compute-asset-base:latest

.PHONY: compute_asset
compute_asset: compute_asset_base
	docker build docker/assets -f docker/assets/compute-asset.Dockerfile -t mahiru-test/compute-asset:latest
