"""Components that manage local container-based execution."""
import gzip
import json
import logging
from threading import Lock
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import docker
from docker.models.images import Image
from docker.models.containers import Container

from proof_of_concept.components.asset_store import AssetStore
from proof_of_concept.definitions.assets import (
        Asset, ComputeAsset, DataAsset, Metadata)
from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import IDomainAdministrator
from proof_of_concept.definitions.workflows import Job, WorkflowStep
from proof_of_concept.rest.client import SiteRestClient


OUTPUT_ASSET_ID = Identifier('asset:ns:output_base:ns:site')


logger = logging.getLogger(__name__)


class PlainDockerDA(IDomainAdministrator):
    """Manages container and network resources using Docker.

    This is a simple Domain Administrator which uses the local Docker
    installation to run steps. It's intended to be a baseline
    implementation which doesn't offer much in the way of security
    or performance.
    """
    def __init__(
            self, site_rest_client: SiteRestClient, target_store: AssetStore
            ) -> None:
        """Create a PlainDockerDA.

        Args:
            site_rest_client: Client to use for downloading images.
            target_store: Store to put step results into.
        """
        self._site_rest_client = site_rest_client
        self._target_store = target_store
        self._dcli = docker.from_env()

        # Note: this is a unique ID per executed step, it is unrelated
        # to Job objects, which represent an entire submitted workflow.
        # It is used to avoid name collisions among Docker containers
        # and images in the docker repository.
        self._next_job_id = 1
        self._job_id_lock = Lock()

    def execute_step(
            self, step: WorkflowStep, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset, id_hashes: Dict[str, str],
            step_subjob: Job) -> None:
        """Execute the given workflow step.

        Executes the step by instantiating data and compute containers
        and connecting them in a network.

        Args:
            step: The step to execute.
            inputs: Input assets indexed by input name.
            compute_asset: The compute asset to run.
            id_hashes: A hash for each workflow item, indexed by its
                name.
            step_subjob: A subjob for the step's results' metadata.
        """
        with self._job_id_lock:
            job_id = self._next_job_id
            self._next_job_id += 1

        logger.info(f'Executing container step {step} as job {job_id}')

        with tempfile.TemporaryDirectory() as wd:
            data_containers = None
            compute_container = None
            output_images = None
            images = None

            try:
                workdir = Path(wd)

                image_files = self._download_images(
                        workdir, inputs, compute_asset)

                images = self._load_images_into_docker(job_id, image_files)

                data_containers = self._start_data_containers(
                        job_id, images, inputs, step.outputs)

                config_file = self._create_config_file(
                        workdir, inputs, step.outputs, data_containers)

                compute_container = self._run_compute_container(
                        job_id, images['<compute>'], config_file)

                self._stop_data_containers(
                        inputs, step.outputs, data_containers)

                output_images, output_image_files = self._save_output(
                        job_id, workdir, step.outputs, data_containers)

                self._store_output_assets(
                        step, step_subjob, id_hashes,
                        output_image_files)
            finally:
                self._clean_up(
                        data_containers, compute_container, output_images,
                        images)

    def _download_images(
            self, workdir: Path, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset) -> Dict[str, Path]:
        """Download required container images.

        This downloads the container images for step inputs, compute
        asset and step outputs from their source asset stores, and
        saves them as tarballs in the working directory.

        Args:
            workdir: Working directory for this job.
            inputs: Step input Asset objects to download.
            compute_asset: The ComputeAsset image to download.

        Returns:
            Paths to the downloaded files, indexed by input/output
            name.
        """
        image_files = dict()

        # input images
        for name, asset in inputs.items():
            image_files[name] = workdir / f'{asset.id}.tar.gz'
            if asset.image_location is None:
                raise RuntimeError(
                        f'Asset {asset} does not have an image.')
            self._site_rest_client.retrieve_asset_image(
                    asset.image_location, image_files[name])

        # compute asset image
        image_files['<compute>'] = workdir / f'{compute_asset.id}.tar.gz'
        if compute_asset.image_location is None:
            raise RuntimeError(
                    f'Asset {compute_asset} does not have an image.')
        self._site_rest_client.retrieve_asset_image(
                compute_asset.image_location, image_files['<compute>'])

        # output image
        # TODO: Add asset ids to the compute asset metadata instead of
        # hardcoding them here.
        output_asset = self._site_rest_client.retrieve_asset(
                OUTPUT_ASSET_ID.location(), OUTPUT_ASSET_ID)
        image_files['<output>'] = workdir / f'{output_asset.id}.tar.gz'
        if output_asset.image_location is None:
            raise RuntimeError(
                    f'Asset {output_asset} does not have an image.')
        self._site_rest_client.retrieve_asset_image(
                output_asset.image_location, image_files['<output>'])

        return image_files

    def _load_images_into_docker(
            self, job_id: int, image_files: Dict[str, Path]
            ) -> Dict[str, Image]:
        """Load images from files into Docker.

        This loads container images from tarballs on disk into the
        local Docker image repository.

        Args:
            job_id: Id of the step execution job these are for.
            image_files: Files (tarballs) to load in.

        Returns:
            Docker Image objects indexed by input/output name.
        """
        try:
            images = dict()
            for name, path in image_files.items():
                with path.open('rb') as f:
                    images[name] = self._dcli.images.load(f.read())[0]
            return images
        except Exception:
            for image in images.values():
                self._dcli.images.remove(image.id)
            raise

    def _start_data_containers(
            self, job_id: int, images: Dict[str, Image],
            inputs: Dict[str, Asset], outputs: List[str]
            ) -> Dict[str, Container]:
        """Start input and output data containers.

        Creates and starts containers for the inputs and outputs of
        the step.

        Args:
            job_id: Id of the step execution job these are for.
            images: Images to use, indexed by input/output name.
            inputs: The step's inputs' names and Assets.
            outputs: The step's outputs' names.

        Returns:
            Docker Container objects indexed by input/output name.
        """
        try:
            containers = dict()
            for name, asset in inputs.items():
                docker_name = f'mahiru-{job_id}-data-asset-{name}'
                containers[name] = self._dcli.containers.run(
                        images[name].id, name=docker_name,
                        detach=True, network_mode='bridge')

            for name in outputs:
                docker_name = f'mahiru-{job_id}-data-asset-{name}'
                containers[name] = self._dcli.containers.run(
                        images['<output>'].id, name=docker_name,
                        detach=True, network_mode='bridge')
            return containers
        except Exception:
            for container in containers.values():
                container.remove(force=True)
            raise

    def _create_config_file(
            self, workdir: Path, inputs: Dict[str, Asset], outputs: List[str],
            containers: Dict[str, Container]) -> Path:
        """Create config file in workdir and return path.

        This creates a JSON configuration file in the working directory
        which is mounted into the compute container, and tells it where
        on the network to find the data containers for inputs and
        outputs. It assumes that all connections go through HTTP on
        port 80, and gets the IP addresses from Docker. For that to
        work, the data containers must be running, as IPs are assigned
        on start-up.

        Args:
            workdir: Working directory for this step execution job.
            inputs: Assets for the step's inputs.
            outputs: The step's outputs.
            containers: Containers for each input and output.

        Returns:
            Path to the created configuration file.
        """
        config = {
                'inputs': dict(),
                'outputs': dict()}  # type: Dict[str, Dict[str, str]]
        for name in inputs:
            # IP addresses were added after creation, so we need to
            # reload data from the Docker daemon to get them.
            containers[name].reload()
            addr = containers[name].attrs['NetworkSettings']['IPAddress']
            config['inputs'][name] = f'http://{addr}'

        for name in outputs:
            containers[name].reload()
            addr = containers[name].attrs['NetworkSettings']['IPAddress']
            config['outputs'][name] = f'http://{addr}'

        logger.info(f'Running with config {config}')
        config_file = workdir / 'step_config.json'
        with config_file.open('w') as f2:
            json.dump(config, f2)
        return config_file

    def _run_compute_container(
            self, job_id: int, compute_image: Image, config_file: Path
            ) -> Container:
        """Run the compute container.

        This runs the compute container, synchronously, returning when
        it is done.

        Args:
            job_id: Id of the step execution job this is for.
            compute_image: The compute image to run.
            config_file: Input/output configuration file to use.

        Returns:
            The (stopped) container that was run.
        """
        try:
            config_mount = docker.types.Mount(
                    '/etc/mahiru/step_config.json', str(config_file), 'bind')
            docker_name = f'mahiru-{job_id}-compute-asset'
            self._dcli.containers.run(
                    compute_image.id, name=docker_name,
                    network_mode='bridge', mounts=[config_mount])
            compute_container = self._dcli.containers.get(docker_name)
            return compute_container
        except Exception:
            compute_container = self._dcli.containers.get(docker_name)
            compute_container.remove(force=True)
            raise

    def _stop_data_containers(
            self, inputs: Dict[str, Asset], outputs: List[str],
            containers: Dict[str, Container]) -> None:
        """Stop input and output data containers.

        Stops data containers, but not the compute container, which
        should be done already.

        Args:
            inputs: Step input Assets indexed by name.
            outputs: Step output names.
            containers: Docker Container objects, indexed by
                input/output name.
        """
        for name in inputs:
            containers[name].stop()

        for name in outputs:
            containers[name].stop()

    def _save_output(
            self, job_id: int, workdir: Path, outputs: List[str],
            containers: Dict[str, Container]
            ) -> Tuple[Dict[str, Image], Dict[str, Path]]:
        """Save output containers to image files.

        This creates images from the output containers of the step,
        then saves those to tarballs in the working directory.

        Args:
            job_id: Id of the step execution job this is for.
            workdir: Working directory to put the files into.
            outputs: Step outputs' names.
            containers: Docker containers indexed by output names.

        Returns:
            Docker Image objects and tarball paths, both indexed by
            output name.
        """
        try:
            output_image_files = dict()
            output_images = dict()
            for name in outputs:
                image_name = f'mahiru-{job_id}-data-asset-{name}'
                containers[name].commit(image_name)
                output_images[name] = self._dcli.images.get(image_name)
                out_path = workdir / f'mahiru-data-asset-{name}.tar.gz'
                with gzip.open(str(out_path), 'wb', 1) as f3:
                    for chunk in output_images[name].save():
                        f3.write(chunk)
                output_image_files[name] = out_path
        except Exception:
            for image in output_images.values():
                self._dcli.images.remove(image.id, force=True)
            raise

        return output_images, output_image_files

    def _store_output_assets(
            self, step: WorkflowStep, step_subjob: Job,
            id_hashes: Dict[str, str], output_image_files: Dict[str, Path]
            ) -> None:
        """Stores results into the target asset store.

        This creates, for each output, an Asset object with metadata
        and stores it in the target store, together with the
        corresponding tarball.

        Args:
            step: The workflow step we're running.
            step_subjob: A minimal workflow to calculate the outputs
                of the current step. See :meth:`Job.subjob`.
            id_hashes: ID hashes of the step's outputs, indexed by
                workflow item. See :meth:`Job.id_hashes`.
            output_image_files: Paths to image files, indexed by output
                name.
        """
        for name in step.outputs:
            result_item = '{}.{}'.format(step.name, name)
            result_id_hash = id_hashes[result_item]
            metadata = Metadata(step_subjob, result_item)
            asset = DataAsset(
                    Identifier.from_id_hash(result_id_hash),
                    None, str(output_image_files[name]),
                    metadata)
            self._target_store.store(asset, True)

    def _clean_up(
            self, data_containers: Optional[Dict[str, Container]],
            compute_container: Optional[Container],
            output_images: Optional[Dict[str, Image]],
            images: Optional[Dict[str, Image]]) -> None:
        """Remove containers and images from Docker.

        This tries to clean up the local Docker environment, removing
        containers and images even if something went wrong during
        execution. In that case, not all containers and images may
        exist, but it's important to remove what is there to avoid
        making a mess, breaking future runs, or running out of disk
        space (a potential DOS).

        Note that the output images, which include the data and were
        created when saving the output containers, must be deleted
        before the original empty output images, because they depend
        on them. So they're passed separately, and deleted first.

        Args:
            data_containers: Docker Container objects for inputs and
                outputs, if any have been created.
            compute_container: Docker Container object for the compute
                container, if it has been created.
            output_images: Images that output containers were saved to.
            images: Images for inputs, outputs and the compute asset.
        """
        if data_containers is not None:
            for container in data_containers.values():
                try:
                    container.remove()
                except Exception as e:
                    logger.warning(
                            f'Failed to remove container {container.id}: {e}')
                    container.remove(force=True)

        if compute_container is not None:
            try:
                compute_container.remove()
            except Exception as e:
                logger.warning(
                        f'Failed to remove container {compute_container.id}:'
                        f'{e}')
                compute_container.remove(force=True)

        if output_images is not None:
            for name, image in output_images.items():
                try:
                    self._dcli.images.remove(image.id)
                except Exception as e:
                    logger.warning(f'Failed to remove image {image.id}: {e}')
                    self._dcli.images.remove(image.id, force=True)

        if images is not None:
            for name, image in images.items():
                try:
                    self._dcli.images.remove(image.id)
                except docker.errors.ImageNotFound:
                    # base output images may already have been deleted
                    pass
                except Exception as e:
                    logger.warning(f'Failed to remove image {image.id}: {e}')
                    self._dcli.images.remove(image.id, force=True)
