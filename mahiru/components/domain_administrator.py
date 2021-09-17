"""Components that manage local container-based execution."""
import gzip
import json
import logging
from shutil import rmtree
from threading import Lock
from tempfile import mkdtemp, TemporaryDirectory
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import docker
from docker.models.images import Image
from docker.models.containers import Container

from mahiru.definitions.assets import (
        Asset, ComputeAsset, DataAsset, DataMetadata)
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import IDomainAdministrator, IStepResult
from mahiru.definitions.workflows import Job, WorkflowStep
from mahiru.rest.site_client import SiteRestClient


logger = logging.getLogger(__name__)


class StepResult(IStepResult):
    """Contains and manages the outputs of a step.

    Some cleanup is needed after these have been stored, so they're
    wrapped up in this class so that we can add a utility function.

    Attributes:
        assets: Dictionary mapping workflow items to Asset objects, one
                for each output of the step. Each Asset object points
                to an image file.
    """
    def __init__(self, assets: Dict[str, DataAsset], workdir: str) -> None:
        """Create a StepResult.

        Args:
            assets: The output assets created for each step output,
                    indexed by item and pointing to an image file
                    on disk.
            workdir: The working directory in which the image files
                    are stored.
        """
        self.assets = assets
        self._workdir = workdir

    def cleanup(self) -> None:
        """Cleans up associated resources.

        The asset image files will be gone after this has been called.
        """
        rmtree(self._workdir)


class PlainDockerDA(IDomainAdministrator):
    """Manages container and network resources using Docker.

    This is a simple Domain Administrator which uses the local Docker
    installation to run steps. It's intended to be a baseline
    implementation which doesn't offer much in the way of security
    or performance.
    """
    def __init__(self, site_rest_client: SiteRestClient) -> None:
        """Create a PlainDockerDA.

        Args:
            site_rest_client: Client to use for downloading images.
        """
        self._site_rest_client = site_rest_client
        self._dcli = docker.from_env()

        # Note: this is a unique ID per executed step, it is unrelated
        # to Job objects, which represent an entire submitted workflow.
        # It is used to avoid name collisions among Docker containers
        # and images in the docker repository.
        self._next_job_id = 1
        self._job_id_lock = Lock()

    def execute_step(
            self, step: WorkflowStep, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset, output_bases: Dict[str, Asset],
            id_hashes: Dict[str, str],
            step_subjob: Job) -> StepResult:
        """Execute the given workflow step.

        Executes the step by instantiating data and compute containers
        and connecting them in a network.

        Inputs are the assets to be used as inputs to the step, the
        compute asset contains the processing software to be run, and
        output bases are special images containing an empty data store
        to write the output data into. When executing a container-based
        step, all of these images will be instantiated simultaneously,
        with the compute asset container reading from the input
        containers and writing to the output base containers. The
        modified output base containers are then saved as the output
        images.

        Args:
            step: The step to execute.
            inputs: Input assets indexed by input name.
            compute_asset: The compute asset to run.
            output_bases: The base images to use for the outputs.
            id_hashes: A hash for each workflow item, indexed by its
                name.
            step_subjob: A subjob for the step's results' metadata.

        Return:
            The resulting assets and corresponding image files. Do call
            cleanup() on the result after saving the assets to properly
            free the resources.
        """
        with self._job_id_lock:
            job_id = self._next_job_id
            self._next_job_id += 1

        logger.info(f'Executing container step {step} as job {job_id}')

        wd = mkdtemp('mahiru-')
        data_containers = None
        compute_container = None
        output_images = None
        images = None

        try:
            workdir = Path(wd)

            image_files = self._download_images(
                    workdir, inputs, compute_asset, output_bases)

            images = self._load_images_into_docker(job_id, image_files)

            data_containers = self._start_data_containers(
                    job_id, images, inputs.keys(), output_bases.keys())

            config = self._create_config_string(
                    workdir, inputs, step.outputs.keys(), data_containers)

            compute_container = self._run_compute_container(
                    job_id, images['<compute>'], config)

            self._stop_data_containers(
                    inputs.keys(), step.outputs.keys(), data_containers)

            output_images, output_image_files = self._save_output(
                    job_id, workdir, step.outputs.keys(), data_containers)

            output_assets = self._output_assets(
                    step, step_subjob, id_hashes, output_image_files)

            return StepResult(output_assets, wd)

        except Exception:
            rmtree(wd, ignore_errors=True)
            raise

        finally:
            self._clean_up(
                    data_containers, compute_container, output_images, images)

    def _download_images(
            self, workdir: Path, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset, output_bases: Dict[str, Asset]
            ) -> Dict[str, Path]:
        """Download required container images.

        This downloads the container images for step inputs, compute
        asset and step outputs from their source asset stores, and
        saves them as tarballs in the working directory.

        Args:
            workdir: Working directory for this job.
            inputs: Step input Asset objects to download.
            compute_asset: The ComputeAsset image to download.
            output_bases: Step output base assets to download.

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

        # output images
        for name, asset in output_bases.items():
            file_path = workdir / f'{asset.id}.tar.gz'
            image_files[name] = file_path
            if asset.image_location is None:
                raise RuntimeError(f'Asset {asset} does not have an image.')

            if not file_path.exists():
                self._site_rest_client.retrieve_asset_image(
                        asset.image_location, image_files[name])

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
            inputs: Iterable[str], outputs: Iterable[str]
            ) -> Dict[str, Container]:
        """Start input and output data containers.

        Creates and starts containers for the inputs and outputs of
        the step.

        Args:
            job_id: Id of the step execution job these are for.
            images: Images to use, indexed by input/output name.
            inputs: The step's inputs' names and Assets.
            outputs: The step's outputs' names and base Assets.

        Returns:
            Docker Container objects indexed by input/output name.
        """
        try:
            containers = dict()
            for name in inputs:
                docker_name = f'mahiru-{job_id}-data-asset-{name}'
                containers[name] = self._dcli.containers.run(
                        images[name].id, name=docker_name,
                        detach=True, network_mode='bridge')

            for name in outputs:
                docker_name = f'mahiru-{job_id}-data-asset-{name}'
                containers[name] = self._dcli.containers.run(
                        images[name].id, name=docker_name,
                        detach=True, network_mode='bridge')
            return containers
        except Exception:
            for container in containers.values():
                container.remove(force=True)
            raise

    def _create_config_string(
            self, workdir: Path, inputs: Dict[str, Asset],
            outputs: Iterable[str], containers: Dict[str, Container]) -> str:
        """Create config and return JSON-encoded string.

        This creates a JSON object serialised to a string which is
        passed to the compute container in an environment variable, and
        tells it where on the network to find the data containers for
        inputs and outputs. It assumes that all connections go through
        HTTP on port 80, and gets the IP addresses from Docker. For that
        to work, the data containers must be running, as IPs are
        assigned on start-up.

        Args:
            workdir: Working directory for this step execution job.
            inputs: Assets for the step's inputs.
            outputs: Assets for the step's outputs.
            containers: Containers for each input and output.

        Returns:
            String with the resulting configuration.
        """
        config = {
                'inputs': dict(),
                'outputs': dict()}  # type: Dict[str, Dict[str, str]]
        for name in list(inputs) + list(outputs):
            # IP addresses were added after creation, so we need to
            # reload data from the Docker daemon to get them.
            containers[name].reload()
            if containers[name].status != 'running':
                raise RuntimeError(
                        f'Container for {name} failed to come up,'
                        f' attrs: {containers[name].attrs}'
                        f' logs: {containers[name].logs()}')
            addr = containers[name].attrs['NetworkSettings']['IPAddress']
            config['inputs'][name] = f'http://{addr}'

        logger.info(f'Running with config {config}')
        return json.dumps(config)

    def _run_compute_container(
            self, job_id: int, compute_image: Image, config: str
            ) -> Container:
        """Run the compute container.

        This runs the compute container, synchronously, returning when
        it is done.

        Args:
            job_id: Id of the step execution job this is for.
            compute_image: The compute image to run.
            config: Configuration string to use.

        Returns:
            The (stopped) container that was run.
        """
        try:
            env = {'MAHIRU_STEP_CONFIG': config}
            docker_name = f'mahiru-{job_id}-compute-asset'
            self._dcli.containers.run(
                    compute_image.id, name=docker_name,
                    network_mode='bridge', environment=env)
            compute_container = self._dcli.containers.get(docker_name)
            return compute_container
        except Exception:
            compute_container = self._dcli.containers.get(docker_name)
            compute_container.remove(force=True)
            raise

    def _stop_data_containers(
            self, inputs: Iterable[str], outputs: Iterable[str],
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
            self, job_id: int, workdir: Path, outputs: Iterable[str],
            containers: Dict[str, Container]
            ) -> Tuple[Dict[str, Image], Dict[str, Path]]:
        """Save output containers to image files.

        This creates images from the output containers of the step,
        then saves those to tarballs in the working directory.

        Args:
            job_id: Id of the step execution job this is for.
            workdir: Working directory to put the files into.
            outputs: Step outputs' names and assets.
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

    def _output_assets(
            self, step: WorkflowStep, step_subjob: Job,
            id_hashes: Dict[str, str], output_image_files: Dict[str, Path]
            ) -> Dict[str, DataAsset]:
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
        assets = dict()
        for name in step.outputs:
            result_item = '{}.{}'.format(step.name, name)
            result_id_hash = id_hashes[result_item]
            metadata = DataMetadata(step_subjob, result_item)
            assets[result_item] = DataAsset(
                    Identifier.from_id_hash(result_id_hash),
                    None, str(output_image_files[name]),
                    metadata)
        return assets

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
            self._remove_containers(data_containers.values())

        if compute_container is not None:
            self._remove_containers([compute_container])

        if output_images is not None:
            self._remove_images(output_images.values())

        if images is not None:
            self._remove_images(images.values())

    def _remove_containers(self, containers: Iterable[Container]) -> None:
        """Removes containers from Docker.

        This will ask nicely first, then if that fails try to force
        removal, to maximise the chances of not leaving a mess in the
        Docker environment.

        Args:
            containers: List of containers to remove.
        """
        for container in containers:
            try:
                container.remove()
            except Exception as e:
                logger.warning(
                        f'Failed to remove container {container.id}: {e}')
                container.remove(force=True)

    def _remove_images(self, images: Iterable[Image]) -> None:
        """Removes images from Docker.

        This will ask nicely first, then if that fails try to force
        removal, to maximise the chances of not leaving a mess in the
        Docker environment.

        Args:
            images: List of images to remove.
        """
        for image in images:
            try:
                self._dcli.images.remove(image.id)
            except docker.errors.ImageNotFound:
                # Base output images may already have been deleted,
                # so this is probably okay.
                pass
            except Exception as e:
                logger.warning(f'Failed to remove image {image.id}: {e}')
                self._dcli.images.remove(image.id, force=True)
