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
from mahiru.definitions.connections import (
        ConnectionInfo, ConnectionRequest)
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import (
        IDomainAdministrator, INetworkAdministrator, IStepResult)
from mahiru.definitions.workflows import Job, WorkflowStep
from mahiru.rest.site_client import SiteRestClient


logger = logging.getLogger(__name__)


class StepResult(IStepResult):
    """Contains and manages the outputs of a step.

    Some cleanup is needed after these have been stored, so they're
    wrapped up in this class so that we can add a utility function.

    Attributes:
        files: Dictionary mapping workflow output items to Paths of
                their image files.
    """
    def __init__(self, files: Dict[str, Path], workdir: str) -> None:
        """Create a StepResult.

        Args:
            files: Dictionary mapping workflow output items to Paths
                    of their image files.
            workdir: The working directory in which the image files
                    are stored.
        """
        self.files = files
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
    def __init__(
            self, network_administrator: INetworkAdministrator,
            site_rest_client: SiteRestClient) -> None:
        """Create a PlainDockerDA.

        Args:
            network_administrator: Network administrator to use.
            site_rest_client: Client to use for downloading images.
        """
        self._network_administrator = network_administrator
        self._site_rest_client = site_rest_client
        self._dcli = docker.from_env()

        # Note: this is a unique ID per executed step, it is unrelated
        # to Job objects, which represent an entire submitted workflow.
        # It is used to avoid name collisions among Docker containers
        # and images inside the local docker repository.
        self._next_job_id = 1
        self._job_id_lock = Lock()

        # Images currently loaded into Docker, indexed by asset id,
        # and a reference count.
        self._loaded_images_lock = Lock()
        self._loaded_images = dict()            # type: Dict[str, Identifier]
        self._loaded_images_ref_count = dict()  # type: Dict[str, int]

        # These are indexed by connection id, which is the stringified
        # job id of the serving job.
        self._served_lock = Lock()          # Protects the below
        self._served_assets = dict()        # type: Dict[str, Identifier]
        self._served_containers = dict()    # type: Dict[str, Container]

    def execute_step(
            self, step: WorkflowStep, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset, output_bases: Dict[str, Asset],
            id_hashes: Dict[str, str], step_subjob: Job) -> StepResult:
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
        job_id = self._get_job_id()

        logger.info(f'Executing container step {step} as job {job_id}')

        wd = mkdtemp(prefix='mahiru-')
        pilot_container = None
        input_containers = None
        output_containers = None
        compute_container = None
        nets = dict()               # type: Dict[str, str]
        images = None

        try:
            workdir = Path(wd)

            pilot_container = self._create_pilot_container(job_id)

            nets, local_inputs = self._network_administrator.connect_to_inputs(
                    job_id, inputs, pilot_container.attrs['State']['Pid'])

            logger.debug(f'Nets: {nets}')
            logger.debug(f'Remaining local inputs: {local_inputs}')

            local_assets = dict(
                    **local_inputs, **{'<compute>': compute_asset},
                    **output_bases)

            images = self._ensure_images_available(workdir, local_assets)

            input_containers = self._start_input_containers(
                    job_id, images, local_inputs.keys())

            output_containers = self._start_output_containers(
                    job_id, images, output_bases.keys())

            config = self._create_config_string(
                    workdir, local_inputs, nets, step.outputs.keys(),
                    dict(**input_containers, **output_containers))

            compute_container = self._run_compute_container(
                    job_id, pilot_container.id, images['<compute>'], config)

            output_files = dict()  # type: Dict[str, Path]
            for name in step.outputs:
                output_files[name] = self._save_output(
                        workdir, job_id, name, output_containers[name])

            return StepResult(output_files, wd)

        except Exception:
            rmtree(wd, ignore_errors=True)
            raise

        finally:
            if nets:
                self._network_administrator.disconnect_inputs(job_id, inputs)

            if input_containers is not None:
                self._remove_containers(input_containers.values())

            if compute_container is not None:
                self._remove_containers([compute_container])

            if pilot_container is not None:
                self._remove_containers([pilot_container])

            if local_assets is not None:
                for asset in local_assets.values():
                    self._free_image(asset.id)

    def serve_asset(
            self, asset: Asset, connection_request: ConnectionRequest
            ) -> ConnectionInfo:
        """Serve an asset as a VPN-reachable service.

        The asset's image_location must be a path on the local disk,
        this function does not serve remote images.

        Args:
            asset: A local asset to serve.
            connection_request: A description of the remote end of the
                VPN connection to set up.
        """
        if asset.image_location is None:
            raise RuntimeError(
                    f'Cannot serve asset {asset} because it has no image')

        if asset.image_location.startswith('http:'):
            raise RuntimeError(f'Refusing to serve remote asset {asset}')

        job_id = self._get_job_id()
        conn_id = str(job_id)

        logger.info(f'Serving container {asset.id} as job {job_id}')

        image = None
        container = None
        try:
            image = self._ensure_image_available(asset)

            container_name = f'mahiru-{job_id}-data-asset-remote'
            container = self._dcli.containers.run(
                    image, name=container_name,
                    detach=True, network_mode='none')
            container.reload()
            pid = container.attrs['State']['Pid']

            with self._served_lock:
                self._served_assets[conn_id] = asset.id
                self._served_containers[conn_id] = container

            return self._network_administrator.serve_asset(
                    conn_id, pid, connection_request)

        except Exception as e:
            logger.error(f'Error serving asset connection: {e}')
            if container is not None:
                self._remove_containers([container])
            if image is not None:
                self._free_image(asset.id)
            if conn_id in self._served_assets:
                del self._served_assets[conn_id]
            if conn_id in self._served_containers:
                del self._served_containers[conn_id]
            raise

    def stop_serving_asset(self, conn_id: str) -> None:
        """Stop and remove an asset container being served remotely.

        The conn_id argument must have been returned by serve_asset()
        previously. Implementation note: it's just the job id converted
        to a string.

        Args:
            conn_id: Connection id for the serving job.
        """
        logger.info(f'Stopping with serving for job {conn_id}')
        pid = self._served_containers[conn_id].attrs['State']['Pid']
        self._network_administrator.stop_serving_asset(conn_id, pid)
        with self._served_lock:
            self._remove_containers([self._served_containers[conn_id]])
            del self._served_containers[conn_id]
            self._free_image(self._served_assets[conn_id])
            del self._served_assets[conn_id]

    def _get_job_id(self) -> int:
        """Return a new unique job id."""
        with self._job_id_lock:
            job_id = self._next_job_id
            self._next_job_id += 1
        return job_id

    def _create_pilot_container(self, job_id: int) -> Container:
        """Creates the pilot container for this job.

        The pilot container provides a network namespace we can
        configure with external connections before we create the
        compute asset container. The latter will then be started in
        the pilot container's network namespace.

        Args:
            job_id: Id of this job.

        Return:
            The running pilot container.
        """
        image_file = Path(__file__).parents[1] / 'data' / 'pilot.tar.gz'
        with image_file.open('rb') as f:
            self._dcli.images.load(f.read())[0]

        docker_name = f'mahiru-{job_id}-pilot',
        container = self._dcli.containers.run(
                'mahiru-pilot:latest', name=docker_name,
                detach=True, network_mode='bridge')
        # reload so we can get the PID correctly
        container.reload()
        return container

    def _ensure_image_available(
            self, asset: Asset, workdir: Optional[Path] = None) -> Image:
        """Ensures the asset's image is available in Docker.

        This will download it from another site if necessary, then
        load it into the local Docker daemon so that we can use it
        when starting containers. If it's already there, this
        increments the ref count so that it remains available.

        Args:
            asset: The asset to make available.
            workdir: Directory to download files into. May be omitted
                    or None if the asset is local and its
                    .image_location points to a file rather than a URL.

        Return:
            The loaded Docker Image.
        """
        with self._loaded_images_lock:
            if self._loaded_images_ref_count.get(asset.id, 0) != 0:
                self._loaded_images_ref_count[asset.id] += 1
                image_id = self._loaded_images[asset.id]
                image = self._dcli.images.get(image_id)
            else:
                if asset.image_location is None:
                    raise RuntimeError(
                            f'Asset {asset} does not have an image.')

                if asset.image_location.startswith('http:'):
                    if workdir is None:
                        raise RuntimeError(
                                f'Remote asset and no workdir given.')

                    image_file = workdir / f'{asset.id}.tar.gz'
                    self._site_rest_client.retrieve_asset_image(
                            asset.image_location, image_file)
                else:
                    image_file = Path(asset.image_location)

                with image_file.open('rb') as f:
                    image = self._dcli.images.load(f.read())[0]

                self._loaded_images[asset.id] = image.id
                self._loaded_images_ref_count[asset.id] = 1
                # TODO: could delete the file here to save disk space,
                # but only if it's a remote image and the file is in
                # the workdir! If it's local, we'll delete it from
                # the asset store!

            return image

    def _free_image(self, asset_id: str) -> None:
        """Decrements the use count on the asset image.

        If this was the last user, the image is removed from Docker,
        and will be re-downloaded next time it is needed.

        Args:
            asset_id: The id of the asset that's no longer needed by
                    the caller.
        """
        with self._loaded_images_lock:
            if asset_id not in self._loaded_images_ref_count:
                raise KeyError('Asset not found.')

            self._loaded_images_ref_count[asset_id] -= 1
            if self._loaded_images_ref_count[asset_id] == 0:
                image_id = self._loaded_images[asset_id]
                try:
                    self._dcli.images.remove(image_id)
                except docker.errors.ImageNotFound:
                    # Base output images may already have been deleted,
                    # so this is probably okay.
                    pass
                except Exception as e:
                    logger.warning(f'Failed to remove image {image_id}: {e}')
                    self._dcli.images.remove(image_id, force=True)
                finally:
                    del self._loaded_images[asset_id]

    def _ensure_images_available(
            self, workdir: Path, assets: Dict[str, Asset],
            ) -> Dict[str, Image]:
        """Ensure required images are available in Docker.

        Args:
            workdir: Working directory for this job.
            assets: Asset objects to download.

        Returns:
            Docker Image objects indexed by input/output name.
        """
        images = dict()

        try:
            for name, asset in assets.items():
                images[name] = self._ensure_image_available(asset, workdir)

        except Exception:
            # Restore Docker store to previous state in case of error,
            # so that we don't leave unused images around when we abort
            # the job.
            for name in images:
                self._free_image(assets[name].id)
            raise

        return images

    def _start_input_containers(
            self, job_id: int, images: Dict[str, Image],
            inputs: Iterable[str]) -> Dict[str, Container]:
        """Start input data containers.

        Creates and starts containers for the inputs of the step.

        Args:
            job_id: Id of the step execution job these are for.
            images: Images to use, indexed by input/output name.
            inputs: The step's inputs' names and Assets.

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
            return containers

        except Exception:
            for container in containers.values():
                container.remove(force=True)
            raise

    def _start_output_containers(
            self, job_id: int, images: Dict[str, Image],
            outputs: Iterable[str]) -> Dict[str, Container]:
        """Start output data containers.

        Creates and starts containers for the outputs of the step.

        Args:
            job_id: Id of the step execution job these are for.
            images: Images to use, indexed by input/output name.
            outputs: The step's outputs' names and base Assets.

        Returns:
            Docker Container objects indexed by output name.
        """
        try:
            containers = dict()
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
            self, workdir: Path, local_inputs: Dict[str, Asset],
            nets: Dict[str, str], outputs: Iterable[str],
            containers: Dict[str, Container]) -> str:
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
            local_inputs: Assets for the step's local inputs.
            nets: IP addresses for the step's remote inputs.
            outputs: Assets for the step's outputs.
            containers: Containers for each input and output.

        Returns:
            String with the resulting configuration.
        """
        config = {
                'inputs': dict(),
                'outputs': dict()}  # type: Dict[str, Dict[str, str]]

        for name, addr in nets.items():
            config['inputs'][name] = f'http://{addr}'

        for name in list(local_inputs) + list(outputs):
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
            self, job_id: int, pilot_container_id: str, compute_image: Image,
            config: str) -> Container:
        """Run the compute container.

        This runs the compute container, synchronously, returning when
        it is done.

        Args:
            job_id: Id of the step execution job this is for.
            pilot_container_id: Docker id of the pilot container, whose
                    network namespace we'll use.
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
                    network_mode=f'container:{pilot_container_id}',
                    environment=env)
            compute_container = self._dcli.containers.get(docker_name)
            return compute_container
        except Exception:
            compute_container = self._dcli.containers.get(docker_name)
            compute_container.remove(force=True)
            raise

    def _save_output(
            self, workdir: Path, job_id: int, output_name: str,
            container: Container) -> Path:
        """Saves the output container to an Asset plus image.

        The image is put into the work dir, and refered to by the Asset
        object's image_location. The container is removed once it's
        been saved to an image file.

        Args:
            workdir: Temporary working directory for this job.
            job_id: Unique id of this job.
            output_name: Name of the output.
            container: Output container containing the result.

        Return:
            An Asset object for the saved output.
        """
        logger.debug(f'Saving container {container.id}')
        container.stop()

        image = None
        try:
            image_name = f'mahiru-{job_id}-data-asset-{output_name}'
            container.commit(image_name)
            image = self._dcli.images.get(image_name)
            out_path = workdir / f'mahiru-data-asset-{output_name}.tar.gz'
            with gzip.open(str(out_path), 'wb', 1) as f:
                for chunk in image.save():
                    f.write(chunk)
            container.remove()
            self._dcli.images.remove(image.id)
            return out_path

        except Exception:
            if image is not None:
                self._dcli.images.remove(image.id, force=True)
            raise

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
                container.stop()
            except Exception:
                # ignore, we may be cleaning up an error situation
                pass

            try:
                container.remove()
            except Exception as e:
                logger.warning(
                        f'Failed to remove container {container.id} safely:'
                        f' {e}')
                logger.warning(
                        f'Attempting to force removal of {container.id}')
                container.remove(force=True)
