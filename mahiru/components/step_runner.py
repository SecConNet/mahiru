"""Components for on-site workflow execution."""
import logging
from threading import Thread
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.assets import (
        Asset, ComputeAsset, DataAsset, DataMetadata)
from mahiru.definitions.interfaces import IDomainAdministrator, IStepRunner
from mahiru.definitions.workflows import ExecutionRequest, WorkflowStep
from mahiru.policy.evaluation import (
        PermissionCalculator, PolicyEvaluator)
from mahiru.rest.site_client import SiteRestClient
from mahiru.components.asset_store import AssetStore


logger = logging.getLogger(__name__)


class JobRun(Thread):
    """A run of a job.

    This is a reification of the process of executing a job locally.
    """
    def __init__(
            self,
            site_rest_client: SiteRestClient,
            permission_calculator: PermissionCalculator,
            domain_administrator: IDomainAdministrator,
            this_site: Identifier,
            request: ExecutionRequest,
            target_store: AssetStore
            ) -> None:
        """Creates a JobRun object.

        This represents the execution of (parts of) a workflow at a
        site.

        Args:
            site_rest_client: A SiteRestClient to use.
            permission_calculator: A permission calculator to use to
                    check policy.
            domain_administrator: A domain administrator to use to
                    manage containers and networks.
            this_site: The site we're running at.
            request: The job to execute and plan to do it.
            target_store: The asset store to put results into.

        """
        super().__init__(name='JobAtRunner-{}'.format(this_site))
        self._permission_calculator = permission_calculator
        self._site_rest_client = site_rest_client
        self._domain_administrator = domain_administrator
        self._this_site = this_site
        self._job = request.job
        self._workflow = request.job.workflow
        self._inputs = request.job.inputs
        self._plan = request.plan
        self._sites = request.plan.step_sites
        self._target_store = target_store

    def run(self) -> None:
        """Runs the job.

        This executes the steps in the job one at a time, in an order
        compatible with their dependencies.
        """
        logger.info('Starting job at {}'.format(self._this_site))
        if not self._permission_calculator.is_legal(self._job, self._plan):
            # for each output we were supposed to produce
            #     store an error object instead
            # for now, we raise and crash
            raise RuntimeError(
                    'Security violation, asked to perform an illegal job.')

        id_hashes = self._job.id_hashes()

        steps_to_do = {
                step for step in self._workflow.steps.values()
                if self._sites[step.name] == self._this_site}

        while len(steps_to_do) > 0:
            for step in steps_to_do:
                success = self._try_execute_step(step, id_hashes)
                if success:
                    steps_to_do.remove(step)
                    break
            else:
                sleep(0.5)
        logger.info('Job at {} done'.format(self._this_site))

    def _try_execute_step(
            self, step: WorkflowStep, id_hashes: Dict[str, str]
            ) -> bool:
        """Try to execute a step, if its inputs are ready.

        Supports both container-based and plain steps; if the compute
        asset has an associated image then a container run will be
        attempted, otherwise we'll use the built-in hack.
        """
        inputs = self._get_step_inputs(step, id_hashes)
        if inputs is not None:
            compute_asset = self._retrieve_compute_asset(step.compute_asset_id)
            if compute_asset.image_location is not None:
                logger.info('Job at {} executing container step {}'.format(
                    self._this_site, step))

                output_bases = self._get_output_bases(step)
                step_subjob = self._job.subjob(step)
                result = self._domain_administrator.execute_step(
                        step, inputs, compute_asset, output_bases, id_hashes,
                        step_subjob)

                for name, path in result.files.items():
                    result_item = '{}.{}'.format(step.name, name)
                    result_id_hash = id_hashes[result_item]
                    metadata = DataMetadata(step_subjob, result_item)
                    asset = DataAsset(
                            Identifier.from_id_hash(result_id_hash),
                            None, str(path), metadata)
                    self._target_store.store(asset, True)

                result.cleanup()

            else:
                self._run_step(step, inputs, compute_asset, id_hashes)
        return inputs is not None

    def _get_step_inputs(
            self, step: WorkflowStep, id_hashes: Dict[str, str]
            ) -> Optional[Dict[str, Asset]]:
        """Find and obtain inputs for the step.

        If all inputs are available, returns a dictionary mapping their
        names to their values. If at least one input is not yet
        available, returns None.

        Args:
            step: The step to obtain inputs for.
            id_hashes: Id hashes for the workflow's items.

        Return:
            A dictionary keyed by input name with corresponding
            assets.

        """
        step_input_data = dict()
        for inp_name, inp_source in step.inputs.items():
            source_site, source_asset = self._source(inp_source, id_hashes)
            logger.info('Job at {} getting input {} from site {}'.format(
                self._this_site, source_asset, source_site))
            try:
                asset = self._site_rest_client.retrieve_asset(
                        source_site, source_asset)
                step_input_data[inp_name] = asset
                logger.info('Job at {} found input {} available.'.format(
                    self._this_site, source_asset))
                logger.info('Metadata: {}'.format(asset.metadata))
            except KeyError:
                logger.info(f'Job at {self._this_site} found input'
                            f' {source_asset} not yet available.')
                return None

        return step_input_data

    def _get_output_bases(self, step: WorkflowStep) -> Dict[str, Asset]:
        """Find and obtain output base assets for the compute asset.

        Args:
            step: The step we're going to execute.

        Return:
            A dictionary keyed by output name with corresponding
            base assets.

        """
        step_output_bases = dict()
        for out_name, asset_id in step.outputs.items():
            if asset_id is None:
                # Should not happen, test misconfigured?
                raise RuntimeError(f'Base asset needed for output {out_name}')
            try:
                asset = self._site_rest_client.retrieve_asset(
                        asset_id.location(), asset_id)
                step_output_bases[out_name] = asset
            except KeyError:
                logger.info(
                        f'Could not retrieve output base asset'
                        f' {asset_id} for output {out_name} of step'
                        f' {step.name}')
                raise
        return step_output_bases

    def _run_step(
            self, step: WorkflowStep, inputs: Dict[str, Asset],
            compute_asset: ComputeAsset, id_hashes: Dict[str, str]) -> None:
        """Run a workflow step."""
        logger.info('Job at {} executing step {}'.format(
            self._this_site, step))

        # run compute asset step
        outputs = compute_asset.run(
                {inp: asset.data for inp, asset in inputs.items()})

        # store asset objects for outputs
        results = list()    # type: List[DataAsset]
        step_subjob = self._job.subjob(step)
        for output_name, output_value in outputs.items():
            result_item = '{}.{}'.format(step.name, output_name)
            result_id_hash = id_hashes[result_item]
            metadata = DataMetadata(step_subjob, result_item)
            asset = DataAsset(
                    Identifier.from_id_hash(result_id_hash),
                    output_value, None, metadata)
            self._target_store.store(asset)

    def _retrieve_compute_asset(
            self, compute_asset_id: Identifier) -> ComputeAsset:
        asset = self._site_rest_client.retrieve_asset(
                compute_asset_id.location(), compute_asset_id)
        if not isinstance(asset, ComputeAsset):
            raise TypeError('Expecting a compute asset in workflow')
        return asset

    def _source(
            self, inp_source: str, id_hashes: Dict[str, str]
            ) -> Tuple[Identifier, Identifier]:
        """Extracts the source from a source description.

        If the input is of the form 'step.output', this will return the
        target site which is to execute that step according to the
        current plan, and the output (result) identifier.

        If the input is a reference to a workflow input, then this will
        return the site where the corresponding workflow input can be
        found, and its asset id.

        Args:
            inp_source: Source description as above.
            id_hashes: Id hashes for the workflow's items.

        """
        if '.' in inp_source:
            step_name, output_name = inp_source.split('.')
            return self._sites[step_name], Identifier.from_id_hash(
                    id_hashes[inp_source])
        else:
            dataset = self._inputs[inp_source]
            return dataset.location(), dataset


class StepRunner(IStepRunner):
    """A service for running steps of a workflow at a given site."""
    def __init__(
            self, site: Identifier,
            site_rest_client: SiteRestClient,
            policy_evaluator: PolicyEvaluator,
            domain_administrator: IDomainAdministrator,
            target_store: AssetStore) -> None:
        """Creates a StepRunner.

        Args:
            site: Name of the site this runner is located at.
            site_rest_client: A SiteRestClient to use.
            policy_evaluator: A PolicyEvaluator to use.
            domain_administrator: A domain administrator to use.
            target_store: An AssetStore to store result in.
        """
        self._site = site
        self._site_rest_client = site_rest_client
        self._permission_calculator = PermissionCalculator(policy_evaluator)
        self._domain_administrator = domain_administrator
        self._target_store = target_store

    def execute_request(self, request: ExecutionRequest) -> None:
        """Start a job in a separate thread.

        Args:
            request: The job to execute and plan to do it.

        """
        run = JobRun(
                self._site_rest_client, self._permission_calculator,
                self._domain_administrator, self._site, request,
                self._target_store)
        run.start()
