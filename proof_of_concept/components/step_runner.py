"""Components for on-site workflow execution."""
import logging
from threading import Thread
from time import sleep
from typing import Any, Dict, Optional, Tuple

from proof_of_concept.definitions.assets import (
        ComputeAsset, DataAsset, Metadata)
from proof_of_concept.definitions.interfaces import IStepRunner
from proof_of_concept.definitions.workflows import JobSubmission, WorkflowStep
from proof_of_concept.policy.evaluation import (
        PermissionCalculator, PolicyEvaluator)
from proof_of_concept.rest.client import SiteRestClient
from proof_of_concept.components.asset_store import AssetStore
from proof_of_concept.components.registry_client import RegistryClient


logger = logging.getLogger(__file__)


class JobRun(Thread):
    """A run of a job.

    This is a reification of the process of executing a job locally.
    """
    def __init__(
            self, policy_evaluator: PolicyEvaluator,
            this_site: str,
            registry_client: RegistryClient,
            site_rest_client: SiteRestClient,
            submission: JobSubmission,
            target_store: AssetStore
            ) -> None:
        """Creates a JobRun object.

        This represents the execution of (parts of) a workflow at a
        site.

        Args:
            policy_evaluator: A policy evaluator to use to check policy.
            this_site: The site we're running at.
            registry_client: A RegistryClient to use.
            site_rest_client: A SiteRestClient to use.
            submission: The job to execute and plan to do it.
            target_store: The asset store to put results into.

        """
        super().__init__(name='JobAtRunner-{}'.format(this_site))
        self._policy_evaluator = policy_evaluator
        self._permission_calculator = PermissionCalculator(policy_evaluator)
        self._this_site = this_site
        self._registry_client = registry_client
        self._site_rest_client = site_rest_client
        self._job = submission.job
        self._workflow = submission.job.workflow
        self._inputs = submission.job.inputs
        self._plan = submission.plan
        self._sites = submission.plan.step_sites
        self._target_store = target_store

    def run(self) -> None:
        """Runs the job.

        This executes the steps in the job one at a time, in an order
        compatible with their dependencies.
        """
        if not self._is_legal():
            # for each output we were supposed to produce
            #     store an error object instead
            # for now, we raise and crash
            raise RuntimeError(
                    'Security violation, asked to perform an illegal job.')

        keys = self._job.keys()

        steps_to_do = {
                step for step in self._workflow.steps.values()
                if self._sites[step.name] == self._this_site}

        while len(steps_to_do) > 0:
            for step in steps_to_do:
                inputs = self._get_step_inputs(step, keys)
                compute_asset = self._retrieve_compute_asset(
                    step.compute_asset_id)
                if inputs is not None:
                    logger.info('Job at {} executing step {}'.format(
                        self._this_site, step))
                    # run compute asset step
                    outputs = compute_asset.run(inputs)

                    # save output to store
                    step_subjob = self._job.subjob(step)
                    for output_name, output_value in outputs.items():
                        result_item = '{}.{}'.format(step.name, output_name)
                        result_key = keys[result_item]
                        metadata = Metadata(step_subjob, result_item)
                        asset = DataAsset(id=result_key,
                                          data=output_value,
                                          metadata=metadata)
                        self._target_store.store(asset)

                    steps_to_do.remove(step)
                    break
            else:
                sleep(0.5)
        logger.info('Job at {} done'.format(self._this_site))

    def _is_legal(self) -> bool:
        """Checks whether this request is legal.

        If we have permission to execute all of our steps, then this
        is a legal job as far as we are concerned.
        """
        perms = self._permission_calculator.calculate_permissions(self._job)
        for step in self._workflow.steps.values():
            if self._sites[step.name] == self._this_site:
                # check that we can access the step's inputs
                for inp_name, inp_src in step.inputs.items():
                    inp_id = '{}.{}'.format(step.name, inp_name)
                    inp_perms = perms[inp_id]
                    if not self._policy_evaluator.may_access(
                            inp_perms, self._this_site):
                        return False
                    # check that the site we'll download this input
                    # from may access it
                    if '.' in inp_src:
                        src_step, _ = inp_src.split('.')
                        src_site = self._sites[src_step]
                    else:
                        inp_asset_id = self._job.inputs[inp_src]
                        src_site = self._plan.input_sites[inp_asset_id]

                    if not self._policy_evaluator.may_access(
                            perms[inp_src], src_site):
                        return False

                # check that we can access the compute asset
                if not self._policy_evaluator.may_access(
                        perms[step.name], self._this_site):
                    return False

                # check that we can access the step's outputs
                for outp_name in step.outputs:
                    outp_id = '{}.{}'.format(step.name, outp_name)
                    outp_perms = perms[outp_id]
                    if not self._policy_evaluator.may_access(
                            outp_perms, self._this_site):
                        return False

        return True

    def _get_step_inputs(
            self, step: WorkflowStep, keys: Dict[str, str]
            ) -> Optional[Dict[str, Any]]:
        """Find and obtain inputs for the steps.

        If all inputs are available, returns a dictionary mapping their
        keys to their values. If at least one input is not yet
        available, returns None.

        Args:
            step: The step to obtain inputs for.
            keys: Keys for the workflow's items.

        Return:
            A dictionary keyed by output name with corresponding
            values.

        """
        step_input_data = dict()
        for inp_name, inp_source in step.inputs.items():
            source_site, data_key = self._source(inp_source, keys)
            logger.info('Job at {} getting input {} from site {}'.format(
                self._this_site, data_key, source_site))
            try:
                asset = self._site_rest_client.retrieve_asset(
                        source_site, data_key)
                step_input_data[inp_name] = asset.data
                logger.info('Job at {} found input {} available.'.format(
                    self._this_site, data_key))
                logger.info('Metadata: {}'.format(asset.metadata))
            except KeyError:
                logger.info(f'Job at {self._this_site} found input'
                            f' {data_key} not yet available.')
                return None

        return step_input_data

    def _retrieve_compute_asset(self, compute_asset_id: str) -> ComputeAsset:
        site_name = self._registry_client.get_asset_location(compute_asset_id)
        asset = self._site_rest_client.retrieve_asset(
                site_name, compute_asset_id)
        if not isinstance(asset, ComputeAsset):
            raise TypeError('Expecting a compute asset in workflow')
        return asset

    def _source(
            self, inp_source: str, keys: Dict[str, str]) -> Tuple[str, str]:
        """Extracts the source from a source description.

        If the input is of the form 'step.output', this will return the
        target site which is to execute that step according to the
        current plan, and the output name.

        If the input is of the form 'site:data', this will return the
        corresponding site from the plan and the name of the input
        data asset to get from there.

        Args:
            inp_source: Source description as above.
            keys: Keys for the workflow's items.

        """
        if '.' in inp_source:
            step_name, output_name = inp_source.split('.')
            return self._sites[step_name], keys[inp_source]
        else:
            dataset = self._inputs[inp_source]
            return self._plan.input_sites[dataset], dataset


class StepRunner(IStepRunner):
    """A service for running steps of a workflow at a given site."""
    def __init__(
            self, site: str,
            registry_client: RegistryClient,
            site_rest_client: SiteRestClient,
            policy_evaluator: PolicyEvaluator,
            target_store: AssetStore) -> None:
        """Creates a StepRunner.

        Args:
            site: Name of the site this runner is located at.
            registry_client: A RegistryClient to use.
            site_rest_client: A SiteRestClient to use.
            policy_evaluator: A PolicyEvaluator to use.
            target_store: An AssetStore to store result in.

        """
        self._site = site
        self._registry_client = registry_client
        self._site_rest_client = site_rest_client
        self._policy_evaluator = policy_evaluator
        self._target_store = target_store

    def execute_job(self, submission: JobSubmission) -> None:
        """Start a job in a separate thread.

        Args:
            submission: The job to execute and plan to do it.

        """
        run = JobRun(
                self._policy_evaluator, self._site,
                self._registry_client, self._site_rest_client,
                submission,
                self._target_store)
        run.start()
