"""Supports running DDM-wide workflows."""
import logging
from copy import copy
from time import sleep
from typing import Any, Dict, Generator, List

from mahiru.components.registry_client import RegistryClient
from mahiru.definitions.assets import Asset
from mahiru.definitions.identifier import Identifier
from mahiru.definitions.workflows import (
        ExecutionRequest, Job, Plan, Workflow, WorkflowStep)
from mahiru.policy.evaluation import (
        PermissionCalculator, Permissions, PolicyEvaluator)
from mahiru.rest.site_client import SiteRestClient


logger = logging.getLogger(__name__)


class WorkflowPlanner:
    """Plans workflow execution across sites in a DDM."""
    def __init__(
            self, registry_client: RegistryClient,
            policy_evaluator: PolicyEvaluator
            ) -> None:
        """Create a WorkflowOrchestrator.

        Args:
            registry_client: RegistryClient to get sites from.
            policy_evaluator: PolicyEvaluator to use for permissions.
        """
        self._registry_client = registry_client
        self._policy_evaluator = policy_evaluator
        self._permission_calculator = PermissionCalculator(policy_evaluator)

    def make_plans(
            self, submitting_party: Identifier, submitting_site: Identifier,
            job: Job) -> List[Plan]:
        """Assigns a site to each workflow step.

        Uses the given result collections to determine where steps can
        be executed.

        Args:
            submitting_party: Id of the party which submitted this, and
                    who will use the results.
            submitting_site: Id of the site which submitted this, and to
                    which results should be returned.
            job: The job to plan.

        Returns:
            A list of plans that will execute the workflow.
        """
        permissions = self._permission_calculator.calculate_permissions(job)

        # if we cannot access the outputs, then there are no plans
        for output in job.workflow.outputs:
            output_perms = permissions[output]
            if not self._policy_evaluator.may_access(
                    output_perms, submitting_site):
                return []

        sites = self._registry_client.list_sites_with_runners()
        permitted_sites = self._permission_calculator.permitted_sites(
                job, sites, permissions)
        logger.debug(f'Permitted sites: {permitted_sites}')

        sorted_steps = self._sort_workflow(job.workflow)
        sorted_step_names = [step.name for step in sorted_steps]

        plan = [Identifier('*')] * len(sorted_steps)

        def plan_from(
                cur_step_idx: int) -> Generator[List[Identifier], None, None]:
            """Make remaining plan, starting at cur_step.

            Yields any complete plans found.
            """
            cur_step = sorted_steps[cur_step_idx]
            for site in permitted_sites[cur_step.name]:
                plan[cur_step_idx] = site
                if cur_step_idx == len(plan) - 1:
                    yield plan
                else:
                    yield from plan_from(cur_step_idx + 1)

        return [
                Plan(dict(zip(sorted_step_names, plan)))
                for plan in plan_from(0)]

    def _sort_workflow(self, workflow: Workflow) -> List[WorkflowStep]:
        """Sorts the workflow's steps topologically.

        In the returned list, each step is preceded by the ones it
        depends on.
        """
        # find dependencies for each step
        deps = dict()       # type: Dict[WorkflowStep, List[WorkflowStep]]
        for step in workflow.steps.values():
            step_deps = list()   # type: List[WorkflowStep]
            for name, ref in step.inputs.items():
                if '.' in ref:
                    dep_name = ref.split('.')[0]
                    step_deps.append(workflow.steps[dep_name])
            deps[step] = step_deps

        # sort based on dependencies
        result = list()     # type: List[WorkflowStep]
        while len(result) < len(workflow.steps):
            for step in workflow.steps.values():
                if step in result:
                    continue
                if all([dep in result for dep in deps[step]]):
                    result.append(step)
                    break
        return result


class WorkflowExecutor:
    """Executes workflows across sites in a DDM."""
    def __init__(self, site_rest_client: SiteRestClient) -> None:
        """Create a WorkflowExecutor.

        Args:
            site_rest_client: A client for connecting to other sites.
        """
        self._site_rest_client = site_rest_client

    def start_workflow(self, request: ExecutionRequest) -> None:
        """Starts the given workflow execution request.

        This sends requests to all the sites at which the request is to
        be run to start executing the request.

        Args:
            request: The job and plan to execute.

        Returns:
            A dictionary of results, indexed by workflow output name.
        """
        for site_id in set(request.plan.step_sites.values()):
            self._site_rest_client.submit_request(site_id, request)

    def is_done(self, request: ExecutionRequest) -> bool:
        """Checks whether a request is done.

        Returns:
            True iff the request is done.
        """
        wf = request.job.workflow
        id_hashes = request.job.id_hashes()
        for wf_outp_name, wf_outp_source in wf.outputs.items():
            src_step_name, src_step_output = wf_outp_source.split('.')
            src_site = request.plan.step_sites[src_step_name]
            outp_id_hash = id_hashes[wf_outp_name]
            asset_id = Identifier.from_id_hash(outp_id_hash)
            try:
                self._site_rest_client.retrieve_asset(src_site, asset_id)
            except KeyError:
                return False
        return True

    def get_results(self, request: ExecutionRequest) -> Dict[str, Asset]:
        """Downloads the results of a completed request.

        This blocks until all results are available.

        Args:
            request: The job that was submitted.

        Returns:
            A dictionary of results, indexed by workflow output name.
        """
        wf = request.job.workflow
        id_hashes = request.job.id_hashes()
        results = dict()    # type: Dict[str, Any]
        while len(results) < len(wf.outputs):
            for wf_outp_name, wf_outp_source in wf.outputs.items():
                if wf_outp_name not in results:
                    src_step_name, src_step_output = wf_outp_source.split('.')
                    src_site = request.plan.step_sites[src_step_name]
                    outp_id_hash = id_hashes[wf_outp_name]
                    try:
                        asset_id = Identifier.from_id_hash(outp_id_hash)
                        asset = self._site_rest_client.retrieve_asset(
                                src_site, asset_id)
                        results[wf_outp_name] = asset
                    except KeyError:
                        continue
            sleep(5)

        return results


class WorkflowOrchestrator:
    """Plans and runs workflows across sites in DDM.

    Keeps track of jobs by an id, which is a URL-safe string.

    """
    def __init__(
            self, policy_evaluator: PolicyEvaluator,
            registry_client: RegistryClient, site_rest_client: SiteRestClient
            ) -> None:
        """Create a WorkflowOrchestrator.

        Args:
            policy_evaluator: Component that knows about policies.
            registry_client: Client for accessing the registry.
            site_rest_client: Client for accessing other sites.
        """
        self._planner = WorkflowPlanner(registry_client, policy_evaluator)
        self._executor = WorkflowExecutor(site_rest_client)
        self._next_id = 1
        self._jobs = dict()     # type: Dict[str, ExecutionRequest]
        self._results = dict()  # type: Dict[str, Dict[str, Any]]

    def start_job(
            self, submitting_party: Identifier, submitting_site: Identifier,
            job: Job) -> str:
        """Plans and executes the given workflow.

        Args:
            submitting_party: The party which submitted this job.
            submitting_site: The site which submitted this job.
            job: The job to execute.

        Return:
            A string containing the new job's id.
        """
        plans = self._planner.make_plans(
                submitting_party, submitting_site, job)
        if not plans:
            logger.warning('No plans!')
            raise RuntimeError(
                    'This workflow cannot be run due to insufficient'
                    ' permissions.')
        for i, plan in enumerate(plans):
            logger.info(f'Plan {i}: {plan}')
        selected_plan = plans[-1]
        request = ExecutionRequest(job, selected_plan)
        self._executor.start_workflow(request)

        job_id = str(self._next_id)
        self._next_id += 1
        self._jobs[job_id] = request
        return job_id

    def get_submitted_job(self, job_id: str) -> Job:
        """Returns the submitted job with the given id.

        Args:
            job_id: The id of the job to retrieve.

        Returns:
            The JobSubmission with that id.

        Raises:
            KeyError: If no job with this id was found.

        """
        return self._jobs[job_id].job

    def get_plan(self, job_id: str) -> Plan:
        """Returns the plan used to execute the given job.

        Args:
            job_id: The id of the job to get the plan for.

        Returns:
            The Plan for the job with that id.

        Raises:
            KeyError: If no job with this id was found.

        """
        return self._jobs[job_id].plan

    def is_done(self, job_id: str) -> bool:
        """Checks whether the given job is done.

        Args:
            job_id: The id of the job to check.

        Returns:
            True iff the job is done.

        Raises:
            KeyError: If the job id does not exist.
        """
        return self._executor.is_done(self._jobs[job_id])

    def get_results(self, job_id: str) -> Dict[str, Asset]:
        """Returns results of a completed job.

        This blocks until the results are available. You can check if
        that is the case using :func:`is_done`.

        Args:
            job_id: The id of the job to check.

        Returns:
            The resulting assets, indexed by output name.

        Raises:
            KeyError: If the job id does not exist.
        """
        return self._executor.get_results(self._jobs[job_id])
