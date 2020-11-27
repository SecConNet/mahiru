"""Supports running DDM-wide workflows."""
import logging
from copy import copy
from time import sleep
from typing import Any, Dict, Generator, List

from proof_of_concept.components.registry_client import RegistryClient
from proof_of_concept.definitions.asset_id import AssetId
from proof_of_concept.definitions.workflows import (
        Job, JobSubmission, Plan, Workflow, WorkflowStep)
from proof_of_concept.policy.evaluation import (
        PermissionCalculator, Permissions, PolicyEvaluator)
from proof_of_concept.rest.client import SiteRestClient


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
            self, submitter: str, job: Job) -> List[Plan]:
        """Assigns a site to each workflow step.

        Uses the given result collections to determine where steps can
        be executed.

        Args:
            submitter: Name of the site which submitted this, and to
                    which results should be returned.
            job: The job to plan.

        Returns:
            A list of plans that will execute the workflow.
        """
        def may_run(
                permissions: Dict[str, Permissions],
                step: WorkflowStep, site: str
                ) -> bool:
            """Check whether the given site may run the given step."""
            # check each input
            for inp_name in step.inputs:
                inp_key = '{}.{}'.format(step.name, inp_name)
                inp_perms = permissions[inp_key]
                if not self._policy_evaluator.may_access(inp_perms, site):
                    return False

            # check step itself (i.e. compute asset)
            step_perms = permissions[step.name]
            if not self._policy_evaluator.may_access(step_perms, site):
                return False

            # check each output
            for outp_name in step.outputs:
                outp_key = '{}.{}'.format(step.name, outp_name)
                outp_perms = permissions[outp_key]
                if not self._policy_evaluator.may_access(outp_perms, site):
                    return False

            return True

        permissions = self._permission_calculator.calculate_permissions(job)

        for output in job.workflow.outputs:
            output_perms = permissions[output]
            if not self._policy_evaluator.may_access(output_perms, submitter):
                return []

        sorted_steps = self._sort_workflow(job.workflow)
        plan = [''] * len(sorted_steps)

        def plan_from(cur_step_idx: int) -> Generator[List[str], None, None]:
            """Make remaining plan, starting at cur_step.

            Yields any complete plans found.
            """
            cur_step = sorted_steps[cur_step_idx]
            step_perms = permissions[cur_step.name]
            for site in self._registry_client.list_sites_with_runners():
                if may_run(permissions, cur_step, site):
                    plan[cur_step_idx] = site
                    if cur_step_idx == len(plan) - 1:
                        yield copy(plan)
                    else:
                        yield from plan_from(cur_step_idx + 1)

        sorted_step_names = [step.name for step in sorted_steps]

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

    def execute_workflow(self, submission: JobSubmission) -> Dict[str, Any]:
        """Executes the given workflow execution plan.

        Args:
            submission: The job and plan to execute.

        Returns:
            A dictionary of results, indexed by workflow output name.
        """
        # launch all the runners
        for site_name in set(submission.plan.step_sites.values()):
            self._site_rest_client.submit_job(site_name, submission)

        # get workflow outputs whenever they're available
        wf = submission.job.workflow
        keys = submission.job.keys()
        results = dict()    # type: Dict[str, Any]
        while len(results) < len(wf.outputs):
            for wf_outp_name, wf_outp_source in wf.outputs.items():
                if wf_outp_name not in results:
                    src_step_name, src_step_output = wf_outp_source.split('.')
                    src_site = submission.plan.step_sites[src_step_name]
                    outp_key = keys[wf_outp_name]
                    try:
                        asset_id = AssetId.from_key(outp_key)
                        asset = self._site_rest_client.retrieve_asset(
                                src_site, asset_id)
                        results[wf_outp_name] = asset.data
                    except KeyError:
                        continue
            sleep(0.5)

        return results


class WorkflowOrchestrator:
    """Plans and runs workflows across sites in DDM."""
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

    def execute(
            self, submitter: str, job: Job) -> Dict[str, Any]:
        """Plans and executes the given workflow.

        Args:
            submitter: Name of the site to submit this job.
            job: The job to execute.
        """
        plans = self._planner.make_plans(submitter, job)
        if not plans:
            raise RuntimeError(
                    'This workflow cannot be run due to insufficient'
                    ' permissions.')
        for i, plan in enumerate(plans):
            logger.info(f'Plan {i}: {plan}')
        selected_plan = plans[-1]
        submission = JobSubmission(job, selected_plan)
        results = self._executor.execute_workflow(submission)
        return results
