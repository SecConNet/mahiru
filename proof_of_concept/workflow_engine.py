"""Classes for running DDM-wide workflows."""
from copy import copy
from time import sleep
from typing import Any, Dict, Generator, List, Set

from proof_of_concept.ddm_client import DDMClient
from proof_of_concept.definitions import Plan
from proof_of_concept.policy import Permissions, PolicyManager
from proof_of_concept.policy_evaluator import PolicyEvaluator
from proof_of_concept.workflow import Job, Workflow, WorkflowStep


class WorkflowPlanner:
    """Plans workflow execution across sites in a DDM."""
    def __init__(
            self, ddm_client: DDMClient, policy_manager: PolicyManager
            ) -> None:
        """Create a GlobalWorkflowRunner.

        Args:
            ddm_client: Component that communicates with the DDM.
            policy_manager: Component that knows about policies.
        """
        self._ddm_client = ddm_client
        self._policy_manager = policy_manager
        self._policy_evaluator = PolicyEvaluator(policy_manager)

    def make_plans(
            self, submitter: str, job: Job) -> List[Plan]:
        """Assigns a site to each workflow step.

        Uses the given result collections to determine where steps can
        be executed.

        Args:
            submitter: Name of the party which submitted this, and to
                    which results should be returned.
            job: The job to plan.

        Returns:
            A list of plans that will execute the workflow.
        """
        def may_run(
                permissions: Dict[str, Permissions],
                step: WorkflowStep, runner: str
                ) -> bool:
            """Check whether the given runner may run the given step."""
            party = self._ddm_client.get_runner_administrator(runner)

            # check each input
            for inp_name in step.inputs:
                inp_key = '{}.{}'.format(step.name, inp_name)
                inp_perms = permissions[inp_key]
                if not self._policy_manager.may_access(inp_perms, party):
                    return False

            # check step itself (i.e. outputs)
            step_perms = permissions[step.name]
            return self._policy_manager.may_access(step_perms, party)

        permissions = self._policy_evaluator.calculate_permissions(job)
        sorted_steps = self._sort_workflow(job.workflow)
        plan = [''] * len(sorted_steps)

        def plan_from(cur_step_idx: int) -> Generator[List[str], None, None]:
            """Make remaining plan, starting at cur_step.

            Yields any complete plans found.
            """
            cur_step = sorted_steps[cur_step_idx]
            step_perms = permissions[cur_step.name]
            for runner in self._ddm_client.list_runners():
                if may_run(permissions, cur_step, runner):
                    plan[cur_step_idx] = runner
                    if cur_step_idx == len(plan) - 1:
                        if self._policy_manager.may_access(
                                step_perms, submitter):
                            yield copy(plan)
                    else:
                        yield from plan_from(cur_step_idx + 1)

        step_runners = [dict(zip(sorted_steps, plan)) for plan in plan_from(0)]
        # We'll have some other kind of resolver here later
        input_stores = {
                inp: inp.split(':')[1].split('/')[0]
                for inp in job.inputs.values()}

        return [Plan(input_stores, runners) for runners in step_runners]

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
    def __init__(self, ddm_client: DDMClient) -> None:
        """Create a WorkflowExecutor.

        Args:
            ddm_client: A client for connecting to other sites.
        """
        self._ddm_client = ddm_client

    def execute_workflow(
            self, job: Job, plan: Plan
            ) -> Dict[str, Any]:
        """Executes the given workflow execution plan.

        Args:
            job: The job to execute.
            plan: The plan according to which to execute the workflow.

        Returns:
            A dictionary of results, indexed by workflow output name.
        """
        # launch all the local runners
        selected_runner_names = set(plan.step_runners.values())
        for runner_name in selected_runner_names:
            self._ddm_client.submit_job(runner_name, job, plan)

        # get workflow outputs whenever they're available
        wf = job.workflow
        keys = job.keys()
        results = dict()    # type: Dict[str, Any]
        while len(results) < len(wf.outputs):
            for wf_outp_name, wf_outp_source in wf.outputs.items():
                if wf_outp_name not in results:
                    src_step_name, src_step_output = wf_outp_source.split('.')
                    src_runner_name = plan.step_runners[
                            wf.steps[src_step_name]]
                    src_store = self._ddm_client.get_target_store(
                            src_runner_name)
                    outp_key = keys[wf_outp_name]
                    try:
                        results[wf_outp_name], _ = (
                                self._ddm_client.retrieve_data(
                                    src_store, outp_key))
                    except KeyError:
                        continue
            sleep(0.5)

        return results


class GlobalWorkflowRunner:
    """Plans and runs workflows across sites in DDM."""
    def __init__(
            self, policy_manager: PolicyManager, ddm_client: DDMClient
            ) -> None:
        """Create a GlobalWorkflowRunner.

        Args:
            policy_manager: Component that knows about policies.
            ddm_client: Client for accessing other sites.
        """
        self._planner = WorkflowPlanner(ddm_client, policy_manager)
        self._executor = WorkflowExecutor(ddm_client)
        self._ddm_client = ddm_client

    def execute(
            self, submitter: str, job: Job) -> Dict[str, Any]:
        """Plans and executes the given workflow.

        Args:
            submitter: Name of the party to submit this job.
            job: The job to execute.
        """
        plans = self._planner.make_plans(submitter, job)
        print('Plans:')
        for plan in plans:
            print(plan)
        print()

        if not plans:
            raise RuntimeError(
                    'This workflow cannot be run due to insufficient'
                    ' permissions.')

        selected_plan = plans[-1]
        results = self._executor.execute_workflow(job, selected_plan)
        return results