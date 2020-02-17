from copy import copy
from time import sleep
from typing import Any, Dict, Generator, List, Set

from ddm_client import DDMClient
from definitions import Plan
from policy import PolicyManager
from workflow import Workflow, WorkflowStep


class WorkflowPlanner:
    """Plans workflow execution across sites in DDM.
    """
    def __init__(
            self, ddm_client: DDMClient, policy_manager: PolicyManager
            ) -> None:
        """Create a GlobalWorkflowRunner.

        Args:
            policy_manager: Component that knows about policies.
        """
        self._ddm_client = ddm_client
        self._policy_manager = policy_manager

    def make_plans(
            self, submitter: str,
            workflow: Workflow, inputs: Dict[str, str]
            ) -> List[Plan]:
        """Assigns a site to each workflow step.

        Uses the given result collections to determine where steps can
        be executed.

        Args:
            submitter: Name of the party which submitted this, and to
                    which results should be returned.
            workflow: The workflow to plan.
            result_collections: List of sets of collections
                    representing access permissions for previously
                    computed results, indexed by result id.

        Returns:
            A list of plans, each consisting of a list of sites
            corresponding to the given list of steps.
        """
        def may_access_step(
                result_collections: Dict[str, List[Set[str]]],
                step: WorkflowStep, party: str
                ) -> bool:
            step_key = 'steps.{}'.format(step.name)
            asset_coll = result_collections[step_key]
            return self._policy_manager.may_access(asset_coll, party)

        def may_run(
                result_collections: Dict[str, List[Set[str]]],
                step: WorkflowStep, runner: str
                ) -> bool:
            """Checks whether the given runner may run the given step.
            """
            party = self._ddm_client.get_runner_administrator(runner)

            # check each input
            for inp_name in step.inputs:
                inp_key = 'steps.{}.inputs.{}'.format(step.name, inp_name)
                asset_coll = result_collections[inp_key]
                if not self._policy_manager.may_access(asset_coll, party):
                    return False

            # check step itself (i.e. outputs)
            return may_access_step(result_collections, step, party)

        result_collections = self._find_result_collections(workflow, inputs)
        sorted_steps = self._sort_workflow(workflow)
        plan = [''] * len(sorted_steps)

        def plan_from(cur_step_idx: int) -> Generator[List[str], None, None]:
            """Make remaining plan, starting at cur_step.

            Yields any complete plans found.
            """
            cur_step = sorted_steps[cur_step_idx]
            step_name = cur_step.name
            for runner in self._ddm_client.list_runners():
                if may_run(result_collections, cur_step, runner):
                    plan[cur_step_idx] = runner
                    if cur_step_idx == len(plan) - 1:
                        if may_access_step(
                                result_collections, cur_step, submitter):
                            yield copy(plan)
                    else:
                        yield from plan_from(cur_step_idx + 1)

        return [dict(zip(sorted_steps, plan)) for plan in plan_from(0)]

    def _sort_workflow(self, workflow: Workflow) -> List[WorkflowStep]:
        """Sorts the workflow's steps topologically.

        In the returned list, each step is preceded by the ones it
        depends on.
        """
        deps = dict()       # type: Dict[WorkflowStep, List[WorkflowStep]]
        for step in workflow.steps.values():
            step_deps = list()   # type: List[WorkflowStep]
            for name, ref in step.inputs.items():
                if '/' in ref:
                    dep_name = ref.split('/')[0]
                    step_deps.append(workflow.steps[dep_name])
            deps[step] = step_deps

        result = list()     # type: List[WorkflowStep]
        while len(result) < len(workflow.steps):
            for step in workflow.steps.values():
                if step in result:
                    continue
                if all([dep in result for dep in deps[step]]):
                    result.append(step)
                    break
        return result

    def _find_result_collections(
            self,
            workflow: Workflow,
            inputs: Dict[str, str]
            ) -> Dict[str, List[Set[str]]]:
        """Finds collections each workflow value is in.

        This function returns a dictionary with a list of sets of
        assets for each workflow input, workflow output, step input,
        step, and step output. The keys are as follows:

        - inputs.<name> for a workflow input
        - outputs.<name> for a workflow output
        - steps.<name> for a workflow step
        - steps.<name>.inputs.<name> for a step input
        - steps.<name>.outputs.<name> for a step output

        Args:
            workflow: The workflow to evaluate
            inputs: Map from input names to assets.

        Returns:
            A dictionary with lists of sets of assets per workflow
                    value.
        """
        def source_key(inp_source: str) -> str:
            """Converts a source description to a key.
            """
            if '/' in inp_source:
                return 'steps.{}.outputs.{}'.format(*inp_source.split('/'))
            else:
                return 'inputs.{}'.format(inp_source)

        def set_workflow_inputs_requirements(
                requirements: Dict[str, List[Set[str]]],
                workflow: Workflow
                ) -> None:
            """Returns asset collections for the workflow's inputs.
            """
            for inp_name in workflow.inputs:
                inp_source = inputs[inp_name]
                inp_key = source_key(inp_name)
                requirements[inp_key] = [
                        self._policy_manager.equivalent_assets(inp_source)]

        class InputNotAvailable(RuntimeError):
            pass

        def prop_input_sources(
                requirements: Dict[str, List[Set[str]]],
                step: WorkflowStep
                ) -> None:
            """Propagates requirements of a step input from its source.

            Raises InputNotAvailable if the input source is not (yet) available.
            """
            for inp, inp_source in step.inputs.items():
                inp_key = '{}.inputs.{}'.format(step_key, inp)
                if inp_key not in requirements:
                    inp_source_key = source_key(inp_source)
                    if inp_source_key not in requirements:
                        raise InputNotAvailable()
                    requirements[inp_key] = requirements[inp_source_key]

        def calc_step_input_requirements(
                requirements: Dict[str, List[Set[str]]],
                step: WorkflowStep, inp: str
                ) -> List[Set[str]]:
            """Calculates requirements for the step from those of its input.
            """
            result = list()     # type: List[Set[str]]
            inp_key = 'steps.{}.inputs.{}'.format(step.name, inp)
            for asset_set in requirements[inp_key]:
                rules = self._policy_manager.resultofin_rules(
                        asset_set, step.compute_asset)
                step_assets = set()     # type: Set[str]
                for rule in rules:
                    step_assets |= self._policy_manager.equivalent_assets(
                            rule.collection)
                result.append(step_assets)
            return result

        def calc_step_requirements(
                requirements: Dict[str, List[Set[str]]],
                step: WorkflowStep
                ) -> None:
            """Derives the step's requirements and stores them.
            """
            step_requirements = list()   # type: List[Set[str]]
            for inp, inp_source in step.inputs.items():
                step_requirements.extend(calc_step_input_requirements(
                    requirements, step, inp))

            step_key = 'steps.{}'.format(step.name)
            requirements[step_key] = step_requirements

        def prop_step_outputs(
                requirements: Dict[str, List[Set[str]]],
                step: WorkflowStep
                ) -> None:
            """Copies step requirements to its outputs.
            """
            step_key = 'steps.{}'.format(step.name)
            for output in step.outputs:
                output_key = '{}.outputs.{}'.format(step_key, output)
                requirements[output_key] = requirements[step_key]

        def set_workflow_output_requirements(
                requirements: Dict[str, List[Set[str]]],
                workflow: Workflow
                ) -> None:
            """Copies workflow output requirements from their sources.
            """
            for name, source in workflow.outputs.items():
                output_key = 'outputs.{}'.format(name)
                requirements[output_key] = requirements[source_key(source)]

        # Main function
        requirements = dict()    # type: Dict[str, List[Set[str]]]
        set_workflow_inputs_requirements(requirements, workflow)

        steps_done = set()  # type: Set[str]
        while len(steps_done) < len(workflow.steps):
            for step in workflow.steps.values():
                step_key = 'steps.{}'.format(step.name)
                if step_key not in requirements:
                    try:
                        prop_input_sources(requirements, step)
                    except InputNotAvailable:
                        continue

                    calc_step_requirements(requirements, step)
                    prop_step_outputs(requirements, step)

                    steps_done.add(step_key)

        set_workflow_output_requirements(requirements, workflow)

        return requirements


class WorkflowExecutor:
    """Executes workflows across sites in DDM.
    """
    def __init__(self, ddm_client: DDMClient) -> None:
        """Create a WorkflowExecutor.

        Args:
            ddm_client: A client for connecting to other sites.
        """
        self._ddm_client = ddm_client

    def execute_workflow(
            self, workflow: Workflow, inputs: Dict[str, str], plan: Plan
            ) -> Dict[str, Any]:
        """Executes the given workflow execution plan.

        Args:
            workflow: The workflow to execute.
            inputs: A dictionary mapping the workflow's input
                    parameters to references to data sets.
            plan: The plan according to which to execute the workflow.

        Returns:
            A dictionary of results, indexed by workflow output name.
        """
        # launch all the local runners
        selected_runner_names = set(plan.values())
        for runner_name in selected_runner_names:
            self._ddm_client.execute_plan(
                    runner_name, workflow, inputs, plan)

        # get workflow outputs whenever they're available
        results = dict()    # type: Dict[str, Any]
        while len(results) < len(workflow.outputs):
            for wf_outp_name, wf_outp_source in workflow.outputs.items():
                if wf_outp_name not in results:
                    src_step_name, src_step_output = wf_outp_source.split('/')
                    src_runner_name = plan[workflow.steps[src_step_name]]
                    src_store = self._ddm_client.get_target_store(
                            src_runner_name)
                    outp_key = 'steps.{}.outputs.{}'.format(
                            src_step_name, src_step_output)
                    try:
                        results[wf_outp_name] = self._ddm_client.retrieve_data(
                                src_store, outp_key)
                    except KeyError:
                        continue
            sleep(0.5)

        return results


class GlobalWorkflowRunner:
    """Plans and runs workflows across sites in DDM.
    """
    def __init__(
            self, policy_manager: PolicyManager, ddm_client: DDMClient
            ) -> None:
        """Create a GlobalWorkflowRunner.

        Args:
            policy_manager: Component that knows about policies.
        """
        self._planner = WorkflowPlanner(ddm_client, policy_manager)
        self._executor = WorkflowExecutor(ddm_client)
        self._ddm_client = ddm_client

    def execute(
            self, submitter: str, workflow: Workflow, inputs: Dict[str, str]
            ) -> Dict[str, Any]:
        """Plans and executes the given workflow.

        Args:
            submitter: Name of the party to submit this request.
            workflow: The workflow to execute.
            inputs: A dictionary mapping the workflow's input
                    parameters to references to data sets.
        """
        plans = self._planner.make_plans(submitter, workflow, inputs)
        print('Plans:')
        for plan in plans:
            for step, site in plan.items():
                print('    {} -> {}'.format(step.name, site))
            print()
        print()

        if not plans:
            raise RuntimeError(
                    'This workflow cannot be run due to insufficient'
                    ' permissions.')

        selected_plan = plans[-1]
        results = self._executor.execute_workflow(
                workflow, inputs, selected_plan)
        return results
