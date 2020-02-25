from typing import Dict

from policy import Permissions, PolicyManager
from workflow import Job, Workflow, WorkflowStep


class PolicyEvaluator:
    """Evaluates policies pertaining to a given workflow.
    """
    def __init__(self, policy_manager: PolicyManager) -> None:
        """Create a Policy Evaluator.

        Args:
            policy_manager: The policy manager to use.
        """
        self._policy_manager = policy_manager


    def calculate_permissions(
            self, job: Job) -> Dict[str, Permissions]:
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
            job: The job to evaluate.

        Returns:
            A dictionary with permissions per workflow value.
        """
        def source_key(inp_source: str) -> str:
            """Converts a source description to a key.
            """
            if '/' in inp_source:
                return 'steps.{}.outputs.{}'.format(*inp_source.split('/'))
            else:
                return 'inputs.{}'.format(inp_source)

        def set_workflow_inputs_permissions(
                permissions: Dict[str, Permissions],
                job: Job
                ) -> None:
            """Sets permissions for the workflow's inputs.

            This modifies the permissions argument.
            """
            for inp_name in job.workflow.inputs:
                inp_source = job.inputs[inp_name]
                inp_key = source_key(inp_name)
                permissions[inp_key] = (
                        self._policy_manager.permissions_for_asset(inp_source))

        class InputNotAvailable(RuntimeError):
            pass

        def prop_input_sources(
                permissions: Dict[str, Permissions],
                step: WorkflowStep
                ) -> None:
            """Propagates permissions of a step input from its source.

            This modifies the permissions argument.

            Raises:
                InputNotAvailable if the input source is not (yet)
                available.
            """
            for inp, inp_source in step.inputs.items():
                inp_key = '{}.inputs.{}'.format(step_key, inp)
                if inp_key not in permissions:
                    inp_source_key = source_key(inp_source)
                    if inp_source_key not in permissions:
                        raise InputNotAvailable()
                    permissions[inp_key] = permissions[inp_source_key]

        def calc_step_permissions(
                permissions: Dict[str, Permissions],
                step: WorkflowStep
                ) -> None:
            """Derives the step's permissions and stores them.
            """
            input_perms = list()     # type: List[Permissions]
            for inp in step.inputs:
                inp_key = 'steps.{}.inputs.{}'.format(step.name, inp)
                input_perms.append(permissions[inp_key])

            step_key = 'steps.{}'.format(step.name)
            permissions[step_key] = \
                    self._policy_manager.propagate_permissions(
                            input_perms, step.compute_asset)

        def prop_step_outputs(
                permissions: Dict[str, Permissions],
                step: WorkflowStep
                ) -> None:
            """Copies step permissions to its outputs.

            This modifies the permissions argument.
            """
            step_key = 'steps.{}'.format(step.name)
            for output in step.outputs:
                output_key = '{}.outputs.{}'.format(step_key, output)
                permissions[output_key] = permissions[step_key]

        def set_workflow_outputs_permissions(
                permissions: Dict[str, Permissions],
                workflow: Workflow
                ) -> None:
            """Copies workflow output permissions from their sources.
            """
            for name, source in workflow.outputs.items():
                output_key = 'outputs.{}'.format(name)
                permissions[output_key] = permissions[source_key(source)]

        # Main function
        permissions = dict()    # type: Dict[str, Permissions]
        set_workflow_inputs_permissions(permissions, job)

        steps_done = set()  # type: Set[str]
        while len(steps_done) < len(job.workflow.steps):
            for step in job.workflow.steps.values():
                step_key = 'steps.{}'.format(step.name)
                if step_key not in steps_done:
                    try:
                        prop_input_sources(permissions, step)
                        calc_step_permissions(permissions, step)
                        prop_step_outputs(permissions, step)
                        steps_done.add(step_key)
                    except InputNotAvailable:
                        continue

        set_workflow_outputs_permissions(permissions, job.workflow)
        return permissions

