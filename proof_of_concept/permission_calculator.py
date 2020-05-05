"""Components for calculating workflow permissions."""
from typing import Dict, List, Set

from proof_of_concept.policy import Permissions, PolicyEvaluator
from proof_of_concept.workflow import Job, Workflow, WorkflowStep


class PermissionCalculator:
    """Evaluates policies pertaining to a given workflow."""
    def __init__(self, policy_evaluator: PolicyEvaluator) -> None:
        """Create a Policy Evaluator.

        Args:
            policy_evaluator: The policy evaluator to use.
        """
        self._policy_evaluator = policy_evaluator

    def calculate_permissions(
            self, job: Job) -> Dict[str, Permissions]:
        """Finds collections each workflow value is in.

        This function returns a dictionary with a list of sets of
        assets for each workflow input, workflow output, step input,
        step, and step output. Workflow inputs are keyed by their
        name, step inputs and outputs by <step>.<name>.

        Args:
            job: The job to evaluate.

        Returns:
            A dictionary with permissions per workflow value.
        """
        def set_input_assets_permissions(
                permissions: Dict[str, Permissions],
                job: Job) -> None:
            """Sets permissions for the job's inputs.

            This modifies the permissions argument.
            """
            for inp_asset in job.inputs.values():
                if inp_asset not in permissions:
                    permissions[inp_asset] = (
                            self._policy_evaluator.permissions_for_asset(
                                inp_asset))

        def prop_workflow_inputs(
                permissions: Dict[str, Permissions],
                job: Job
                ) -> None:
            """Propagates permissions for the workflow's inputs.

            This modifies the permissions argument.
            """
            for inp_name in job.workflow.inputs:
                source_asset = job.inputs[inp_name]
                permissions[inp_name] = permissions[source_asset]

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
                inp_key = '{}.{}'.format(step.name, inp)
                if inp_key not in permissions:
                    if inp_source not in permissions:
                        raise InputNotAvailable()
                    permissions[inp_key] = permissions[inp_source]

        def calc_step_permissions(
                permissions: Dict[str, Permissions],
                step: WorkflowStep
                ) -> None:
            """Derives the step's permissions and stores them.

            These are the permissions needed to access the compute
            asset.
            """
            permissions[step.name] = (
                    self._policy_evaluator.permissions_for_asset(
                        step.compute_asset))

        def prop_step_outputs(
                permissions: Dict[str, Permissions],
                step: WorkflowStep
                ) -> None:
            """Derives step output permissions.

            Propagates permissions from inputs via the step to the
            outputs. This takes into account permissions induced by
            the compute asset via ResultOfComputeIn rules, but not
            access to the compute asset itself.

            This modifies the permissions argument.
            """
            input_perms = list()     # type: List[Permissions]
            for inp in step.inputs:
                inp_key = '{}.{}'.format(step.name, inp)
                input_perms.append(permissions[inp_key])

            perms = self._policy_evaluator.propagate_permissions(
                        input_perms, step.compute_asset)

            for output in step.outputs:
                output_key = '{}.{}'.format(step.name, output)
                permissions[output_key] = perms

        def set_workflow_outputs_permissions(
                permissions: Dict[str, Permissions],
                workflow: Workflow
                ) -> None:
            """Copies workflow output permissions from their sources."""
            for name, source in workflow.outputs.items():
                permissions[name] = permissions[source]

        # Main function
        permissions = dict()    # type: Dict[str, Permissions]
        set_input_assets_permissions(permissions, job)
        prop_workflow_inputs(permissions, job)

        steps_done = set()  # type: Set[str]
        while len(steps_done) < len(job.workflow.steps):
            for step in job.workflow.steps.values():
                if step.name not in steps_done:
                    try:
                        prop_input_sources(permissions, step)
                        calc_step_permissions(permissions, step)
                        prop_step_outputs(permissions, step)
                        steps_done.add(step.name)
                    except InputNotAvailable:
                        continue

        set_workflow_outputs_permissions(permissions, job.workflow)
        return permissions
