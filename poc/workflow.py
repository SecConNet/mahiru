from typing import Dict, List


class WorkflowStep:
    """Defines a workflow step.
    """
    def __init__(
            self, name: str,
            inputs: Dict[str, str], outputs: List[str],
            compute_asset: str
            ) -> None:
        """Create a WorkflowStep.

        Args:
            name: Name of this step.
            inputs: Dict mapping input parameter names to references to
                    their sources, either the name of a workflow input,
                    or of the form other_step/output_name.
            outputs: List of names of outputs produced.
            compute_asset: Name of the compute asset to use.
        """
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.compute_asset = compute_asset

    def __repr__(self) -> str:
        return 'Step("{}", {} -> {} -> {})'.format(
                self.name, self.inputs, self.compute_asset, self.outputs)

    def execute(self, args: Dict[str, int]) -> Dict[str, int]:
        pass


class Workflow:
    """Defines a workflow.
    """
    def __init__(
            self, inputs: List[str], outputs: Dict[str, str],
            steps: List[WorkflowStep]
            ) -> None:
        """Create a workflow.

        Args:
            inputs: List of input parameter names.
            outputs: Dict mapping output parameter names to
                    corresponding step outputs of the form step/output.
            steps: Dict of steps comprising this workflow, indexed by
                    step name.
        """
        self.inputs = inputs
        self.outputs = outputs
        self.steps = dict()     # type: Dict[str, WorkflowStep]

        for step in steps:
            self.steps[step.name] = step

        # TODO: check validity
        # every step input must match a workflow input or a step output
        # every output must match a workflow input or a step output

    def __str__(self) -> str:
        steps = ''
        for step in self.steps.values():
            steps += '    {}\n'.format(step)
        return 'Workflow({} -> {}:\n{})'.format(
                self.inputs, self.outputs, steps)

    def __repr__(self) -> str:
        return 'Workflow({}, {}, {})'.format(
                self.inputs, self.steps, self.outputs)
