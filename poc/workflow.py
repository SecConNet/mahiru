from typing import Dict, List, Set, Tuple


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

    def subworkflow(self, step: WorkflowStep) -> 'Workflow':
        """Returns a minimal subworkflow that creates the given step.

        This returns a subworkflow of the current workflow which
        contains only the steps that are direct or indirect
        predecessors of the given step, plus that step itself.

        The resulting workflow will not have any outputs.

        Args:
            step: Final step in the subworkflow.
        """
        def predecessors(
                steps_done: Set[WorkflowStep]
                ) -> Tuple[Set[WorkflowStep], Set[str]]:
            """Returns predecessors of steps_done.

            In particular, this returns the direct precedessors of any
            step in steps_done which are not in steps_done already.

            Args:
                steps_done: A set of already-processed steps.

            Returns:
                (predecessors, inputs), where predecessors are
                        predecessor steps, and inputs are workflow
                        input names referenced from steps_done.
            """
            preds = set()   # type: Set[WorkflowStep]
            inps = set()    # type: Set[str]
            for step in steps_done:
                print('checking step {}'.format(step))
                for inp in step.inputs.values():
                    print('checking input {}'.format(inp))
                    if '/' in inp:
                        pred_name = inp.split('/')[0]
                        pred = self.steps[pred_name]
                        if pred not in steps_done:
                            preds.add(pred)
                    else:
                        inps.add(inp)
            return preds, inps

        steps_done = {step}
        inputs_selected = set()     # type: Set[str]

        new_steps, new_inputs = predecessors(steps_done)
        print('new_steps: {}'.format(new_steps))
        inputs_selected |= new_inputs
        while new_steps:
            steps_done |= new_steps
            new_steps, new_inputs = predecessors(steps_done)
            print('new_steps: {}'.format(new_steps))
            inputs_selected |= new_inputs

        return Workflow(list(inputs_selected), {}, list(steps_done))


class Job:
    """Represents a job to the system from a user.

    This class can also be used for provenance, to describe how an
    asset was made.
    """
    def __init__(self, workflow: Workflow, inputs: Dict[str, str]) -> None:
        """Create a job.

        Args:
            workflow: The workflow to run.
            inputs: A dictionary mapping the workflow's input
                    parameters to data set ids.
        """
        self.workflow = workflow
        self.inputs = inputs

    def __repr__(self) -> str:
        return 'Job({}, {})'.format(
                self.inputs, self.workflow)
