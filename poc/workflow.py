from hashlib import sha256
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

        self._validate()

    def __repr__(self) -> str:
        return 'Step("{}", {} -> {} -> {})'.format(
                self.name, self.inputs, self.compute_asset, self.outputs)

    def _validate(self) -> None:
        """Validates the step.

        This checks that inputs and outputs have unique names.

        Raises:
            RuntimeError: if the step is invalid.
        """
        all_names = list(self.inputs) + self.outputs
        for i, name1 in enumerate(all_names):
            for j, name2 in enumerate(all_names):
                if i != j and name1 == name2:
                    raise RuntimeError((
                        'Duplicate name {} for workflow step {}'
                        ' inputs/outputs').format(name1, self.name))

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

        self._validate()

    def __str__(self) -> str:
        steps = ''
        for step in self.steps.values():
            steps += '    {}\n'.format(step)
        return 'Workflow({} -> {}:\n{})'.format(
                self.inputs, self.outputs, steps)

    def __repr__(self) -> str:
        return 'Workflow({}, {}, {})'.format(
                self.inputs, self.steps, self.outputs)

    def _validate(self) -> None:
        """Validates this workflow.

        This checks that workflow inputs and steps have unique names.

        Raises:
            RuntimeError: If a duplicate name is detected.
        """
        # TODO: check internal consistency
        # every step input must match a workflow input or a step output
        # every output must match a workflow input or a step output

        all_names = self.inputs + list(self.steps) + list(self.outputs)
        for i, name1 in enumerate(all_names):
            for j, name2 in enumerate(all_names):
                if i != j and name1 == name2:
                    raise RuntimeError((
                        'Duplicate name {} among workflow steps, inputs'
                        ' and outputs').format(name1))


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
                for inp in step.inputs.values():
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
        inputs_selected |= new_inputs
        while new_steps:
            steps_done |= new_steps
            new_steps, new_inputs = predecessors(steps_done)
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

    @staticmethod
    def niljob(key: str) -> 'Job':
        """Returns a zero-step job for a dataset.

        Args:
            key: The key identifying the dataset.

        The job will have no steps, and a single input named `dataset`.
        """
        return Job(Workflow(['dataset'], {}, []), {'dataset': key})

    def subjob(
            self, step: WorkflowStep) -> 'Job':
        """Returns a minimal job for a given step.

        This returns a new Job object containing a minimal Workflow to
        calculate the given step's outputs and the required subset of
        job inputs.

        Args:
            step: The step to get a subjob for.
        """
        sub_wf = self.workflow.subworkflow(step)
        inputs = {
                wf_inp: asset
                for wf_inp, asset in self.inputs.items()
                if wf_inp in sub_wf.inputs}
        return Job(sub_wf, inputs)

    def keys(self) -> Dict[str, str]:
        """Calculates keys (hashes) of all items in the job's workflow.

        Returns:
            A dict mapping workflow items to their key.
        """
        class DependencyMissing(RuntimeError):
            pass

        def prop_input_keys(item_keys: Dict[str, str]) -> None:
            for inp_name, inp_src in self.inputs.items():
                inp_hash = sha256()
                inp_hash.update(inp_src.encode('utf-8'))
                item_keys[inp_name] = inp_hash.hexdigest()

        def prop_input_sources(
                item_keys: Dict[str, str], step: WorkflowStep) -> None:
            for inp_name, inp_src in step.inputs.items():
                inp_item = '{}/{}'.format(step.name, inp_name)
                if inp_item not in item_keys:
                    if inp_src not in item_keys:
                        raise DependencyMissing()
                    item_keys[inp_item] = item_keys[inp_src]

        def calc_step_outputs(
                item_keys: Dict[str, str], step: WorkflowStep) -> None:
            step_hash = sha256()
            for inp_name in sorted(step.inputs):
                inp_item = '{}/{}'.format(step.name, inp_name)
                step_hash.update(item_keys[inp_item].encode('utf-8'))
            step_hash.update(step.compute_asset.encode('utf-8'))
            for outp_name in step.outputs:
                outp_item = '{}/{}'.format(step.name, outp_name)
                outp_hash = step_hash.copy()
                outp_hash.update(outp_name.encode('utf-8'))
                item_keys[outp_item] = 'hash:{}'.format(outp_hash.hexdigest())

        def set_workflow_outputs_keys(
                item_keys: Dict[str, str], outputs: Dict[str, str]) -> None:
            for outp_name, outp_src in outputs.items():
                item_keys[outp_name] = item_keys[outp_src]

        item_keys = dict()      # type: Dict[str, str]
        prop_input_keys(item_keys)

        steps_done = set()      # type: Set[str]
        while len(steps_done) < len(self.workflow.steps):
            for step in self.workflow.steps.values():
                if step.name not in steps_done:
                    try:
                        prop_input_sources(item_keys, step)
                        calc_step_outputs(item_keys, step)
                        steps_done.add(step.name)
                    except DependencyMissing:
                        continue

        set_workflow_outputs_keys(item_keys, self.workflow.outputs)
        return item_keys
