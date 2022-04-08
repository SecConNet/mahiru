"""Classes for describing workflows."""
from hashlib import sha256
from typing import Dict, List, Mapping, Optional, Set, Tuple, Union

from mahiru.definitions.identifier import Identifier


class WorkflowStep:
    """Defines a workflow step."""
    def __init__(
            self, name: str,
            inputs: Dict[str, str],
            outputs: Dict[str, Union[None, str, Identifier]],
            compute_asset_id: Union[str, Identifier]
            ) -> None:
        """Create a WorkflowStep.

        Args:
            name: Name of this step.
            inputs: Dict mapping input parameter names to references to
                    their sources, either the name of a workflow input,
                    or of the form other_step.output_name.
            outputs: Dict mapping output parameter names to references
                    to the base assets to use, or None if containers
                    are not used.
            compute_asset_id: The id of the compute asset to use.
        """
        self.name = name
        self.inputs = inputs
        self.outputs = dict()   # type: Dict[str, Optional[Identifier]]

        for name, base_asset in outputs.items():
            if base_asset is None:
                self.outputs[name] = None
            elif not isinstance(base_asset, Identifier):
                self.outputs[name] = Identifier(base_asset)
            else:
                self.outputs[name] = base_asset

        if not isinstance(compute_asset_id, Identifier):
            compute_asset_id = Identifier(compute_asset_id)
        self.compute_asset_id = compute_asset_id

        self._validate()

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return 'Step("{}", {} -> {} -> {})'.format(
                self.name, self.inputs, self.compute_asset_id, self.outputs)

    def _validate(self) -> None:
        """Validates the step.

        This checks that inputs and outputs have unique names.

        Raises:
            RuntimeError: if the step is invalid.
        """
        all_names = list(self.inputs.keys()) + list(self.outputs.keys())
        for i, name1 in enumerate(all_names):
            for j, name2 in enumerate(all_names):
                if i != j and name1 == name2:
                    raise RuntimeError((
                        'Duplicate name {} for workflow step {}'
                        ' inputs.outputs').format(name1, self.name))


class Workflow:
    """Defines a workflow."""
    def __init__(
            self, inputs: List[str], outputs: Dict[str, str],
            steps: List[WorkflowStep]
            ) -> None:
        """Create a workflow.

        Args:
            inputs: List of input parameter names.
            outputs: Dict mapping output parameter names to
                    corresponding step outputs of the form step.output.
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
        """Returns a string representation of the object."""
        steps = ''
        for step in self.steps.values():
            steps += '    {}\n'.format(step)
        return 'Workflow({} -> {}:\n{})'.format(
                self.inputs, self.outputs, steps)

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
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
                    if '.' in inp:
                        pred_name = inp.split('.')[0]
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

    A Job is a workflow together with a set of inputs for it.
    """
    def __init__(
            self, submitter: Identifier, workflow: Workflow,
            inputs: Mapping[str, Union[str, Identifier]]
            ) -> None:
        """Create a job.

        Args:
            submitter: The party submitting this workflow.
            workflow: The workflow to run.
            inputs: A dictionary mapping the workflow's input
                    parameters to data set ids.
        """
        self.submitter = submitter
        self.workflow = workflow
        self.inputs = {
                inp: aid if isinstance(aid, Identifier) else Identifier(aid)
                for inp, aid in inputs.items()}

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return 'Job({}, {})'.format(
                self.inputs, self.workflow)

    @staticmethod
    def niljob(asset_id: Identifier) -> 'Job':
        """Returns a zero-step job for a dataset.

        Args:
            asset_id: The dataset to represent.

        The job will have no steps, and a single input named `dataset`.
        """
        return Job(
                Identifier('*'), Workflow(['dataset'], {}, []),
                {'dataset': asset_id})

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
        return Job(self.submitter, sub_wf, inputs)

    def id_hashes(self) -> Dict[str, str]:
        """Calculates id hashes of all items in the job's workflow.

        Returns:
            A dict mapping workflow items to their id hash.
        """
        class DependencyMissing(RuntimeError):
            pass

        def prop_input_id_hashes(item_id_hashes: Dict[str, str]) -> None:
            for inp_name, inp_src in self.inputs.items():
                inp_id_hash = sha256()
                inp_id_hash.update(inp_src.encode('utf-8'))
                item_id_hashes[inp_name] = inp_id_hash.hexdigest()

        def prop_input_sources(
                item_id_hashes: Dict[str, str], step: WorkflowStep) -> None:
            for inp_name, inp_src in step.inputs.items():
                inp_item = '{}.{}'.format(step.name, inp_name)
                if inp_item not in item_id_hashes:
                    if inp_src not in item_id_hashes:
                        raise DependencyMissing()
                    item_id_hashes[inp_item] = item_id_hashes[inp_src]

        def calc_step_outputs(
                item_id_hashes: Dict[str, str], step: WorkflowStep) -> None:
            step_hash = sha256()
            for inp_name in sorted(step.inputs):
                inp_item = '{}.{}'.format(step.name, inp_name)
                step_hash.update(
                        item_id_hashes[inp_item].encode('utf-8'))
            step_hash.update(
                    step.compute_asset_id.encode('utf-8'))
            for outp_name in step.outputs:
                outp_item = '{}.{}'.format(step.name, outp_name)
                outp_hash = step_hash.copy()
                outp_hash.update(outp_name.encode('utf-8'))
                item_id_hashes[outp_item] = outp_hash.hexdigest()

        def set_workflow_outputs_id_hashes(
                item_id_hashes: Dict[str, str],
                outputs: Dict[str, str]) -> None:
            for outp_name, outp_src in outputs.items():
                item_id_hashes[outp_name] = item_id_hashes[outp_src]

        item_id_hashes = dict()      # type: Dict[str, str]
        prop_input_id_hashes(item_id_hashes)

        steps_done = set()      # type: Set[str]
        while len(steps_done) < len(self.workflow.steps):
            for step in self.workflow.steps.values():
                if step.name not in steps_done:
                    try:
                        prop_input_sources(item_id_hashes, step)
                        calc_step_outputs(item_id_hashes, step)
                        steps_done.add(step.name)
                    except DependencyMissing:
                        continue

        set_workflow_outputs_id_hashes(item_id_hashes, self.workflow.outputs)
        return item_id_hashes


class Plan:
    """A plan for executing a workflow.

    A plan says which step is to be executed by which site.

    Attributes:
        step_sites (Dict[WorkflowStep, str]): Maps steps to their
                site's id.

    """
    def __init__(self, step_sites: Dict[str, Identifier]) -> None:
        """Create a plan.

        Args:
            step_sites: A map from step names to their site's id.

        """
        self.step_sites = step_sites

    def __str__(self) -> str:
        """Return a string representation of the object."""
        result = ''
        for step_name, site_id in self.step_sites.items():
            result += '{} -> {}\n'.format(step_name, site_id)
        return result


class ExecutionRequest:
    """A request to execute a job according to a plan.

    Attributes:
        job: The job we're asking to execute.
        plan: The plan according to which it should be executed.

    """
    def __init__(self, job: Job, plan: Plan) -> None:
        """Create an ExecutionRequest.

        Args:
            job: The job we're asking to execute.
            plan: The plan according to which it should be executed.
        """
        self.job = job
        self.plan = plan
