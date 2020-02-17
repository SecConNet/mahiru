from threading import Thread
from time import sleep
from typing import Any, Dict, Optional, Tuple

from definitions import ILocalWorkflowRunner
from workflow import WorkflowStep, Workflow
from asset_store import AssetStore
from definitions import Plan
from ddm_client import DDMClient


class Job(Thread):
    def __init__(
            self, this_runner: str,
            workflow: Workflow, inputs: Dict[str, str], plan: Plan,
            target_store: AssetStore
            ) -> None:
        """Creates a Job object.

        This represents the execution of (parts of) a workflow at a
        site.

        Args:
            this_runner: The runner we're running in.
            workflow: The workflow to execute.
            plan: The plan for the workflow to execute.
            target_store: The asset store to put results into.
        """
        super().__init__(name='JobAtRunner-{}'.format(this_runner))
        self._this_runner = this_runner
        self._workflow = workflow
        self._inputs = inputs
        self._plan = {step.name: site for step, site in plan.items()}
        self._target_store = target_store
        self._ddm_client = DDMClient()

    def run(self) -> None:
        """Runs the job.

        This executes the steps in the job one at a time, in an order
        compatible with their dependencies.
        """
        steps_to_do = {
                step for step in self._workflow.steps.values()
                if self._plan[step.name] == self._this_runner}

        while len(steps_to_do) > 0:
            for step in steps_to_do:
                inputs = self._get_step_inputs(step)
                if inputs is not None:
                    print('Job at {} executing step {}'.format(
                        self._this_runner, step))
                    # run step
                    outputs = dict()    # type: Dict[str, Any]
                    if step.compute_asset == 'Combine':
                        outputs['y'] = [inputs['x1'], inputs['x2']]
                    elif step.compute_asset == 'Anonymise':
                        outputs['y'] = [x - 10 for x in inputs['x1']]
                    elif step.compute_asset == 'Aggregate':
                        outputs['y'] = sum(inputs['x1']) / len(inputs['x1'])
                    elif step.compute_asset == 'Addition':
                        outputs['y'] = inputs['x1'] + inputs['x2']
                    else:
                        raise RuntimeError('Unknown compute asset')

                    # save output to store
                    for output_name, output_value in outputs.items():
                        data_key = 'steps.{}.outputs.{}'.format(
                                step.name, output_name)
                        self._target_store.store(data_key, output_value)

                    steps_to_do.remove(step)
                    break
            else:
                sleep(0.5)
        print('Job at {} done'.format(self._this_runner))

    def _get_step_inputs(self, step: WorkflowStep) -> Optional[Dict[str, Any]]:
        """Find and obtain inputs for the steps.

        If all inputs are available, returns a dictionary mapping their
        keys to their values. If at least one input is not yet
        available, returns None.

        Args:
            step: The step to obtain inputs for.

        Return:
            A dictionary keyed by output name with corresponding
            values.
        """
        step_input_data = dict()
        for inp_name, inp_source in step.inputs.items():
            source_store, data_key = self._source(inp_source)
            print('Job at {} getting input {} from site {}'.format(
                self._this_runner, data_key, source_store))
            try:
                step_input_data[inp_name] = self._ddm_client.retrieve_data(
                        source_store, data_key)
                print('Job at {} found input {} available.'.format(
                    self._this_runner, data_key))
            except KeyError:
                print('Job at {} found input {} not yet available.'.format(
                    self._this_runner, data_key))
                return None

        return step_input_data

    def _source(self, inp_source: str) -> Tuple[str, str]:
        """Extracts the source from a source description.

        If the input is of the form 'step/output', this will return the
        target store for the runner which is to execute that step
        according to the current plan, and the output name.

        If the input is of the form 'store:data', this will return the
        given store and the name of the input data set.
        """
        if '/' in inp_source:
            step_name, output_name = inp_source.split('/')
            src_runner_name = self._plan[step_name]
            src_store = self._ddm_client.get_target_store(src_runner_name)
            return src_store, 'steps.{}.outputs.{}'.format(
                    step_name, output_name)
        else:
            inp_source = self._inputs[inp_source]
            if ':' in inp_source:
                store_name, data_name = inp_source.split(':')
                return store_name, data_name
            else:
                raise RuntimeError('Invalid input specification "{}"'.format(
                    inp_source))


class LocalWorkflowRunner(ILocalWorkflowRunner):
    """A service for running workflows at a given site.
    """
    def __init__(self, name: str, target_store: AssetStore) -> None:
        """Creates a LocalWorkflowRunner.

        Args:
            name: Name identifying this runner.
            target_store: An AssetStore to store result in.
        """
        self.name = name
        self._target_store = target_store

    def target_store(self) -> str:
        """Returns the name of the store containing our results.

        Returns:
            A string with the name.
        """
        return self._target_store.name

    def execute_plan(
            self,
            workflow: Workflow, inputs: Dict[str, str], plan: Plan
            ) -> None:
        job = Job(self.name, workflow, inputs, plan, self._target_store)
        job.start()
