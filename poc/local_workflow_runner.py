from threading import Thread
from time import sleep
from typing import Any, Dict, Optional, Tuple

from asset_store import AssetStore
from ddm_client import DDMClient
from definitions import ILocalWorkflowRunner, Plan
from policy import PolicyManager
from policy_evaluator import PolicyEvaluator
from workflow import Job, WorkflowStep, Workflow


class JobRun(Thread):
    def __init__(
            self, policy_manager: PolicyManager,
            this_runner: str, administrator: str,
            job: Job, plan: Plan,
            target_store: AssetStore
            ) -> None:
        """Creates a JobRun object.

        This represents the execution of (parts of) a workflow at a
        site.

        Args:
            this_runner: The runner we're running in.
            administrator: Name of the party administrating this runner.
            job: The job to execute.
            plan: The plan for how to execute the job.
            target_store: The asset store to put results into.
        """
        super().__init__(name='JobAtRunner-{}'.format(this_runner))
        self._policy_manager = policy_manager
        self._policy_evaluator = PolicyEvaluator(policy_manager)
        self._this_runner = this_runner
        self._administrator = administrator
        self._job = job
        self._workflow = job.workflow
        self._inputs = job.inputs
        self._plan = {step.name: site for step, site in plan.items()}
        self._target_store = target_store
        self._ddm_client = DDMClient()

    def run(self) -> None:
        """Runs the job.

        This executes the steps in the job one at a time, in an order
        compatible with their dependencies.
        """
        if not self._is_legal():
            # for each output we were supposed to produce
                # store an error object instead
            # for now, we raise and crash
            raise RuntimeError(
                    'Security violation, asked to perform an illegal job.')

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
                    # Would be more correct to actually build the provenance
                    # here by adding the current step and any new inputs to
                    # a provenance object. TODO
                    prov = self._job.provenance(step)
                    for output_name, output_value in outputs.items():
                        data_key = 'steps.{}.outputs.{}'.format(
                                step.name, output_name)
                        self._target_store.store(data_key, output_value, prov)

                    steps_to_do.remove(step)
                    break
            else:
                sleep(0.5)
        print('Job at {} done'.format(self._this_runner))

    def _is_legal(self) -> bool:
        """Checks whether this request is legal.

        If we have permission to execute all of our steps, then this
        is a legal job as far as we are concerned.
        """
        perms = self._policy_evaluator.calculate_permissions(self._job)
        for step in self._workflow.steps.values():
            if self._plan[step.name] == self._this_runner:
                step_id = 'steps.{}'.format(step.name)
                # check that we can access the step's inputs
                for inp_name, inp_src in step.inputs.items():
                    inp_id = '{}.inputs.{}'.format(step_id, inp_name)
                    inp_perms = perms[inp_id]
                    if not self._policy_manager.may_access(
                            inp_perms, self._administrator):
                        return False
                    # check that the site we'll download this input from may
                    # access it
                    if '/' in inp_src:
                        src_step, src_outp = inp_src.split('/')
                        inp_src_id = 'steps.{}.outputs.{}'.format(
                                src_step, src_outp)
                        src_perms = perms[inp_src_id]
                        src_party = self._ddm_client.get_runner_administrator(
                                self._plan[src_step])
                        if not self._policy_manager.may_access(
                                src_perms, src_party):
                            return False

                # check that we can access the step itself
                if not self._policy_manager.may_access(
                        perms[step_id], self._administrator):
                    return False

        return True

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
                step_input_data[inp_name], prov = (
                        self._ddm_client.retrieve_data(
                            source_store, data_key))
                print('Job at {} found input {} available.'.format(
                    self._this_runner, data_key))
                print('Provenance: {}'.format(prov))
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
    def __init__(
            self, name: str, administrator: str, policy_manager: PolicyManager,
            target_store: AssetStore) -> None:
        """Creates a LocalWorkflowRunner.

        Args:
            name: Name identifying this runner.
            administrator: Party administrating this runner.
            policy_manager: A PolicyManager to use.
            target_store: An AssetStore to store result in.
        """
        self.name = name
        self._administrator = administrator
        self._policy_manager = policy_manager
        self._target_store = target_store

    def target_store(self) -> str:
        """Returns the name of the store containing our results.

        Returns:
            A string with the name.
        """
        return self._target_store.name

    def execute_job(
            self, job: Job, plan: Plan) -> None:
        run = JobRun(
                self._policy_manager, self.name, self._administrator,
                job, plan,
                self._target_store)
        run.start()
