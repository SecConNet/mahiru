from copy import copy
from itertools import repeat
from textwrap import indent
from threading import Thread
from time import sleep
from typing import Any, Dict, Generator, List, Optional, Set, Tuple


class Rule:
    """Abstract base class for policy rules.
    """
    pass


class InAssetCollection(Rule):
    """Says that Asset 1 is in AssetCollection 2.
    """
    def __init__(self, asset: str, collection: str) -> None:
        self.asset = asset
        self.collection = collection

    def __repr__(self) -> str:
        return '("{}" is in "{}")'.format(self.asset, self.collection)


class InPartyCollection(Rule):
    """Says that Party party is in PartyCollection collection.
    """
    def __init__(self, party: str, collection: str) -> None:
        self.party = party
        self.collection = collection

    def __repr__(self) -> str:
        return '("{}" is in "{}")'.format(self.party, self.collection)


class MayAccess(Rule):
    """Says that Party party may access Asset asset.
    """
    def __init__(self, party: str, asset: str) -> None:
        self.party = party
        self.asset = asset

    def __repr__(self) -> str:
        return '("{}" may access "{}")'.format(self.party, self.asset)


class ResultOfIn(Rule):
    """Defines collections of results.

    Says that any Asset that was computed from data_asset via
    compute_asset is in collection.
    """
    def __init__(self, data_asset: str, compute_asset: str, collection: str
                 ) -> None:
        self.data_asset = data_asset
        self.compute_asset = compute_asset
        self.collection = collection

    def __repr__(self) -> str:
        return '(result of "{}" on "{}" is in collection "{}")'.format(
                self.compute_asset, self.data_asset, self.collection)


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


class AssetStore:
    """A simple store for assets.
    """
    def __init__(self, name: str) -> None:
        """Create a new empty AssetStore.
        """
        self.name = name
        self._assets = dict()

    def __repr__(self) -> str:
        return 'AssetStore({})'.format(self.name)

    def store(self, name: str, data: Any) -> None:
        """Stores an asset.

        Args:
            name: Name to store asset under.
            data: Asset data to store.

        Raises:
            KeyError: If there's already an asset with name ``name``.
        """
        if name in self._assets:
            raise KeyError('There is already an asset with that name')
        self._assets[name] = data

    def retrieve(self, name: str) -> Any:
        """Retrieves an asset.

        Args:
            name: Name of the asset to retrieve.

        Return:
            The asset data stored under the given name.

        Raises:
            KeyError: If no asset with the given name is stored here.
        """
        print('{} servicing request for data {}, '.format(self, name), end='')
        try:
            data = self._assets[name]
            print('sending...')
            return data
        except KeyError:
            print('not found.')
            raise


class Registry:
    """Global registry of data stores and local workflow runners.

    In a real system, these would just be identified by the URL, and
    use the DNS to resolve. Here, we use this registry instead.
    """
    def __init__(self) -> None:
        """Create a new registry.
        """
        self._runners = dict()          # type: Dict[str, LocalWorkflowRunner]
        self._runner_admins = dict()    # type: Dict[str, str]
        self._stores = dict()           # type: Dict[str, AssetStore]

    def register_runner(
            self, admin: str, runner: 'LocalWorkflowRunner'
            ) -> None:
        """Register a LocalWorkflowRunner with the Registry.

        Args:
            admin: The party administrating this runner.
            runner: The runner to register.
        """
        if runner.name in self._runners:
            raise RuntimeError('There is already a runner with this name')
        self._runners[runner.name] = runner
        self._runner_admins[runner.name] = admin

    def register_store(self, store: 'AssetStore') -> None:
        """Register a AssetStore with the Registry.

        Args:
            store: The data store to register.
        """
        if store.name in self._stores:
            raise RuntimeError('There is already a store with this name')
        self._stores[store.name] = store

    def list_runners(self) -> List[str]:
        """List names of all registered runners.

        Returns:
            A list of names as strings.
        """
        return list(self._runners.keys())

    def get_runner(self, name: str) -> 'LocalWorkflowRunner':
        """Look up a LocalWorkflowRunner.

        Args:
            name: The name of the runner to look up.

        Return:
            The runner with that name.

        Raises:
            KeyError: If no runner with the given name is
                    registered.
        """
        return self._runners[name]

    def get_runner_admin(self, name: str) -> str:
        """Look up who administrates a given runner.

        Args:
            name: The name of the runner to look up.

        Return:
            The name of the party administrating it.

        Raises:
            KeyError: If no runner with the given name is regstered.
        """
        return self._runner_admins[name]

    def get_store(self, name: str) -> 'AssetStore':
        """Look up an AssetStore.

        Args:
            name: The name of the store to look up.

        Return:
            The store with that name.

        Raises:
            KeyError: If no store with the given name is currently
                    registered.
        """
        return self._stores[name]


global_registry = Registry()


Plan = Dict[WorkflowStep, str]      # maps step to LocalWorkflowRunner name


class DDMClient:
    """Handles connecting to global registry, runners and stores.
    """
    def __init__(self) -> None:
        pass

    def register_runner(
            self, admin: str, runner: 'LocalWorkflowRunner'
            ) -> None:
        """Register a LocalWorkflowRunner with the Registry.

        Args:
            admin: The party administrating this runner.
            runner: The runner to register.
        """
        global_registry.register_runner(admin, runner)

    def register_store(self, store: 'AssetStore') -> None:
        """Register a AssetStore with the Registry.

        Args:
            store: The data store to register.
        """
        global_registry.register_store(store)

    def list_runners(self) -> List[str]:
        """Returns a list of id's of available runners.
        """
        return global_registry.list_runners()

    def get_target_store(self, runner_id: str) -> str:
        """Returns the id of the target store of the given runner.
        """
        return global_registry.get_runner(runner_id).target_store()

    def get_runner_administrator(self, runner_id: str) -> str:
        """Returns the id of the party administrating a runner.
        """
        return global_registry.get_runner_admin(runner_id)

    def retrieve_data(self, store_id: str, name: str) -> Any:
        """Obtains a data item from a store.
        """
        store = global_registry.get_store(store_id)
        return store.retrieve(name)

    def execute_plan(
            self, runner_id: str,
            workflow: Workflow, inputs: Dict[str, str], plan: Plan
            ) -> None:
        """Submits a plan for execution to a local runner.

        Args:
            runner_id: The runner to submit to.
            workflow: The workflow to submit.
            inputs: The inputs to feed into the workflow.
            plan: The plan to execute the workflow to.
        """
        runner = global_registry.get_runner(runner_id)
        return runner.execute_plan(workflow, inputs, plan)


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
                    outputs = dict()
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
                return inp_source.split(':')
            else:
                raise RuntimeError('Invalid input specification "{}"'.format(
                    inp_source))


class LocalWorkflowRunner:
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


class PolicyManager:
    """Holds a set of rules and can interpret them.
    """
    def __init__(self, policies: List[Rule]) -> None:
        self.policies = policies

    def may_access(self, asset_colls: List[Set[str]], party: str) -> bool:
        """Checks whether an asset can be at a site.

        This function checks whether the given site has access rights
        to at least one asset in each of the given set of assets.
        """
        def matches_one(asset_coll: Set[str], party: str) -> bool:
            for asset in asset_coll:
                for rule in self.policies:
                    if isinstance(rule, MayAccess):
                        if rule.asset == asset and rule.party == party:
                            return True
            return False

        return all([matches_one(asset_coll, party)
                    for asset_coll in asset_colls])

    def equivalent_assets(self, asset: str) -> Set[str]:
        """Returns all the assets whose rules apply to an asset.

        These are the asset itself, and all assets that are asset
        collections that the asset is directly or indirectly in.
        """
        cur_assets = set()     # type: Set[str]
        new_assets = {asset}
        while new_assets:
            cur_assets |= new_assets
            new_assets = set()
            for asset in cur_assets:
                for rule in self.policies:
                    if isinstance(rule, InAssetCollection):
                        if rule.asset == asset:
                            if rule.collection not in cur_assets:
                                new_assets.add(rule.collection)
        cur_assets.add('*')
        return cur_assets

    def _equivalent_parties(self, party: str) -> List[str]:
        """Returns all the parties whose rules apply to an asset.

        These are the parties itself, and all parties that are party
        collections that the party is directly or indirectly in.
        """
        cur_parties = list()     # type: List[str]
        new_parties = [party]
        while new_parties:
            cur_parties.extend(new_parties)
            new_parties = list()
            for party in cur_parties:
                for rule in self.policies:
                    if isinstance(rule, InPartyCollection):
                        if rule.party == party:
                            new_parties.append(rule.collection)
        return cur_parties

    def resultofin_rules(
            self, asset_set: Set[str], compute_asset: str
            ) -> List[ResultOfIn]:
        """Returns all ResultOfIn rules that apply to these assets.

        These are rules that have one of the given assets in their
        asset field, and the given compute_asset or an equivalent one.
        """
        def rules_for_asset(asset: str) -> List[ResultOfIn]:
            """Gets all matching rules for the given single asset.
            """
            result = list()     # type: List[ResultOfIn]
            assets = self.equivalent_assets(asset)
            for rule in self.policies:
                if isinstance(rule, ResultOfIn):
                    for asset in assets:
                        if rule.data_asset == asset:
                            result.append(rule)
            return result

        comp_assets = self.equivalent_assets(compute_asset)

        rules = list()  # type: List[ResultOfIn]
        for asset in asset_set:
            new_rules = rules_for_asset(asset)
            rules.extend([rule
                          for rule in new_rules
                          if rule.compute_asset in comp_assets])
        return rules


class WorkflowEngine:
    """Executes workflows across sites in DDM.
    """
    def __init__(
            self, policy_manager: PolicyManager, ddm_client: DDMClient
            ) -> None:
        """Create a WorkflowEngine.

        Args:
            policy_manager: Component that knows about policies.
        """
        self._policy_manager = policy_manager
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
        result_collections = self._find_result_collections(
                workflow, inputs)
        print('Result collections:')
        for result, coll in result_collections.items():
            print('    {} -> {}'.format(result, coll))
        print()

        plans = self._make_plans(submitter, workflow, result_collections)
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
        selected_runner_names = set(selected_plan.values())
        for runner_name in selected_runner_names:
            self._ddm_client.execute_plan(
                    runner_name, workflow, inputs, selected_plan)

        # get workflow outputs
        results = dict()
        while len(results) < len(workflow.outputs):
            for wf_outp_name, wf_outp_source in workflow.outputs.items():
                if wf_outp_name not in results:
                    src_step_name, src_step_output = wf_outp_source.split('/')
                    src_runner_name = selected_plan[workflow.steps[src_step_name]]
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

    def _make_plans(
            self, submitter: str, workflow: Workflow,
            result_collections: Dict[str, List[Set[str]]]
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
                    computed results.

        Returns:
            A list of plans, each consisting of a list of sites
            corresponding to the given list of steps.
        """
        def may_access_step(step: WorkflowStep, party: str) -> bool:
            step_key = 'steps.{}'.format(step.name)
            asset_coll = result_collections[step_key]
            return self._policy_manager.may_access(asset_coll, party)

        def may_run(step: WorkflowStep, runner: str) -> bool:
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
            return may_access_step(step, party)

        sorted_steps = self._sort_workflow(workflow)
        plan = [''] * len(sorted_steps)

        def plan_from(cur_step_idx: int) -> Generator[List[str], None, None]:
            """Make remaining plan, starting at cur_step.

            Yields any complete plans found.
            """
            cur_step = sorted_steps[cur_step_idx]
            step_name = cur_step.name
            for runner in self._ddm_client.list_runners():
                if may_run(cur_step, runner):
                    plan[cur_step_idx] = runner
                    if cur_step_idx == len(plan) - 1:
                        if may_access_step(cur_step, submitter):
                            yield copy(plan)
                    else:
                        yield from plan_from(cur_step_idx + 1)

        return [dict(zip(sorted_steps, plan)) for plan in plan_from(0)]


class Site:
    def __init__(
            self, name: str, administrator: str, stored_data: Dict[str, int],
            rules: List[Rule]) -> None:
        """Create a Site.

        Also registers its runner and store in the global registry.

        Args:
            name: Name of the site
            administrator: Party which administrates this site.
            stored_data: Data sets stored at this site.
            rules: A policy to adhere to.
        """
        # Metadata
        self.name = name
        self.administrator = administrator

        self._ddm_client = DDMClient()

        # Server side
        self.store = AssetStore(name + '-store')
        for key, val in stored_data.items():
            self.store.store(key, val)
        self._ddm_client.register_store(self.store)

        self.runner = LocalWorkflowRunner(name + '-runner', self.store)
        self._ddm_client.register_runner(administrator, self.runner)

        # Client side
        self._policy_manager = PolicyManager(rules)
        self._workflow_engine = WorkflowEngine(
                self._policy_manager, self._ddm_client)

    def __repr__(self) -> str:
        return 'Site({})'.format(self.name)

    def run_workflow(
            self, workflow: Workflow, inputs: Dict[str, str]
            ) -> Dict[str, Any]:
        """Run a workflow on behalf of the party running this site.
        """
        return self._workflow_engine.execute(
                self.administrator, workflow, inputs)


def run_scenario(scenario: Dict[str, Any]) -> None:
    # run
    print('Rules:')
    for rule in scenario['rules']:
        print('    {}'.format(rule))
    print()
    print('On behalf of: {}'.format(scenario['user_site'].administrator))
    print()
    print('Workflow:')
    print(indent(str(scenario['workflow']), ' '*4))
    print()
    print('Inputs: {}'.format(scenario['inputs']))
    print()

    result = scenario['user_site'].run_workflow(
            scenario['workflow'], scenario['inputs'])
    print()
    print('Result:')
    print(result)


def scenario_saas_with_data() -> Dict[str, Any]:
    result = dict()     # type: Dict[str, Any]

    result['rules'] = [
            MayAccess('party1', 'site1-store:data1'),
            MayAccess('party2', 'site1-store:data1'),
            MayAccess('party2', 'site2-store:data2'),
            ResultOfIn('site1:data1', 'Addition', 'result1'),
            ResultOfIn('site2:data2', 'Addition', 'result2'),
            MayAccess('party2', 'result1'),
            MayAccess('party1', 'result1'),
            MayAccess('party1', 'result2'),
            MayAccess('party2', 'result2'),
            ]

    result['sites'] = [
            Site('site1', 'party1', {'data1': 42}, result['rules']),
            Site('site2', 'party2', {'data2': 3}), result['rules']]

    result['workflow'] = Workflow(
            ['x1', 'x2'], {'y': 'addstep/y'}, [
                WorkflowStep(
                    'addstep', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Addition')
                ])

    result['inputs'] = {'x1': 'site1-store:data1', 'x2': 'site2-store:data2'}

    result['user_site'] = result['sites'][0]

    return result


def scenario_pii() -> Dict[str, Any]:
    scenario = dict()     # type: Dict[str, Any]

    scenario['rules'] = [
            InAssetCollection('site1-store:pii1', 'PII1'),
            MayAccess('party1', 'PII1'),
            ResultOfIn('PII1', '*', 'PII1'),
            ResultOfIn('PII1', 'Anonymise', 'ScienceOnly1'),
            ResultOfIn('PII1', 'Aggregate', 'Public'),
            ResultOfIn('ScienceOnly1', '*', 'ScienceOnly1'),
            InAssetCollection('ScienceOnly1', 'ScienceOnly'),
            ResultOfIn('Public', '*', 'Public'),

            InAssetCollection('site2-store:pii2', 'PII2'),
            MayAccess('party2', 'PII2'),
            MayAccess('party1', 'PII2'),
            ResultOfIn('PII2', '*', 'PII2'),
            ResultOfIn('PII2', 'Anonymise', 'ScienceOnly2'),
            ResultOfIn('ScienceOnly2', '*', 'ScienceOnly2'),
            InAssetCollection('ScienceOnly2', 'ScienceOnly'),

            MayAccess('party3', 'ScienceOnly'),
            MayAccess('party1', 'Public'),
            MayAccess('party2', 'Public'),
            MayAccess('party3', 'Public'),
            ]

    scenario['sites'] = [
            Site('site1', 'party1', {'pii1': 42}, scenario['rules']),
            Site('site2', 'party2', {'pii2': 3}, scenario['rules']),
            Site('site3', 'party3', {}, scenario['rules'])]

    scenario['workflow'] = Workflow(
            ['x1', 'x2'], {'result': 'aggregate/y'}, [
                WorkflowStep(
                    'combine', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Combine'),
                WorkflowStep(
                    'anonymise', {'x1': 'combine/y'}, ['y'], 'Anonymise'),
                WorkflowStep(
                    'aggregate', {'x1': 'anonymise/y'}, ['y'], 'Aggregate')])

    scenario['inputs'] = {'x1': 'site1-store:pii1', 'x2': 'site2-store:pii2'}
    scenario['user_site'] = scenario['sites'][2]

    return scenario


run_scenario(scenario_pii())
