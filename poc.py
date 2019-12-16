from copy import copy
from itertools import repeat
from textwrap import indent
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


class Site:
    def __init__(
            self, name: str, administrator: str, stored_data: Dict[str, int]
            ) -> None:
        """Create a Site.

        Args:
            name: Name of the site
            administrator: Party which administrates this site.
            stored_data: Data sets stored at this site.
        """
        self.name = name
        self.administrator = administrator
        self._stored_data = stored_data

    def __repr__(self) -> str:
        return 'Site({})'.format(self.name)

    def get_data(self, key: str) -> int:
        return self._stored_data[key]

    def execute_workflow_locally(self, workflow: Workflow) -> None:
        pass

    def _store_data(self, key: str, value: int) -> None:
        self._stored_data[key] = value


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
    def __init__(self, sites: List[Site], policy_manager: PolicyManager
                 ) -> None:
        """Create a WorkflowEngine.

        Args:
            sites: List of sites in the DDM.
            policy_manager: Component that knows about policies.
        """
        self._sites = sites
        self._policy_manager = policy_manager

    def execute(
            self, submitter: str, workflow: Workflow, inputs: Dict[str, str]
            ) -> None:
        """Plans and executes the given workflow.

        Args:
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

        # execute subplans on sites

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

            Raises KeyError if the input source is not (yet) available.
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
            ) -> List[Dict[WorkflowStep, Site]]:
        """Assigns a site to each workflow step.

        Uses the given result collections to determine where steps can
        be executed.

        Returns:
            A list of plans, each consisting of a list of sites
            corresponding to the given list of steps.
        """
        def may_run(step: WorkflowStep, site: Site) -> bool:
            """Checks whether the given site may run the given step.
            """
            party = site.administrator

            # check each input
            for inp_name in step.inputs:
                inp_key = 'steps.{}.inputs.{}'.format(step.name, inp_name)
                asset_coll = result_collections[inp_key]
                if not self._policy_manager.may_access(asset_coll, party):
                    return False

            # check step itself (i.e. outputs)
            step_key = 'steps.{}'.format(step.name)
            asset_coll = result_collections[step_key]
            if not self._policy_manager.may_access(asset_coll, party):
                return False

            return True

        sorted_steps = self._sort_workflow(workflow)
        dummy_site = Site('', '', {})
        plan = list(repeat(dummy_site, len(sorted_steps)))  # type: List[Site]

        def plan_from(cur_step: int) -> Generator[List[Site], None, None]:
            """Make remaining plan, starting at cur_step.

            Yields any complete plans found
            """
            step_name = sorted_steps[cur_step].name
            for site in self._sites:
                if may_run(sorted_steps[cur_step], site):
                    plan[cur_step] = site
                    if cur_step == len(plan) - 1:
                        yield copy(plan)
                    else:
                        yield from plan_from(cur_step + 1)

        return [{step: site for step, site in zip(sorted_steps, plan)}
                for plan in plan_from(0)]


def scenario_saas_with_data() -> Dict[str, Any]:
    result = dict()     # type: Dict[str, Any]

    result['sites'] = [
            Site('site1', 'party1', {'data1': 42}),
            Site('site2', 'party2', {'data2': 3})]

    result['rules'] = [
            MayAccess('party1', 'data1'),
            MayAccess('party2', 'data1'),
            MayAccess('party2', 'data2'),
            ResultOfIn('data1', 'addition', 'result1'),
            MayAccess('party2', 'result1'),
            MayAccess('party1', 'result2'),
            ]

    result['workflow'] = Workflow(
            ['x1', 'x2'], {'y': 'addstep/y'}, [
                WorkflowStep(
                    'addstep', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'addition')
                ])

    result['inputs'] = {'x1': 'data1', 'x2': 'data2'}
    result['user'] = 'party2'

    return result


def scenario_pii() -> Dict[str, Any]:
    scenario = dict()     # type: Dict[str, Any]

    scenario['sites'] = [
            Site('site1', 'party1', {'pii1': 42}),
            Site('site2', 'party2', {'pii2': 3}),
            Site('site3', 'party3', {})]

    scenario['rules'] = [
            InAssetCollection('pii1', 'PII1'),
            MayAccess('party1', 'PII1'),
            ResultOfIn('PII1', '*', 'PII1'),
            ResultOfIn('PII1', 'Anonymise', 'ScienceOnly1'),
            ResultOfIn('PII1', 'Aggregate', 'Public'),
            ResultOfIn('ScienceOnly1', '*', 'ScienceOnly1'),
            InAssetCollection('ScienceOnly1', 'ScienceOnly'),
            ResultOfIn('Public', '*', 'Public'),

            InAssetCollection('pii2', 'PII2'),
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

    scenario['workflow'] = Workflow(
            ['x1', 'x2'], {'result': 'aggregate/y'}, [
                WorkflowStep(
                    'combine', {'x1': 'x1', 'x2': 'x2'}, ['y'], 'Combine'),
                WorkflowStep(
                    'anonymise', {'x1': 'combine/y'}, ['y'], 'Anonymise'),
                WorkflowStep(
                    'aggregate', {'x1': 'anonymise/y'}, ['y'], 'Aggregate')])

    scenario['inputs'] = {'x1': 'pii1', 'x2': 'pii2'}
    scenario['user'] = 'party3'

    return scenario


def run_scenario(scenario: Dict[str, Any]) -> None:
    policy_manager = PolicyManager(scenario['rules'])
    workflow_engine = WorkflowEngine(scenario['sites'], policy_manager)

    # run
    print('Rules:')
    for rule in scenario['rules']:
        print('    {}'.format(rule))
    print()
    print('On behalf of: {}'.format(scenario['user']))
    print()
    print('Workflow:')
    print(indent(str(scenario['workflow']), ' '*4))
    print()
    print('Inputs: {}'.format(scenario['inputs']))
    print()

    workflow_engine.execute(
            scenario['user'], scenario['workflow'], scenario['inputs'])


run_scenario(scenario_pii())
