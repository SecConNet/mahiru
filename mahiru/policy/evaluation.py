"""Components for evaluating workflow permissions."""
from typing import Dict, Iterable, List, Optional, Set, Tuple, Type, TypeVar

from mahiru.definitions.identifier import Identifier
from mahiru.definitions.interfaces import IPolicyCollection
from mahiru.definitions.workflows import Job, Plan, Workflow, WorkflowStep
from mahiru.policy.rules import (
        GroupingRule, InAssetCategory, InAssetCollection, InPartyCollection,
        InSiteCategory, MayAccess, ResultOfIn, ResultOfDataIn,
        ResultOfComputeIn)


_GroupingRule = TypeVar('_GroupingRule', bound=GroupingRule)


class Permissions:
    """Represents permissions for an asset."""
    def __init__(self, sets: Optional[List[Set[Identifier]]] = None) -> None:
        """Creates Permissions that do not allow access."""
        # friend PolicyEvaluator
        self._sets = list()     # type: List[Set[Identifier]]
        if sets is not None:
            self._sets = sets

    def __str__(self) -> str:
        """Returns a string representationf this object."""
        return 'Permissions({})'.format(self._sets)

    def __repr__(self) -> str:
        """Returns a string representationf this object."""
        return 'Permissions({})'.format(repr(self._sets))


class PolicyEvaluator:
    """Interprets policies to support planning and execution."""
    def __init__(self, policy_collection: IPolicyCollection) -> None:
        """Create a PolicyEvaluator.

        Args:
            policy_collection: A collections of policies to evaluate.
        """
        self._policy_collection = policy_collection

    def permissions_for_asset(self, asset: Identifier) -> Permissions:
        """Returns permissions for the given asset.

        This must be a primary asset, not an intermediate result, as
        this function only follows InAssetCollection rules.

        Args:
            asset: The asset to get permissions for.
        """
        result = Permissions()
        result._sets = [self._upward_equivalent_objects(
            InAssetCollection, asset)]
        return result

    def propagate_permissions(
            self,
            input_permissions: List[Permissions],
            compute_asset: Identifier,
            output: str
            ) -> Permissions:
        """Determines access for the result of an operation.

        This applies the ResultOfDataIn and ResultOfComputeIn rules to
        determine, from the access permissions for the inputs of an
        operation and the compute asset to use, the access permissions
        of the results.

        Args:
            input_permissions: A list of access permissions to
                    propagate, one for each input.
            compute_asset: The compute asset used in the operation.
            output: The step output to propagate permissions for.

        Returns:
            The access permissions of the results.
        """
        result = Permissions()
        for input_perms in input_permissions:
            for asset_set in input_perms._sets:
                data_coll, compute_coll = self._resultofin_collections(
                        asset_set, compute_asset, output)
                result._sets.append(data_coll)
                result._sets.append(compute_coll)

        return result

    def may_access(self, permissions: Permissions, site: Identifier) -> bool:
        """Checks whether an asset can be at a site.

        This function checks whether the given site has access rights
        to at least one asset in each of the given set of assets.

        Args:
            permissions: Permissions for the asset to check.
            site: A site which needs access.
        """
        def matches_one(
                asset_set: Set[Identifier], equiv_sites: Set[Identifier]
                ) -> bool:
            for asset in asset_set:
                for rule in self._policy_collection.policies():
                    if isinstance(rule, MayAccess):
                        if rule.asset == asset and rule.site in equiv_sites:
                            return True
                        if rule.asset == asset and rule.site == '*':
                            return True
            return False

        equiv_sites = self._upward_equivalent_objects(InSiteCategory, site)
        return all([matches_one(asset_set, equiv_sites)
                    for asset_set in permissions._sets])

    def _equivalent_parties(self, party: str) -> List[str]:
        """Returns all the parties whose rules apply to an asset.

        These are the parties itself, and all parties that are party
        collections that the party is directly or indirectly in.

        Args:
            party: The party to find equivalents for.
        """
        cur_parties = list()     # type: List[str]
        new_parties = [party]
        while new_parties:
            cur_parties.extend(new_parties)
            new_parties = list()
            for party in cur_parties:
                for rule in self._policy_collection.policies():
                    if isinstance(rule, InPartyCollection):
                        if rule.party == party:
                            new_parties.append(rule.collection)
        return cur_parties

    def _upward_equivalent_objects(
            self, rule_type: Type[_GroupingRule], obj: Identifier
            ) -> Set[Identifier]:
        """Returns all the upward-equivalent objects.

        These are the object itself, and all objects that are object
        groupings that the object is directly or indirectly in.

        Args:
            rule_type: Type of rule to follow, e.g. InAssetCategory.
            obj: The object to find equivalents for.
        """
        cur_objects = set()     # type: Set[Identifier]
        new_objects = {obj}
        while new_objects:
            cur_objects |= new_objects
            new_objects = set()
            for o in cur_objects:
                for rule in self._policy_collection.policies():
                    if isinstance(rule, rule_type):
                        if rule.grouped() == o:
                            if rule.group() not in cur_objects:
                                new_objects.add(rule.group())
        return cur_objects

    def _downward_equivalent_objects(
            self, rule_type: Type[_GroupingRule], obj: Identifier
            ) -> Set[Identifier]:
        """Returns all the downward-equivalent objects.

        These are the object itself, and if it is a grouping, all
        objects that are in it or in a subgrouping.

        Args:
            rule_type: Type of rule to follow, e.g. InAssetCategory.
            obj: The object or object category to find equivalents for.
        """
        cur_objects = set()      # type: Set[Identifier]
        new_objects = {obj}
        while new_objects:
            cur_objects |= new_objects
            new_objects = set()
            for o in cur_objects:
                for rule in self._policy_collection.policies():
                    if isinstance(rule, rule_type):
                        if rule.group() == o:
                            if rule.grouped() not in cur_objects:
                                new_objects.add(rule.grouped())
        return cur_objects

    def _resultofin_collections(
            self, input_assets: Set[Identifier],
            compute_asset: Identifier, output: str,
            ) -> Tuple[Set[Identifier], Set[Identifier]]:
        """Returns collections these assets propagate to.

        This finds ResultOfIn rules that apply to the given assets,
        and collects the collections that they propagate to. Two sets
        of collections are returned, the first one for ResultOfDataIn
        rules and the second one for ResultOfComputein rules.

        Args:
            input_assets: Set of data assets to match rules to.
            compute_asset: Compute asset to match rules to.
            output: Output to match rules to.
        """
        data_collections, compute_collections = set(), set()

        input_assets_colls = {
                a for input_asset in input_assets
                for a in self._upward_equivalent_objects(
                    InAssetCollection, input_asset)}
        compute_asset_colls = self._upward_equivalent_objects(
                InAssetCollection, compute_asset)

        for rule in self._policy_collection.policies():
            if isinstance(rule, ResultOfIn):
                if rule.output != '*' and rule.output != output:
                    continue

            if isinstance(rule, ResultOfDataIn):
                if rule.data_asset in input_assets_colls:
                    if rule.compute_asset == '*':
                        data_collections.add(rule.collection)
                    elif compute_asset in self._downward_equivalent_objects(
                            InAssetCategory, rule.compute_asset):
                        data_collections.add(rule.collection)

            elif isinstance(rule, ResultOfComputeIn):
                if rule.compute_asset in compute_asset_colls:
                    if rule.data_asset == '*':
                        compute_collections.add(rule.collection)
                        continue

                    equiv_data_assets = self._downward_equivalent_objects(
                            InAssetCategory, rule.data_asset)
                    if not input_assets.isdisjoint(equiv_data_assets):
                        compute_collections.add(rule.collection)

        return data_collections, compute_collections


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
            for inp_name, inp_asset in job.inputs.items():
                permissions[inp_name] = (
                        self._policy_evaluator.permissions_for_asset(
                            inp_asset))

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
                inp_item = '{}.{}'.format(step.name, inp)
                if inp_item not in permissions:
                    if inp_source not in permissions:
                        # TODO: does this ever happen?
                        raise InputNotAvailable()
                    permissions[inp_item] = permissions[inp_source]

        def calc_step_permissions(
                permissions: Dict[str, Permissions],
                step: WorkflowStep
                ) -> None:
            """Derives the step's permissions and stores them.

            These are the permissions needed to access the compute
            asset, and the permissions needed to access the output
            base assets.
            """
            permissions[step.name] = (
                    self._policy_evaluator.permissions_for_asset(
                        step.compute_asset_id))

            for outp, base_asset in step.outputs.items():
                if base_asset is not None:
                    outp_base_item = '{}.@{}'.format(step.name, outp)
                    permissions[outp_base_item] = (
                            self._policy_evaluator.permissions_for_asset(
                                base_asset))

        def prop_step_outputs(
                permissions: Dict[str, Permissions],
                step: WorkflowStep
                ) -> None:
            """Derives step output permissions.

            Propagates permissions from inputs and output bases via the
            step to the outputs. This takes into account permissions
            induced by the compute asset via ResultOfComputeIn rules,
            but not access to the compute asset itself.

            This modifies the permissions argument.
            """
            input_perms = list()     # type: List[Permissions]
            for inp in step.inputs:
                inp_item = '{}.{}'.format(step.name, inp)
                input_perms.append(permissions[inp_item])

            for output in step.outputs:
                o_input_perms = list(input_perms)
                base_item = '{}.@{}'.format(step.name, output)
                if base_item in permissions:
                    o_input_perms.append(permissions[base_item])

                perms = self._policy_evaluator.propagate_permissions(
                            o_input_perms, step.compute_asset_id, output)

                output_item = '{}.{}'.format(step.name, output)
                permissions[output_item] = perms

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

    def permitted_sites(
            self, job: Job, sites: Iterable[Identifier],
            permissions: Optional[Dict[str, Permissions]] = None
            ) -> Dict[str, List[Identifier]]:
        """Determine permitted sites for workflow steps.

        This function applies the current policies to the given job,
        and for each step in the workflow produces a list of sites at
        which that step is allowed to run.

        Args:
            job: The job to evaluate.
            sites: A set of sites to consider executing steps at.
            permissions: Workflow permissions as calculated by
                    calculate_permissions(). If omitted, they will
                    be calculated automatically.

        Return:
            For each step (by name), a list of ids of sites at which
            the step is allowed to execute.
        """
        if permissions is None:
            permissions = self.calculate_permissions(job)

        result = dict()
        for step in job.workflow.steps.values():
            allowed_sites = list()
            for site in sites:
                site_permitted = True

                # check each input
                for inp_name in step.inputs:
                    inp_item = '{}.{}'.format(step.name, inp_name)
                    inp_perms = permissions[inp_item]
                    if not self._policy_evaluator.may_access(inp_perms, site):
                        site_permitted = False

                # check step itself (i.e. compute asset)
                step_perms = permissions[step.name]
                if not self._policy_evaluator.may_access(step_perms, site):
                    site_permitted = False

                # check each output and its base asset
                for outp_name in step.outputs:
                    base_item = '{}.@{}'.format(step.name, outp_name)
                    if base_item in permissions:
                        base_perms = permissions[base_item]
                        if not self._policy_evaluator.may_access(
                                base_perms, site):
                            site_permitted = False

                    outp_item = '{}.{}'.format(step.name, outp_name)
                    outp_perms = permissions[outp_item]
                    if not self._policy_evaluator.may_access(outp_perms, site):
                        site_permitted = False

                if site_permitted:
                    allowed_sites.append(site)

            result[step.name] = allowed_sites

        return result

    def is_legal(self, job: Job, plan: Plan) -> bool:
        """Checks whether this plan for this job is legal.

        The plan is considered legal if each step can be executed at
        the planned site.

        Args:
            job: The job to be executed.
            plan: The plan to check for legality
        """
        permitted_sites = self.permitted_sites(job, plan.step_sites.values())

        for step_name, site in plan.step_sites.items():
            if site not in permitted_sites[step_name]:
                return False

        return True
