"""Components for evaluating workflow permissions."""
from typing import Dict, List, Set, Type

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import IPolicyCollection
from proof_of_concept.definitions.workflows import Job, Workflow, WorkflowStep
from proof_of_concept.policy.rules import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfIn,
        ResultOfDataIn, ResultOfComputeIn)


class Permissions:
    """Represents permissions for an asset."""
    def __init__(self) -> None:
        """Creates Permissions that do not allow access."""
        # friend PolicyEvaluator
        self._sets = list()     # type: List[Set[Identifier]]

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
        result._sets = [self._equivalent_assets(asset)]
        return result

    def propagate_permissions(
            self,
            input_permissions: List[Permissions],
            compute_asset: Identifier
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

        Returns:
            The access permissions of the results.
        """
        result = Permissions()
        for input_perms in input_permissions:
            for asset_set in input_perms._sets:
                data_rules = self._resultofin_rules(
                        ResultOfDataIn, asset_set, compute_asset)
                result._sets.append({
                        asset
                        for rule in data_rules
                        for asset in self._equivalent_assets(rule.collection)})

                compute_rules = self._resultofin_rules(
                        ResultOfComputeIn, asset_set, compute_asset)
                result._sets.append({
                        asset
                        for rule in compute_rules
                        for asset in self._equivalent_assets(rule.collection)})
        return result

    def may_access(self, permissions: Permissions, site: str) -> bool:
        """Checks whether an asset can be at a site.

        This function checks whether the given site has access rights
        to at least one asset in each of the given set of assets.

        Args:
            permissions: Permissions for the asset to check.
            site: A site which needs access.
        """
        def matches_one(asset_set: Set[Identifier], site: str) -> bool:
            for asset in asset_set:
                for rule in self._policy_collection.policies():
                    if isinstance(rule, MayAccess):
                        if rule.asset == asset and rule.site == site:
                            return True
                        if rule.asset == asset and rule.site == '*':
                            return True
            return False

        return all([matches_one(asset_set, site)
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

    def _equivalent_assets(self, asset: Identifier) -> Set[Identifier]:
        """Returns all the assets whose rules apply to an asset.

        These are the asset itself, and all assets that are asset
        collections that the asset is directly or indirectly in.

        Args:
            asset: The asset to find equivalents for.
        """
        cur_assets = set()     # type: Set[Identifier]
        new_assets = {asset}
        while new_assets:
            cur_assets |= new_assets
            new_assets = set()
            for asset in cur_assets:
                for rule in self._policy_collection.policies():
                    if isinstance(rule, InAssetCollection):
                        if rule.asset == asset:
                            if rule.collection not in cur_assets:
                                new_assets.add(rule.collection)
        cur_assets.add(Identifier('*'))
        return cur_assets

    def _resultofin_rules(
            self, typ: Type, asset_set: Set[Identifier],
            compute_asset: Identifier
            ) -> List[ResultOfIn]:
        """Returns all ResultOfIn rules that apply to these assets.

        These are rules that have one of the given assets in their
        asset field, and the given compute_asset or an equivalent one.

        The returned list contains only items of type typ.

        Args:
            typ: Either ResultOfDataIn or ResultOfComputeIn, specifies
                    the kind of rules to return.
            asset_set: Set of data assets to match rules to.
            compute_asset: Compute asset to match rules to.
        """
        def rules_for_asset(asset: Identifier) -> List[ResultOfIn]:
            """Gets all matching rules for the given single asset."""
            result = list()     # type: List[ResultOfIn]
            assets = self._equivalent_assets(asset)
            for rule in self._policy_collection.policies():
                if isinstance(rule, typ):
                    for asset in assets:
                        if rule.data_asset == asset:
                            result.append(rule)
            return result

        comp_assets = self._equivalent_assets(compute_asset)

        rules = list()  # type: List[ResultOfIn]
        for asset in asset_set:
            new_rules = rules_for_asset(asset)
            rules.extend([rule
                          for rule in new_rules
                          if rule.compute_asset in comp_assets])
        return rules


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
            for outp in step.outputs:
                base_item = '{}.@{}'.format(step.name, outp)
                if base_item in permissions:
                    input_perms.append(permissions[base_item])

            perms = self._policy_evaluator.propagate_permissions(
                        input_perms, step.compute_asset_id)

            for output in step.outputs:
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
