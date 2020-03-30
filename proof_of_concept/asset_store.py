"""Asset stores store data and compute assets."""
from typing import Any, Dict, Optional, Tuple

from proof_of_concept.definitions import IAssetStore, Metadata
from proof_of_concept.policy import PolicyEvaluator
from proof_of_concept.permission_calculator import PermissionCalculator
from proof_of_concept.workflow import Job, Workflow


class AssetStore(IAssetStore):
    """A simple store for assets."""
    def __init__(self, name: str, policy_evaluator: PolicyEvaluator) -> None:
        """Create a new empty AssetStore."""
        self.name = name
        self._policy_evaluator = policy_evaluator
        self._permission_calculator = PermissionCalculator(policy_evaluator)
        self._assets = dict()   # type: Dict[str, Any]
        self._metadata = dict()   # type: Dict[str, Metadata]

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return 'AssetStore({})'.format(self.name)

    def store(
            self, name: str, data: Any, metadata: Metadata
            ) -> None:
        """Stores an asset.

        Args:
            name: Name to store asset under.
            data: Asset data to store.
            metadata: Metadata to store.

        Raises:
            KeyError: If there's already an asset with name ``name``.
        """
        if name in self._assets:
            raise KeyError('There is already an asset with that name')
        self._assets[name] = data
        self._metadata[name] = metadata

    def retrieve(
            self, asset_name: str, requester: str) -> Tuple[Any, Metadata]:
        """Retrieves an asset.

        Args:
            asset_name: Name of the asset to retrieve.
            requester: Name of the party making this request.

        Returns:
            The asset data stored under the given name, and its
            metadata.

        Raises:
            KeyError: If no asset with the given name is stored here.
        """
        print(
                '{} servicing request from {} for data {}, '.format(
                    self, requester, asset_name),
                end='')
        try:
            data = self._assets[asset_name]
            metadata = self._metadata[asset_name]
            perms = self._permission_calculator.calculate_permissions(
                    metadata.job)
            perm = perms[metadata.item]
            if not self._policy_evaluator.may_access(perm, requester):
                raise RuntimeError('Security error, access denied')
            print('sending...')
            return data, metadata
        except KeyError:
            print('not found.')
            raise
