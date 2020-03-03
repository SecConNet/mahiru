from typing import Any, Dict, Optional, Tuple

from definitions import IAssetStore
from policy import PolicyManager
from policy_evaluator import PolicyEvaluator
from workflow import Job, Workflow


class AssetStore(IAssetStore):
    """A simple store for assets.
    """
    def __init__(self, name: str, policy_manager: PolicyManager) -> None:
        """Create a new empty AssetStore.
        """
        self.name = name
        self._policy_manager = policy_manager
        self._policy_evaluator = PolicyEvaluator(policy_manager)
        self._assets = dict()   # type: Dict[str, Any]
        self._provenance = dict()   # type: Dict[str, Job]

    def __repr__(self) -> str:
        return 'AssetStore({})'.format(self.name)

    def store(
            self, name: str, data: Any, provenance: Optional[Job] = None
            ) -> None:
        """Stores an asset.

        Args:
            name: Name to store asset under.
            data: Asset data to store.
            provenance: Workflow that generated this asset.

        Raises:
            KeyError: If there's already an asset with name ``name``.
        """
        if name in self._assets:
            raise KeyError('There is already an asset with that name')
        self._assets[name] = data
        if provenance is None:
            full_name = '{}:{}'.format(self.name, name)
            self._provenance[name] = Job(Workflow([], {}, []), {'': full_name})
        else:
            self._provenance[name] = provenance

    def retrieve(self, asset_name: str, requester: str) -> Tuple[Any, Job]:
        """Retrieves an asset.

        Args:
            asset_name: Name of the asset to retrieve.
            requester: Name of the party making this request.

        Returns:
            The asset data stored under the given name, and its
            provenance.

        Raises:
            KeyError: If no asset with the given name is stored here.
        """
        print(
                '{} servicing request from {} for data {}, '.format(
                    self, requester, asset_name),
                end='')
        try:
            data = self._assets[asset_name]
            provenance = self._provenance[asset_name]
            perms = self._policy_evaluator.calculate_permissions(provenance)

            perms_asset_name = asset_name
            if '/' not in perms_asset_name:
                # will clean this up later when we get a better data locating
                # solution.
                perms_asset_name = '{}:{}'.format(self.name, asset_name)

            perm = perms[perms_asset_name]
            if not self._policy_manager.may_access(perm, requester):
                raise RuntimeError('Security error, access denied')
            print('sending...')
            return data, provenance
        except KeyError:
            print('not found.')
            raise
