"""Central registry of remote-accessible things."""
from typing import Dict, List

from proof_of_concept.definitions import IAssetStore, ILocalWorkflowRunner


class Registry:
    """Global registry of remote-accessible things.

    Registers runners, stores, and assets. In a real system, runners
    and stores would be identified by a URL, and use the DNS to
    resolve. For now the registry helps with this.
    """
    def __init__(self) -> None:
        """Create a new registry."""
        self._runners = dict()          # type: Dict[str, ILocalWorkflowRunner]
        self._runner_admins = dict()    # type: Dict[str, str]
        self._stores = dict()           # type: Dict[str, IAssetStore]
        self._store_admins = dict()     # type: Dict[str, str]
        self._assets = dict()           # type: Dict[str, str]

    def register_runner(
            self, admin: str, runner: ILocalWorkflowRunner
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

    def register_store(self, admin: str, store: IAssetStore) -> None:
        """Register an AssetStore with the Registry.

        Args:
            admin: The party administrating this runner.
            store: The data store to register.
        """
        if store.name in self._stores:
            raise RuntimeError('There is already a store with this name')
        self._stores[store.name] = store
        self._store_admins[store.name] = admin

    def register_asset(self, asset_id: str, store_name: str) -> None:
        """Register an Asset with the Registry.

        Args:
            asset_id: The id of the asset to register.
            store_name: Name of the store where it can be found.
        """
        if asset_id in self._assets:
            raise RuntimeError('There is already an asset with this name')
        self._assets[asset_id] = store_name

    def list_runners(self) -> List[str]:
        """List names of all registered runners.

        Returns:
            A list of names as strings.
        """
        return list(self._runners.keys())

    def get_runner(self, name: str) -> ILocalWorkflowRunner:
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

    def get_store(self, name: str) -> IAssetStore:
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

    def get_store_admin(self, name: str) -> str:
        """Look up who administrates a given store.

        Args:
            name: The name of the store to look up.

        Return:
            The name of the party administrating it.

        Raises:
            KeyError: If no runner with the given name is registered.
        """
        return self._store_admins[name]

    def get_asset_location(self, asset_id: str) -> str:
        """Returns the name of the store this asset is in.

        Args:
            asset_id: ID of the asset to find.

        Return:
            The store it can be found in.

        Raises:
            KeyError: If no asset with the given id is registered.
        """
        return self._assets[asset_id]


global_registry = Registry()
