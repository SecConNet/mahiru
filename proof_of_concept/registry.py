"""Central registry of asset stores and workflow runners."""
from typing import Dict, List

from proof_of_concept.definitions import IAssetStore, ILocalWorkflowRunner


class Registry:
    """Global registry of data stores and local workflow runners.

    In a real system, these would just be identified by the URL, and
    use the DNS to resolve. Here, we use this registry instead.
    """
    def __init__(self) -> None:
        """Create a new registry."""
        self._runners = dict()          # type: Dict[str, ILocalWorkflowRunner]
        self._runner_admins = dict()    # type: Dict[str, str]
        self._stores = dict()           # type: Dict[str, IAssetStore]

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

    def register_store(self, store: IAssetStore) -> None:
        """Register an AssetStore with the Registry.

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


global_registry = Registry()
