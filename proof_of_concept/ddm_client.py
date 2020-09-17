"""Functionality for connecting to other DDM sites."""
from typing import Any, List, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from proof_of_concept.asset import Asset
from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, IPolicyServer, Plan)
from proof_of_concept.workflow import Job, Workflow
from proof_of_concept.registry import (
        global_registry, RegisteredObject, SiteDescription)
from proof_of_concept.replication import Replica


class DDMClient:
    """Handles connecting to global registry, runners and stores."""
    def __init__(self, party: str) -> None:
        """Create a DDMClient.

        Args:
            party: The party on whose behalf this client acts.

        """
        self._party = party
        self._registry_replica = Replica[RegisteredObject](
                global_registry.replication_server)

    def register_party(
            self, name: str, public_key: RSAPublicKey) -> None:
        """Register a party with the Registry.

        Args:
            name: Name of the party.
            public_key: Public key of this party.

        """
        global_registry.register_party(name, public_key)

    def register_site(
            self,
            name: str,
            owner_name: str,
            admin_name: str,
            runner: Optional[ILocalWorkflowRunner] = None,
            store: Optional[IAssetStore] = None,
            namespace: Optional[str] = None,
            policy_server: Optional[IPolicyServer] = None
            ) -> None:
        """Register a site with the Registry.

        Args:
            name: Name of the site.
            owner_name: Name of the owning party.
            admin_name: Name of the administrating party.
            runner: This site's local workflow runner.
            store: This site's asset store.
            namespace: Namespace managed by this site's policy server.
            policy_server: This site's policy server.
        """
        global_registry.register_site(
                name, owner_name, admin_name, runner, store, namespace,
                policy_server)

    def register_asset(self, asset_id: str, site_name: str) -> None:
        """Register an Asset with the Registry.

        Args:
            asset_id: The id of the asset to register.
            site_name: Name of the site where it can be found.

        """
        global_registry.register_asset(asset_id, site_name)

    def get_public_key_for_ns(self, namespace: str) -> RSAPublicKey:
        """Get the public key of the owner of a namespace."""
        site = self._get_site('namespace', namespace)
        if site is not None:
            return site.owner.public_key
        raise RuntimeError(f'No site with namespace {namespace} found')

    def list_sites_with_runners(self) -> List[str]:
        """Returns a list of id's of sites with runners."""
        self._registry_replica.update()
        sites = list()    # type: List[str]
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.runner is not None:
                    sites.append(o.name)
        return sites

    def get_site_administrator(self, site_name: str) -> str:
        """Returns the name of the party administrating a site."""
        site = self._get_site('name', site_name)
        if site is not None:
            return site.admin.name
        raise RuntimeError('Site {} not found'.format(site_name))

    def list_policy_servers(self) -> List[Tuple[str, IPolicyServer]]:
        """List all known policy servers.

        Return:
            A list of all registered policy servers and their
                    namespaces.

        """
        self._registry_replica.update()
        result = list()     # type: List[Tuple[str, IPolicyServer]]
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.namespace is not None and o.policy_server is not None:
                    result.append((o.namespace, o.policy_server))
        return result

    @staticmethod
    def get_asset_location(asset_id: str) -> str:
        """Returns the name of the site which stores this asset."""
        return global_registry.get_asset_location(asset_id)

    def retrieve_asset(self, site_name: str, asset_id: str
                       ) -> Asset:
        """Obtains a data item from a store."""
        site = self._get_site('name', site_name)
        if site is not None and site.store is not None:
            return site.store.retrieve(asset_id, self._party)
        raise RuntimeError(f'Site or store at site {site_name} not found')

    def submit_job(self, site_name: str, job: Job, plan: Plan) -> None:
        """Submits a job for execution to a local runner.

        Args:
            site_name: The site to submit to.
            job: The job to submit.
            plan: The plan to execute the workflow to.

        """
        site = self._get_site('name', site_name)
        if site is not None:
            if site.runner is not None:
                site.runner.execute_job(job, plan)
                return
        raise RuntimeError(f'Site or runner at site {site_name} not found')

    def _get_site(
            self, attr_name: str, value: Any) -> Optional[SiteDescription]:
        """Returns a site with a given attribute value."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                a = getattr(o, attr_name)
                if a is not None and a == value:
                    return o
        return None
