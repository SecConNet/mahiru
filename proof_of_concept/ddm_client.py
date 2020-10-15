"""Functionality for connecting to other DDM sites."""
import logging
from pathlib import Path
import requests
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
import ruamel.yaml as yaml

from proof_of_concept.asset import Asset
from proof_of_concept.definitions import (
        IAssetStore, ILocalWorkflowRunner, IPolicyServer, PartyDescription,
        Plan, SiteDescription)
from proof_of_concept.policy import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfComputeIn,
        ResultOfDataIn, Rule)
from proof_of_concept.serialization import (
        deserialize_asset, serialize, serialize_job, serialize_plan)
from proof_of_concept.registry import global_registry, RegisteredObject
from proof_of_concept.replication import ObjectValidator, Replica
from proof_of_concept.replication_rest import ReplicationClient
from proof_of_concept.validation import Validator
from proof_of_concept.workflow import Job, Workflow


logger = logging.getLogger(__name__)


_PolicyClient = ReplicationClient[Rule]


class RuleValidator(ObjectValidator[Rule]):
    """Validates incoming policy rules by checking signatures."""
    def __init__(self, ddm_client: 'DDMClient', namespace: str) -> None:
        """Create a RuleValidator.

        Checks that rules apply to the given namespace, and that they
        have been signed by the owner of that namespace.

        Args:
            ddm_client: A DDMClient to use.
            namespace: The namespace to expect rules for.
        """
        self._key = ddm_client.get_public_key_for_ns(namespace)
        self._namespace = namespace

    def is_valid(self, rule: Rule) -> bool:
        """Return True iff the rule is properly signed."""
        if isinstance(rule, ResultOfDataIn):
            namespace = rule.data_asset[3:].split('.')[0]
        elif isinstance(rule, ResultOfComputeIn):
            namespace = rule.compute_asset[3:].split('.')[0]
        elif isinstance(rule, MayAccess):
            namespace = rule.asset[3:].split('.')[0]
        elif isinstance(rule, InAssetCollection):
            namespace = rule.asset[3:].split('.')[0]
        elif isinstance(rule, InPartyCollection):
            namespace = rule.collection[3:].split('.')[0]

        if namespace != self._namespace:
            return False
        return rule.has_valid_signature(self._key)


class DDMClient:
    """Handles connecting to global registry, runners and stores."""
    def __init__(self, party: str) -> None:
        """Create a DDMClient.

        Args:
            party: The party on whose behalf this client acts.

        """
        self._party = party
        # TODO: This will be passed in as an argument later.
        self._registry_endpoint = 'http://localhost:4413'

        # Set up connection to registry
        registry_api_file = Path(__file__).parent / 'registry_api.yaml'
        with open(registry_api_file, 'r') as f:
            registry_api_def = yaml.safe_load(f.read())

        registry_validator = Validator(registry_api_def)

        registry_client = ReplicationClient[RegisteredObject](
                self._registry_endpoint + '/updates', registry_validator,
                'RegistryUpdate', 'RegisteredObject')

        self._registry_replica = Replica[RegisteredObject](
                registry_client, on_update=self._on_registry_update)

        # Set up connections to sites
        site_api_file = Path(__file__).parent / 'site_api.yaml'
        with open(site_api_file, 'r') as f:
            site_api_def = yaml.safe_load(f.read())

        self._site_validator = Validator(site_api_def)

        self._policy_replicas = dict()  # type: Dict[str, Replica[Rule]]

        # Get initial data
        self._registry_replica.update()

    def register_party(self, description: PartyDescription) -> None:
        """Register a party with the Registry.

        Args:
            description: Description of the party.

        """
        requests.post(
                self._registry_endpoint + '/parties',
                json=serialize(description))

    def register_site(self, description: SiteDescription) -> None:
        """Register a site with the Registry.

        Args:
            description: Description of the site.

        """
        requests.post(
                self._registry_endpoint + '/sites',
                json=serialize(description))

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
            owner = self._get_party(site.owner_name)
            if owner is None:
                raise RuntimeError(f'Registry replica is broken')
            return owner.public_key
        raise RuntimeError(f'No site with namespace {namespace} found')

    def get_rules(self) -> Dict[str, Set[Rule]]:
        """List all known rules in all namespaces.

        Return:
            A dict mapping namespaces to their governing rules.

        """
        self._registry_replica.update()
        for replica in self._policy_replicas.values():
            replica.update()
        result = {
                namespace: replica.objects
                for namespace, replica in self._policy_replicas.items()}
        return result

    def list_sites_with_runners(self) -> List[str]:
        """Returns a list of id's of sites with runners."""
        self._registry_replica.update()
        sites = list()    # type: List[str]
        for o in self._registry_replica.objects:
            if isinstance(o, SiteDescription):
                if o.runner:
                    sites.append(o.name)
        return sites

    def get_site_administrator(self, site_name: str) -> str:
        """Returns the name of the party administrating a site."""
        site = self._get_site('name', site_name)
        if site is not None:
            return site.admin_name
        raise RuntimeError('Site {} not found'.format(site_name))

    @staticmethod
    def get_asset_location(asset_id: str) -> str:
        """Returns the name of the site which stores this asset."""
        return global_registry.get_asset_location(asset_id)

    def retrieve_asset(self, site_name: str, asset_id: str
                       ) -> Asset:
        """Obtains a data item from a store."""
        site = self._get_site('name', site_name)
        if site is not None and site.store is not None:
            safe_asset_id = quote(asset_id, safe='')
            r = requests.get(
                    f'{site.endpoint}/assets/{safe_asset_id}',
                    params={'requester': self._party})
            if r.status_code == 404:
                raise KeyError('Asset not found')
            elif not r.ok:
                raise RuntimeError('Server error when retrieving asset')

            asset_json = r.json()
            self._site_validator.validate('Asset', asset_json)
            return deserialize_asset(asset_json)

        raise RuntimeError(f'Site or store at site {site_name} not found')

    def submit_job(self, site_name: str, job: Job, plan: Plan) -> None:
        """Submits a job for execution to a local runner.

        Args:
            site_name: The site to submit to.
            job: The job to submit.
            plan: The plan to execute the workflow to.

        """
        site = self._get_site('name', site_name)
        if site is not None and site.runner:
            data = {
                    'job': serialize_job(job),
                    'plan': serialize_plan(plan)}
            requests.post(f'{site.endpoint}/jobs', json=data)
        else:
            raise RuntimeError(f'Site or runner at site {site_name} not found')

    def _get_party(self, name: str) -> Optional[PartyDescription]:
        """Returns the party with the given name."""
        self._registry_replica.update()
        for o in self._registry_replica.objects:
            if isinstance(o, PartyDescription):
                if o.name == name:
                    return o
        return None

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

    def _on_registry_update(
            self, created: Set[RegisteredObject],
            deleted: Set[RegisteredObject]) -> None:
        """Updates policy clients/replicas when sites are updated."""
        for o in created:
            if isinstance(o, SiteDescription) and o.namespace:
                client = ReplicationClient[Rule](
                        o.endpoint + '/rules/updates', self._site_validator,
                        'PolicyUpdate', 'Rule')

                validator = RuleValidator(self, o.namespace)
                self._policy_replicas[o.namespace] = Replica[Rule](
                        client, validator)

        for o in deleted:
            if isinstance(o, SiteDescription) and o.namespace:
                del(self._policy_replicas[o.namespace])
