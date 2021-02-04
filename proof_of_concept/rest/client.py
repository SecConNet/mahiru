"""Clients for REST APIs."""
from pathlib import Path
import requests
from urllib.parse import quote

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.assets import Asset
from proof_of_concept.definitions.workflows import JobSubmission
from proof_of_concept.rest.serialization import deserialize, serialize
from proof_of_concept.rest.validation import Validator
from proof_of_concept.components.registry_client import RegistryClient


class SiteRestClient:
    """Handles connecting to other sites' runners and stores."""
    def __init__(
            self, site: str, site_validator: Validator,
            registry_client: RegistryClient
            ) -> None:
        """Create a SiteRestClient.

        Args:
            site: The site at which this client acts.
            site_validator: A validator for the Site REST API.
            registry_client: A registry client to get sites from.

        """
        self._site = site
        self._site_validator = site_validator
        self._registry_client = registry_client

    def retrieve_asset(self, site_id: Identifier, asset_id: Identifier
                       ) -> Asset:
        """Obtains an asset from a store."""
        try:
            site = self._registry_client.get_site_by_id(site_id)
        except KeyError:
            raise RuntimeError(f'Site or store at site {site_id} not found')

        if site.store is not None:
            safe_asset_id = quote(asset_id, safe='')
            r = requests.get(
                    f'{site.endpoint}/assets/{safe_asset_id}',
                    params={'requester': self._site})
            if r.status_code == 404:
                raise KeyError('Asset not found')
            elif not r.ok:
                raise RuntimeError('Server error when retrieving asset')

            asset_json = r.json()
            self._site_validator.validate('Asset', asset_json)
            return deserialize(Asset, asset_json)

        raise ValueError(f'Site {site_id} does not have a store')

    def retrieve_asset_image(self, asset_location: str, target: Path) -> None:
        """Obtains an asset image from a store.

        This downloads the image at the given location into a file at
        the given path.

        Args:
            asset_location: URL of the image to download.
            target: Path of the file to save.
        """
        with requests.get(
                asset_location,
                params={'requester': self._site},
                stream=True) as r:
            if r.status_code == 404:
                raise KeyError('Asset image not found')
            elif not r.ok:
                raise RuntimeError('Server error when retrieving asset image')

            if r.headers.get('Transfer-Encoding', '') == 'chunked':
                chunk_size = None
            else:
                chunk_size = 1024 * 1024

            with target.open('wb') as f:
                for chunk in r.iter_content(chunk_size):
                    if chunk:
                        f.write(chunk)

    def submit_job(
            self, site_id: Identifier, submission: JobSubmission) -> None:
        """Submits a job for execution to a local runner.

        Args:
            site_id: The site to submit to.
            submission: The job submision to send.

        """
        try:
            site = self._registry_client.get_site_by_id(site_id)
        except KeyError:
            raise RuntimeError(f'Site or runner at site {site_id} not found')

        if site.runner:
            requests.post(f'{site.endpoint}/jobs', json=serialize(submission))
        else:
            raise ValueError(f'Site {site_id} does not have a runner')
