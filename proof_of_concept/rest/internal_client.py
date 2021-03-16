"""Client for internal REST APIs."""
from pathlib import Path
from urllib.parse import quote

import requests

from proof_of_concept.definitions.assets import Asset
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.rest.serialization import serialize


_CHUNK_SIZE = 1024 * 1024


class InternalSiteRestClient:
    """Handles connections to a local site."""
    def __init__(self, endpoint: str) -> None:
        """Create an InternalSiteRestClient.

        Args:
            endpoint: Network location of the site's internal endpoint.

        """
        self._endpoint = endpoint

    def store_asset(self, asset: Asset) -> None:
        """Stores an asset in the site's asset store.

        Args:
            asset: The asset to store.

        """
        r = requests.post(f'{self._endpoint}/assets', json=serialize(asset))
        if r.status_code != 200:
            raise RuntimeError('Error uploading asset to site')

        if asset.image_location is not None:
            with Path(asset.image_location).open('rb') as f:
                r = requests.put(
                        f'{self._endpoint}/assets/{quote(asset.id)}/image',
                        data=f)
                if r.status_code != 204:
                    raise RuntimeError('Error uploading asset image to site')

    def add_rule(self, rule: Rule) -> None:
        """Adds a rule to the site's policy store.

        Args:
            rule: The rule to add.

        """
        r = requests.post(f'{self._endpoint}/rules', json=serialize(rule))
        if r.status_code != 200:
            raise RuntimeError(f'Error adding rule to site: {r.text}')
