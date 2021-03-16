"""Client for internal REST APIs."""
import requests

from proof_of_concept.definitions.assets import Asset
from proof_of_concept.rest.serialization import serialize


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
