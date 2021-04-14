"""Client for internal REST APIs."""
from pathlib import Path
from urllib.parse import quote
import time

import requests

from proof_of_concept.definitions.assets import Asset
from proof_of_concept.definitions.execution import JobResult
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.definitions.workflows import Job
from proof_of_concept.rest.serialization import deserialize, serialize
from proof_of_concept.rest.validation import site_validator


_CHUNK_SIZE = 1024 * 1024
_JOB_RESULT_WAIT_TIME = 0.5     # s


class InternalSiteRestClient:
    """Handles connections to a local site."""
    def __init__(self, site: str, endpoint: str) -> None:
        """Create an InternalSiteRestClient.

        Args:
            site: Site this client is at.
            endpoint: Network location of the site's internal endpoint.

        """
        self._site = site
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

    def submit_job(self, job: Job) -> str:
        """Submits a job to the DDM via the local site.

        Args:
            job: The job to execute.

        Returns:
            The new job's id.

        """
        r = requests.post(
                f'{self._endpoint}/jobs', json=serialize(job),
                params={'requester': self._site}, allow_redirects=False)
        if r.status_code != 303:
            raise RuntimeError(f'Error submitting job: {r.text}')
        if 'location' not in r.headers:
            raise RuntimeError('Invalid server response when submitting job')
        job_uri = r.headers['location']
        prefix = f'{self._endpoint}/jobs/'
        if not job_uri.startswith(prefix):
            raise RuntimeError(
                    'Unexpected server response when submitting job')
        return job_uri

    def is_job_done(self, job_id: str) -> bool:
        """Checks whether a job is done.

        Args:
            job_id: The job's id from :func:`submit_job`.

        Returns:
            True iff the job is done.

        Raises:
            KeyError: if the job id does not exist.
        """
        return self._get_job_result(job_id).is_done

    def get_job_result(self, job_id: str) -> JobResult:
        """Gets the results of a submitted job.

        This waits until the job is done before returning.

        Args:
            job_id: The job's id from :func:`submit_job`.

        Returns:
            The job's results.

        Raises:
            KeyError: If the job id does not exist.
            RuntimeError: If there was an error communicating with the
                    server.
        """
        while True:
            result = self._get_job_result(job_id)
            if result.is_done:
                break
            time.sleep(_JOB_RESULT_WAIT_TIME)
        return result

    def _get_job_result(self, job_id: str) -> JobResult:
        """Gets the job's current result from the server."""
        r = requests.get(job_id)
        if r.status_code == 404:
            raise KeyError('Job not found')
        if r.status_code != 200:
            raise RuntimeError(f'Error getting job status: {r.text}')
        site_validator.validate('JobResult', r.json())
        return deserialize(JobResult, r.json())