"""Definitions for executing jobs."""
from typing import Dict

from mahiru.definitions.assets import Asset
from mahiru.definitions.workflows import Job, Plan


class JobResult:
    """The result of a job submitted to a site by a client.

    Objects of this class are returned by the internal workflow
    submission API in response to a user's request for a job they
    have submitted.
    """
    def __init__(
            self,
            job: Job, plan: Plan, is_done: bool, outputs: Dict[str, Asset]
            ) -> None:
        """Create a JobResult.

        Args:
            job: The job as submitted.
            plan: The plan used to execute this job.
            is_done: Whether the job has finished.
            outputs: The job's output assets.
        """
        self.job = job
        self.plan = plan
        self.is_done = is_done
        self.outputs = outputs
