"""Classes for describing assets."""
from typing import Any, Dict, Optional, Union

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.workflows import Job


class Metadata:
    """Stores metadata for stored assets.

    Attributes:
        job (Job): A minimal job that will generate this asset.
        item (str): The item in the job's workflow corresponding to
                this asset.

    """

    def __init__(self, job: Job, item: str) -> None:
        """Create a Metadata object.

        Args:
            job: A minimal job that will generate this asset.
            item: The item in the job's workflow corresponding to this
                    asset.

        """
        self.job = job
        self.item = item


class Asset:
    """Asset, a representation of a computation or piece of data."""

    def __init__(self, id: Union[str, Identifier], data: Any,
                 image_location: Optional[str] = None,
                 metadata: Optional[Metadata] = None
                 ) -> None:
        """Create an Asset.

        Assets may have either a data item (a JSON-serialisable Python
        value) associated with them, or reference Docker image tarball
        containing data or code.

        Args:
            id: Identifier of the asset
            data: Data related to the asset
            image_location: URL or path to the container image file.
            metadata: Metadata related to the asset. If no metadata is
                passed, metadata is set to a niljob, indicating that
                this is an asset that is not the product of some
                workflow.

        """
        if not isinstance(id, Identifier):
            id = Identifier(id)
        if metadata is None:
            metadata = Metadata(Job.niljob(id), 'dataset')
        self.id = id
        self.data = data
        self.image_location = image_location
        self.metadata = metadata


class ComputeAsset(Asset):
    """Compute asset, represents a computing step, i.e. software."""

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Run compute step.

        For now this executes one of several simple
        algorithms depending on the id attribute.

        Args:
            inputs: inputs for the compute step, a dictionary keyed by
                input name with corresponding values.

        Returns:
            outputs: outputs for the compute step, a dictionary keyed by
                output name with corresponding values.

        """
        outputs = dict()  # type: Dict[str, Any]
        if 'combine' in self.id:
            outputs['y'] = [inputs['x1'], inputs['x2']]
        elif 'anonymise' in self.id:
            outputs['y'] = [x - 10 for x in inputs['x1']]
        elif 'aggregate' in self.id:
            outputs['y'] = sum(inputs['x1']) / len(inputs['x1'])
        elif 'addition' in self.id:
            outputs['y'] = inputs['x1'] + inputs['x2']
        else:
            raise RuntimeError('Unknown compute asset')
        return outputs


class DataAsset(Asset):
    """Data asset, represents a data set."""
