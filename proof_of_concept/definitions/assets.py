"""Classes for describing assets."""
from typing import Any, Dict, Optional, Union

from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.workflows import Job


class Metadata:
    """Empty base for metadata classes."""
    pass


class DataMetadata(Metadata):
    """Stores metadata for stored data assets.

    Attributes:
        job (Job): A minimal job that will generate this asset.
        item (str): The item in the job's workflow corresponding to
                this asset.

    """
    def __init__(self, job: Job, item: str) -> None:
        """Create a DataMetadata object.

        Args:
            job: A minimal job that will generate this asset.
            item: The item in the job's workflow corresponding to this
                    asset.

        """
        self.job = job
        self.item = item


class ComputeMetadata(Metadata):
    """Stores metadata for stored compute assets.

    Attributes:
        output_base: For each output, the asset id of the empty data
            image to use.

    """
    def __init__(
            self, output_base: Dict[str, str]) -> None:
        """Create a ComputeMetadata object.

        Args:
            output_base: The asset id of the empty data image to use
                for each output.

        """
        self.output_base = dict()
        for out_name, asset_id in output_base.items():
            if isinstance(asset_id, Identifier):
                self.output_base[out_name] = asset_id
            else:
                self.output_base[out_name] = Identifier(asset_id)


class Asset:
    """Asset, a representation of a computation or piece of data."""

    KINDS = ('compute', 'data')

    def __init__(self, id: Union[str, Identifier], kind: str, data: Any,
                 image_location: Optional[str] = None,
                 metadata: Optional[Metadata] = None
                 ) -> None:
        """Create an Asset.

        Assets may have either a data item (a JSON-serialisable Python
        value) associated with them, or reference a Docker image tarball
        containing data or code.

        If the asset is of kind compute and an image location is given,
        then metadata must be an instance of ComputeMetadata. If no
        image location is given, metadata may also be omitted.

        If the asset is of kind data, then metadata if present must be
        of type DataMetadata. If metadata is None in this case, it will
        be set to a trivial DataMetadata object suitable for a dataset.

        Args:
            id: Identifier of the asset
            kind: Either "data" or "compute"
            data: Data related to the asset
            image_location: URL or path to the container image file.
            metadata: Metadata related to the asset, see above.

        """
        if not isinstance(id, Identifier):
            id = Identifier(id)
        if kind not in self.KINDS:
            raise ValueError(f'Invalid kind {kind}')
        if kind == 'data':
            if metadata is None:
                metadata = DataMetadata(Job.niljob(id), 'dataset')
        elif kind == 'compute':
            if image_location is None:
                if metadata is None:
                    metadata = ComputeMetadata({})
            else:
                if not isinstance(metadata, ComputeMetadata):
                    raise RuntimeError('Metadata not specified.')

        self.id = id
        self.kind = kind
        self.data = data
        self.image_location = image_location
        self.metadata = metadata


class ComputeAsset(Asset):
    """Compute asset, represents a computing step, i.e. software."""
    metadata: ComputeMetadata

    def __init__(self, id: Union[str, Identifier], data: Any,
                 image_location: Optional[str] = None,
                 metadata: Optional[Metadata] = None
                 ) -> None:
        """Create a ComputeAsset (see Asset)."""
        super().__init__(id, 'compute', data, image_location, metadata)

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
    metadata: DataMetadata

    def __init__(self, id: Union[str, Identifier], data: Any,
                 image_location: Optional[str] = None,
                 metadata: Optional[Metadata] = None
                 ) -> None:
        """Create a DataAsset (see Asset)."""
        super().__init__(id, 'data', data, image_location, metadata)
