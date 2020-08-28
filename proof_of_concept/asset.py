"""Classes for describing assets."""
from typing import Any, Dict, Optional

from proof_of_concept.swagger import util
from proof_of_concept.swagger.base_model_ import Model
from proof_of_concept.workflow import Job


class Metadata(Model):
    """Stores metadata for stored assets.
    """
    def __init__(self, job: Job = None, item: str = None):  # noqa: E501
        """Metadata.

        Attributes:
            job (Job): A minimal job that will generate this asset.
            item (str): The item in the job's workflow corresponding to
                    this asset.
        """
        self.swagger_types = {
            'job': Job,
            'item': str
        }

        self.attribute_map = {
            'job': 'job',
            'item': 'item'
        }
        self._job = job
        self._item = item

    @classmethod
    def from_dict(cls, dikt) -> 'Metadata':
        """Returns the dict as a model."""
        return util.deserialize_model(dikt, cls)

    @property
    def job(self) -> Job:
        """Gets the job of this Metadata."""
        return self._job

    @job.setter
    def job(self, job: Job):
        """Sets the job of this Metadata."""
        if job is None:
            raise ValueError("Invalid value for `job`, must not be `None`")

        self._job = job

    @property
    def item(self) -> str:
        """Gets the item of this Metadata."""
        return self._item

    @item.setter
    def item(self, item: str):
        """Sets the item of this Metadata."""
        if item is None:
            raise ValueError("Invalid value for `item`, must not be `None`")

        self._item = item


class Asset(Model):
    """A representation of a computation or piece of data."""

    def __init__(self, id: str = None,
                 data: object = None, metadata: Optional[Metadata] = None):
        """Asset.

        Args:
            id: Name of the asset
            data: Data related to the asset
            metadata: Metadata related to the asset. If no metadata is
                passed, metadata is set to a niljob, indicating that this is
                an asset that is not the product of some workflow.
        """
        self.swagger_types = {
            'id': str,
            'data': object,
            'metadata': Metadata
        }

        self.attribute_map = {
            'id': 'id',
            'data': 'data',
            'metadata': 'metadata'
        }
        if metadata is None:
            metadata = Metadata(Job.niljob(id), 'dataset')
        self._id = id
        self._data = data
        self._metadata = metadata

    @classmethod
    def from_dict(cls, dikt) -> 'Asset':
        """Returns the dict as a model"""
        return util.deserialize_model(dikt, cls)

    # TODO: Do I need all this property and setters?

    @property
    def id(self) -> str:
        """Gets the id of this Asset."""
        return self._id

    @id.setter
    def id(self, id: str):
        """Sets the id of this Asset.
        """
        if id is None:
            raise ValueError("Invalid value for `id`,"
                             "must not be `None`")
        self._id = id

    @property
    def data(self) -> object:
        """Gets the data of this Asset.
        """
        return self._data

    @data.setter
    def data(self, data: object):
        """Sets the data of this Asset.
        """
        if data is None:
            raise ValueError("Invalid value for `data`, must not be `None`")  # noqa: E501

        self._data = data

    @property
    def metadata(self) -> Metadata:
        """Gets the metadata of this Asset.
        """
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: Metadata):
        """Sets the metadata of this Asset.
        """
        self._metadata = metadata

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Run compute step.

         For now this executes one of several simple
         algorithms depending on the name attribute.

        Args:
            inputs: inputs for the compute step, a dictionary keyed by
            variable name with corresponding values.

        Returns:
            outputs: outputs for the compute step, a dictionary keyed by
                variable name with corresponding values.

        """
        outputs = dict()  # type: Dict[str, Any]
        if self.id == 'id:ddm_ns/software/combine':
            outputs['y'] = [inputs['x1'], inputs['x2']]
        elif self.id == 'id:ddm_ns/software/anonymise':
            outputs['y'] = [x - 10 for x in inputs['x1']]
        elif self.id == 'id:ddm_ns/software/aggregate':
            outputs['y'] = sum(inputs['x1']) / len(inputs['x1'])
        elif self.id == 'id:party2_ns/software/addition':
            outputs['y'] = inputs['x1'] + inputs['x2']
        else:
            raise RuntimeError('Unknown compute asset')
        return outputs
