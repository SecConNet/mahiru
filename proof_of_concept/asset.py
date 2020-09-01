"""Classes for describing assets."""
from typing import Any, Dict, Optional

from proof_of_concept.swagger.base_model_ import Model
from proof_of_concept.workflow import Job


class Metadata(Model):
    """Stores metadata for stored assets."""
    swagger_types = {
        'job': Job,
        'item': str
    }

    attribute_map = {
        'job': 'job',
        'item': 'item'
    }

    def __init__(self, job: Job, item: str):
        """Create a metadata object.

        Args:
            job (Job): A minimal job that will generate this asset.
            item (str): The item in the job's workflow corresponding to
                    this asset.
        """
        self.job = job
        self.item = item


class Asset(Model):
    """A representation of a computation or piece of data."""
    swagger_types = {
        'id': str,
        'data': object,
        'metadata': Metadata
    }

    attribute_map = {
        'id': 'id',
        'data': 'data',
        'metadata': 'metadata'
    }

    def __init__(self, id: str, data: object,
                 metadata: Optional[Metadata] = None):
        """Asset.

        Args:
            id: Name of the asset
            data: Data related to the asset
            metadata: Metadata related to the asset. If no metadata is
                passed, metadata is set to a niljob, indicating that this
                is an asset that is not the product of some workflow.
        """
        if metadata is None:
            metadata = Metadata(Job.niljob(id), 'dataset')
        self.id = id
        self.data = data
        self.metadata = metadata

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
