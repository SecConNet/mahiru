"""Classes for describing assets."""
from typing import Any, Dict


class Asset:
    """Asset, a representation of a computation or piece of data."""

    def __init__(self, id: str, data: Any, metadata: Any):
        """Constructor.

        TODO: typehint Metadata class for metadata argument, problematic
            due to circular imports
        Args:
            id: Name of the asset
            data: Data related to the asset
            metadata: Metadata related to the asset
        """
        self.id = id
        self.data = data
        self.metadata = metadata


class ComputeAsset(Asset):
    """Compute asset, represents a computing step, i.e. software."""

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
