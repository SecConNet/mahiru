"""Tools for validating untrusted JSON against an OpenAPI schema."""
from pathlib import Path
from typing import Dict
import ruamel.yaml as yaml

import jsonschema
from jsonschema.validators import RefResolver
from openapi_schema_validator import OAS30Validator

from proof_of_concept.rest.definitions import JSON


ValidationError = jsonschema.ValidationError


class Validator:
    """Validates untrusted JSON against a schema."""
    def __init__(self, schema: JSON) -> None:
        """Create a Validator.

        Args:
            schema: An OpenAPI schema to check against.
        """
        ref_resolver = RefResolver.from_schema(schema)
        self._validator = dict()   # type: Dict[str, OAS30Validator]
        for schema_type in schema['components']['schemas']:
            self._validator[schema_type] = OAS30Validator(
                    schema['components']['schemas'][schema_type],
                    resolver=ref_resolver)

    def validate(self, class_: str, user_input: JSON) -> None:
        """Validates untrusted JSON against a schema class definition.

        Args:
            class_: The name of the class from the schema to validate
                against.
            user_input: Untrusted user input, JSON objects.

        Raises:
            KeyError: If the class is not available for validation.
            ValidationError: If the input was invalid.
        """
        self._validator[class_].validate(user_input)


# This is a sort-of singleton, which in this case is actually not a
# problem. It's not the best possible solution however, so this is
# temporary. Later, we'll TODO refactor the YAML files and have a
# plain function as the API of this module.

_site_api_file = Path(__file__).parent / 'site_api.yaml'
with open(_site_api_file, 'r') as f:
    _site_api_def = yaml.safe_load(f.read())

site_validator = Validator(_site_api_def)
