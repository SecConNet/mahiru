"""Tools for validating untrusted JSON against an OpenAPI schema.

This module is decidedly less object oriented than the rest of the
code, but that seems to be the best way to do this here. The
validate_json() function is a pure function, and what state there
is for technical reasons is constant. If you consider the declarations
in the YAML file to be code (type definitions), then what we're doing
here is no different from importing something from another module.

"""
from pathlib import Path
from typing import Dict
import ruamel.yaml as yaml

import jsonschema
from jsonschema.validators import RefResolver
from openapi_schema_validator import OAS30Validator

from mahiru.rest.definitions import JSON


ValidationError = jsonschema.ValidationError


def _create_validators() -> Dict[str, OAS30Validator]:
    schemas_file = Path(__file__).parent / 'schemas.yaml'
    with open(schemas_file, 'r') as f:
        schemas = yaml.safe_load(f.read())

    ref_resolver = RefResolver.from_schema(schemas)
    validators = dict()     # type: Dict[str, OAS30Validator]
    for schema_type in schemas['components']['schemas']:
        validators[schema_type] = OAS30Validator(
                schemas['components']['schemas'][schema_type],
                resolver=ref_resolver)
    return validators


_validators = _create_validators()


def validate_json(class_: str, user_input: JSON) -> None:
    """Validates untrusted JSON against a schema class definition.

    Args:
        class_: The name of the class from the schema to validate
            against.
        user_input: Untrusted user input, JSON objects.

    Raises:
        KeyError: If the class is not available for validation.
        ValidationError: If the input was invalid.
    """
    _validators[class_].validate(user_input)
