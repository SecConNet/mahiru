#!/usr/bin/env python3
"""Simple dummy support library for Mahiru compute assets.

This API is just a placeholder so we can get some simple tests running.
My dreams of the future suggest that one day there may be multiple
steps in a container, and the data access API will be quite a bit more
powerful. That's foreshadowed a tiny bit here by the register_step()
function and the separate run() call.
"""

import json
from pathlib import Path
import os
from os import system
import requests
from typing import Any, Callable, Dict, Optional


# Directory for storing input and output files
_BASE_PATH = Path('/srv/mahiru')


StepFunction = Callable[[Dict[str, Path]], None]


def register_step(step_function: StepFunction) -> None:
    """Register a step with Mahiru.

    This tells Mahiru which function to call to run a step. Maybe one
    day we'll have more than one step in a container, but for now
    there's only one, so we don't have to name it.

    Args:
        step_function: Step function to call.
    """
    global _step_function
    _step_function = step_function


def run() -> None:
    """Run the step."""
    config = _get_step_config()

    in_out = _download_inputs(config['inputs'])
    in_out.update(_output_paths(config['outputs']))

    if _step_function is None:
        raise RuntimeError('Cannot run because no step was registered')
    _step_function(in_out)

    _upload_outputs(config['outputs'])


# Yuck! A global!
_step_function = None   # type: Optional[StepFunction]


def _get_step_config() -> Dict[str, Any]:
    """Get the configuration from the environment.

    This variable will be passed to us by the Mahiru runtime. It
    contains a dictionary with items 'inputs' and 'outputs', both
    of which are dictionaries mapping input/output names to URLs we
    can download them from or should upload them to.
    """
    config_str = os.environ['MAHIRU_STEP_CONFIG']
    return json.loads(config_str)    # type: ignore


def _download_inputs(inputs: Dict[str, str]) -> Dict[str, Path]:
    """Download input files from input data assets.

    This gets the input files from the input data assets, assuming
    that they're named data.json and are served by a WebDAV service,
    and stores them locally for the user's script to read.

    Args:
        inputs: A dictionary mapping input names to URLs.

    Returns:
        A dictionary mapping input names to local paths.
    """
    in_paths = dict()
    for name, endpoint in inputs.items():
        target_dir = _BASE_PATH / name
        target_dir.mkdir()
        target_file = target_dir / 'data.json'
        r = requests.get('{}/data.json'.format(endpoint))
        with target_file.open('wb') as f:
            f.write(r.content)
        in_paths[name] = target_file
    return in_paths


def _output_paths(outputs: Dict[str, str]) -> Dict[str, Path]:
    """Prepare local paths to write outputs to.

    Args:
        outputs: A dictionary mapping output names to URLs.

    Returns:
        A dictionary mapping output names to local paths.
    """
    out_paths = dict()
    for name in outputs:
        target_dir = _BASE_PATH / name
        target_dir.mkdir()
        out_paths[name] = target_dir / 'data.json'
    return out_paths


def _upload_outputs(outputs: Dict[str, str]) -> None:
    """Upload the step outputs to the output data assets.

    Args:
        outputs: A dictionary mapping output names to URLs.
    """
    for name, endpoint in outputs.items():
        output_file = _BASE_PATH / name / 'data.json'
        target_url = '{}/data.json'.format(endpoint)
        with output_file.open('rb') as f:
            requests.put(target_url, data=f.read())
