#!/usr/bin/env python3
"""Trivial test application for in a compute step."""

import json
from pathlib import Path
from typing import Dict

import libmahiru


def min_max(inputs_outputs: Dict[str, Path]) -> None:
    """Mock user code.

    This just calculates the minimum and maximum of a list of numbers,
    thus proving the concept of doing statistical analysis on data.

    The API will get more complicated if we get to direct network
    communication between compute containers, but for now this
    works.

    Args:
        inputs_outputs: Dictionary mapping names of step inputs and
            outputs to a local path to read them from or write them
            to.
    """
    with inputs_outputs['input0'].open('r') as f:
        input0 = json.load(f)

    output0 = [min(input0), max(input0)]

    with inputs_outputs['output0'].open('w') as f:
        json.dump(output0, f)


if __name__ == '__main__':
    libmahiru.register_step(min_max)
    libmahiru.run()
