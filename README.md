[![build](https://github.com/ihmeuw-msca/pplkit/workflows/build/badge.svg)](https://github.com/ihmeuw-msca/pplkit/actions)
[![PyPI](https://badge.fury.io/py/pplkit.svg)](https://badge.fury.io/py/pplkit)
[![docs](https://img.shields.io/badge/docs-latest-blue.svg)](https://ihmeuw-msca.github.io/pplkit/)

# Pipeline Building Toolkit

`pplkit` is a lightweight Python library that provides a unified interface for
reading and writing data across multiple file formats, including CSV, JSON,
YAML, TOML, Parquet, and Pickle. It simplifies data pipeline workflows by
managing named directories and automatically dispatching I/O operations based
on file extensions.

For full documentation, visit the
[pplkit docs](https://ihmeuw-msca.github.io/pplkit/).

## Installation

```bash
pip install pplkit
```

## Quick Start

```python
from pplkit.io import IOManager

iom = IOManager(input="path/to/input", output="path/to/output")

# Load data based on file extension
data = iom.load("data.csv", key="input")

# Dump data based on file extension
iom.dump(data, "data.parquet", key="output")
```

## Using Loaders and Dumpers Directly

The `get_loader` and `get_dumper` functions can be used independently of
`IOManager` for lower-level control:

```python
from pplkit.io import get_loader, get_dumper

# Get a loader/dumper by file extension
loader = get_loader(".json")
dumper = get_dumper(".json", obj_type=object)

data = loader("path/to/data.json")
dumper(data, "path/to/output.json")
```

## Registering Custom Handlers

You can register your own loader or dumper for any suffix and object type:

```python
import pathlib
import typing

import numpy as np
import numpy.typing as npt

from pplkit.io import register_loader, register_dumper


@register_loader(".npy", object)
def load_npy(path: pathlib.Path, **options: typing.Any) -> npt.NDArray:
    return np.load(path, **options)


@register_dumper(".npy", object)
def dump_npy(
    obj: npt.NDArray, path: pathlib.Path, **options: typing.Any
) -> None:
    np.save(path, obj, **options)
```
