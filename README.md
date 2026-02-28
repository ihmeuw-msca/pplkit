[![build](https://github.com/ihmeuw-msca/pplkit/workflows/build/badge.svg)](https://github.com/ihmeuw-msca/pplkit/actions)
[![PyPI](https://badge.fury.io/py/pplkit.svg)](https://badge.fury.io/py/pplkit)

# Pipeline Building Toolkit

`pplkit` is a lightweight Python library that provides a unified interface for
reading and writing data across multiple file formats, including CSV, JSON,
YAML, TOML, Parquet, and Pickle. It simplifies data pipeline workflows by
managing named directories and automatically dispatching I/O operations based
on file extensions.

## Installation

```bash
pip install pplkit
```

## Quick Start

```python
from pplkit.data import DataInterface

dataif = DataInterface(input="path/to/input", output="path/to/output")

# Load data based on file extension
data = dataif.load("data.csv", key="input")

# Dump data based on file extension
dataif.dump(data, "data.parquet", key="output")
```
