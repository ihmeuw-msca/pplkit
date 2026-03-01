==========
Quickstart
==========

This guide walks through the main features of **pplkit**: managing directories
with :class:`~pplkit.io.IOManager`, loading and dumping data by file extension,
and registering custom I/O handlers.


IOManager
---------

The :class:`~pplkit.io.IOManager` is the primary entry point. It pairs named
directory paths with automatic file-format detection so that you can read and
write data with a single call.

.. code-block:: python

    from pplkit.io import IOManager

    iom = IOManager(input="path/to/input", output="path/to/output")

Registered directories are accessible via bracket notation:

.. code-block:: python

    >>> iom["input"]
    PosixPath('path/to/input')
    >>> iom["output"]
    PosixPath('path/to/output')

Loading Data
~~~~~~~~~~~~

Use :meth:`~pplkit.io.IOManager.load` to read a file. The format is determined
automatically from the file extension:

.. code-block:: python

    # Load a CSV file as a pandas DataFrame
    df = iom.load("data.csv", key="input")

    # Load a JSON file
    config = iom.load("config.json", key="input")

    # Load a YAML file
    params = iom.load("params.yaml", key="input")

You can also pass format-specific options through as keyword arguments. These
are forwarded directly to the underlying reader:

.. code-block:: python

    df = iom.load("data.csv", key="input", usecols=["col_a", "col_b"])

Dumping Data
~~~~~~~~~~~~

Use :meth:`~pplkit.io.IOManager.dump` to write data. Again, the format is
inferred from the file extension:

.. code-block:: python

    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

    # Write as CSV
    iom.dump(df, "results.csv", key="output")

    # Write as Parquet
    iom.dump(df, "results.parquet", key="output")

    # Write a plain dict as JSON
    iom.dump({"score": 0.95}, "metrics.json", key="output")

By default, :meth:`~pplkit.io.IOManager.dump` creates the parent directory if
it does not already exist. Pass ``mkdir=False`` to disable this behaviour.

Supported Formats
~~~~~~~~~~~~~~~~~

The following formats are registered out of the box:

.. list-table::
   :header-rows: 1

   * - Extension
     - Object Type
     - Notes
   * - ``.csv``
     - ``pandas.DataFrame``
     -
   * - ``.parquet``
     - ``pandas.DataFrame``
     - Uses the PyArrow engine
   * - ``.json``
     - ``object``
     -
   * - ``.yaml`` / ``.yml``
     - ``object``
     - Uses ``SafeLoader`` / ``SafeDumper``
   * - ``.toml``
     - ``dict``
     -
   * - ``.pkl`` / ``.pickle``
     - ``object``
     - Uses :mod:`dill` for extended pickle support


Using Loaders and Dumpers Directly
----------------------------------

The :func:`~pplkit.io.get_loader` and :func:`~pplkit.io.get_dumper` functions
can be used independently of ``IOManager`` for lower-level control:

.. code-block:: python

    from pplkit.io import get_loader, get_dumper

    # Get a loader by file extension
    loader = get_loader(".json")
    data = loader("path/to/data.json")

    # Get a dumper by file extension and object type
    dumper = get_dumper(".json", obj_type=object)
    dumper(data, "path/to/output.json")


Registering Custom Handlers
----------------------------

You can register your own loader or dumper for any file extension and object
type using the :func:`~pplkit.io.register_loader` and
:func:`~pplkit.io.register_dumper` decorators.

For example, to add support for NumPy ``.npy`` files:

.. code-block:: python

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

Once registered, the new format works seamlessly with ``IOManager``:

.. code-block:: python

    iom = IOManager(output="path/to/output")

    arr = np.array([1.0, 2.0, 3.0])
    iom.dump(arr, "array.npy", key="output")

    loaded = iom.load("array.npy", key="output")


Suffix Aliases
--------------

pplkit ships with two suffix aliases pre-registered:

* ``.pickle`` → ``.pkl``
* ``.yml`` → ``.yaml``

You can add your own aliases with :func:`~pplkit.io.register_suffix_alias`:

.. code-block:: python

    from pplkit.io import register_suffix_alias

    register_suffix_alias(suffix=".yaml", suffix_alias=".conf")

After this, files ending in ``.conf`` will be handled by the YAML
loader/dumper.
