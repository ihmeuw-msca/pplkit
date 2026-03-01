pplkit.io.registry
==================

.. automodule:: pplkit.io.registry
   :members:
   :undoc-members:
   :special-members: __call__

Built-in IO Handlers
--------------------

The following loaders and dumpers are registered at import time:

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
