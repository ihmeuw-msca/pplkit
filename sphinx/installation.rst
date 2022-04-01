=================
Installing pplkit
=================

Python version
--------------

The package :code:`pplkit` is written in Python
and requires Python 3.10 or later.

Install pplkit
--------------

Regmod package is distributed at
`PyPI <https://pypi.org/project/pplkit/>`_.
To install the package:

.. code::

   pip install pplkit

For developers
--------------

For developers, you can clone the repository and install the package in the
development mode.

.. code::

    git clone https://github.com/ihmeuw-msca/pplkit.git
    cd pplkit
    pip install -e ".[test,docs]"