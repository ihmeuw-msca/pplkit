"""Registry-based I/O layer for loading and dumping data by file extension.

This module defines three class-level registries:

* :class:`IORegistry` – shared base with suffix-alias resolution, object-type
  resolution, and a ``register`` decorator.
* :class:`LoaderRegistry` – concrete registry for **loaders** (file → object).
* :class:`DumperRegistry` – concrete registry for **dumpers** (object → file).

Loader/dumper functions for CSV, Pickle, YAML, Parquet, JSON, and TOML are
registered at module import time, so callers can immediately do:

>>> loader = LoaderRegistry.get_loader(".json")
>>> data = loader(some_path)

Suffix aliases (e.g. ``.pickle`` → ``.pkl``, ``.yml`` → ``.yaml``) are also
registered at import time.

"""

import json
import pathlib
import tomllib
import typing

import dill
import pandas as pd
import tomli_w
import yaml

type Suffix = str
"""Canonical file-extension string, e.g. ``".csv"`` or ``".pkl"``."""

type SuffixAlias = str
"""Alternative extension string that maps to a canonical :data:`Suffix`."""


class Loader[T](typing.Protocol):
    """Callable protocol for a *loader*: reads a file and returns an object.

    Parameters
    ----------
    path
        Path to the file to read.
    options
        Format-specific keyword arguments forwarded to the underlying reader.

    Returns
    -------
    T
        The deserialised object.

    """

    def __call__(self, path: pathlib.Path, **options: typing.Any) -> T: ...


class Dumper[T](typing.Protocol):
    """Callable protocol for a *dumper*: writes an object to a file.

    Parameters
    ----------
    obj
        The object to serialise.
    path
        Destination file path.
    options
        Format-specific keyword arguments forwarded to the underlying writer.

    """

    def __call__(
        self, obj: T, path: pathlib.Path, **options: typing.Any
    ) -> None: ...


class IORegistry:
    """Base class for suffix-aware I/O registries.

    ``IORegistry`` is not used directly; instead use :class:`LoaderRegistry`
    and :class:`DumperRegistry`, which inherit the shared machinery and
    maintain their own independent ``_handlers`` / ``_obj_types`` dictionaries.

    Class Attributes
    ----------------
    _suffix_aliases
        Maps alternative extensions to their canonical form.
    _handlers
        Maps ``(suffix, obj_type)`` pairs to I/O callables.  Defined
        concretely in each subclass.
    _obj_types
        Tracks registered object types per suffix, ordered so that the
        *most specific* types come first and the catch-all (``object``)
        comes last.

    """

    _suffix_aliases: dict[SuffixAlias, Suffix] = {}
    _handlers: dict[tuple[Suffix, type], typing.Callable]
    _obj_types: dict[Suffix, list[type]]

    @classmethod
    def register_suffix_alias(
        cls, suffix: Suffix, suffix_alias: SuffixAlias
    ) -> None:
        """Register an alternative extension that maps to a canonical suffix.

        Parameters
        ----------
        suffix
            The canonical suffix, e.g. ``".pkl"``.
        suffix_alias
            The alternative extension, e.g. ``".pickle"``.

        """
        cls._suffix_aliases[suffix_alias] = suffix

    @classmethod
    def register(cls, suffix: Suffix, obj_type: type) -> typing.Callable:
        """Decorator that registers an I/O callable for *suffix* / *obj_type*.

        Parameters
        ----------
        suffix
            Canonical file extension, e.g. ``".csv"``.
        obj_type
            The Python type this callable handles.

        Returns
        -------
        Callable
            A decorator that records the wrapped function in ``cls._handlers`` and
            updates ``cls._obj_types``.

        Examples
        --------
        >>> @LoaderRegistry.register(".csv", pd.DataFrame)
        ... def load_csv(path, **options):
        ...     return pd.read_csv(path, **options)

        """

        def register_handler_decorator(
            handler: typing.Callable,
        ) -> typing.Callable:
            cls._handlers[(suffix, obj_type)] = handler
            cls._register_obj_type(suffix, obj_type)
            return handler

        return register_handler_decorator

    @classmethod
    def _resolve_suffix(cls, suffix: Suffix) -> str:
        """Return the canonical suffix for *suffix*.

        If *suffix* is a known alias the canonical form is returned;
        otherwise *suffix* is returned unchanged.

        """
        return cls._suffix_aliases.get(suffix, suffix)

    @classmethod
    def _register_obj_type(cls, suffix: Suffix, obj_type: type) -> None:
        """Insert *obj_type* into the registry for *suffix*.

        Concrete types are prepended (so the most specific type is found
        first), while the generic ``object`` sentinel is appended so it
        acts as the default / catch-all.

        """
        obj_types = cls._obj_types.get(suffix, [])
        if obj_type not in obj_types:
            if obj_type is object:
                obj_types.append(obj_type)
            else:
                obj_types.insert(0, obj_type)
        cls._obj_types[suffix] = obj_types

    @classmethod
    def _resolve_obj_type(cls, suffix: Suffix, obj_type: type | None) -> type:
        """Resolve *obj_type* against a list of registered types.

        Resolution strategy:

        1. If *obj_type* is ``None``, return the **last** registered type
           (the default / catch-all).
        2. If *obj_type* is an exact match, return it.
        3. If *obj_type* is a subclass of a registered type, return that
           registered superclass.
        4. Otherwise raise :class:`TypeError`.

        Parameters
        ----------
        suffix
            Canonical file extension used to look up registered types.
        obj_type
            The desired type, or ``None`` to get the default.

        Returns
        -------
        type
            The matching registered type.

        Raises
        ------
        TypeError
            If no registered type matches *obj_type*.

        """
        registered_obj_types = cls._obj_types[suffix]
        if obj_type is None:
            return registered_obj_types[-1]
        if obj_type in registered_obj_types:
            return obj_type
        for registered_obj_type in registered_obj_types:
            if issubclass(obj_type, registered_obj_type):
                return registered_obj_type
        raise TypeError(
            f"Cannot resolve {obj_type.__name__!r} from "
            f"{[t.__name__ for t in registered_obj_types]}"
        )

    @classmethod
    def _get_handler(
        cls, suffix: Suffix, obj_type: type | None
    ) -> typing.Callable:
        """Return the I/O callable for *suffix* and *obj_type*.

        Resolves aliases and object-type fallback before look-up.

        """
        suffix = cls._resolve_suffix(suffix)
        obj_type = cls._resolve_obj_type(suffix, obj_type)
        return cls._handlers[(suffix, obj_type)]


class LoaderRegistry(IORegistry):
    """Registry of loader functions (file → Python object).

    Each entry is keyed by ``(suffix, obj_type)`` and must conform to the
    :class:`Loader` protocol.  Use :meth:`get_loader` to retrieve a loader.

    """

    _handlers: dict[tuple[Suffix, type], Loader[typing.Any]] = {}
    _obj_types: dict[Suffix, list[type]] = {}

    @classmethod
    def get_loader(
        cls, suffix: Suffix, obj_type: type | None = None
    ) -> Loader[typing.Any]:
        """Return a loader for *suffix*, optionally narrowed by *obj_type*.

        Parameters
        ----------
        suffix
            File extension (canonical or alias).
        obj_type
            Desired return type.  If ``None``, the default (catch-all)
            loader for *suffix* is returned.

        Returns
        -------
        Loader
            A callable ``(path, **options) -> obj``.

        """
        return cls._get_handler(suffix=suffix, obj_type=obj_type)


class DumperRegistry(IORegistry):
    """Registry of dumper functions (Python object → file).

    Each entry is keyed by ``(suffix, obj_type)`` and must conform to the
    :class:`Dumper` protocol.  Use :meth:`get_dumper` to retrieve a dumper.

    """

    _handlers: dict[tuple[Suffix, type], Dumper[typing.Any]] = {}
    _obj_types: dict[Suffix, list[type]] = {}

    @classmethod
    def get_dumper(cls, suffix: Suffix, obj_type: type) -> Dumper[typing.Any]:
        """Return a dumper for *suffix* and *obj_type*.

        Parameters
        ----------
        suffix
            File extension (canonical or alias).
        obj_type
            Type of the object to serialise.

        Returns
        -------
        Dumper
            A callable ``(obj, path, **options) -> None``.

        """
        return cls._get_handler(suffix, obj_type)


@LoaderRegistry.register(".csv", pd.DataFrame)
def load_csv_as_pandas(
    path: pathlib.Path, **options: typing.Any
) -> pd.DataFrame:
    """Load a CSV file into a :class:`~pandas.DataFrame`."""
    return pd.read_csv(path, **options)


@DumperRegistry.register(".csv", pd.DataFrame)
def dump_csv_from_pandas(
    obj: pd.DataFrame, path: pathlib.Path, **options: typing.Any
) -> None:
    """Write a :class:`~pandas.DataFrame` to a CSV file (without index)."""
    options = dict(index=False) | options
    obj.to_csv(path, **options)


@LoaderRegistry.register(".parquet", pd.DataFrame)
def load_parquet_as_pandas(
    path: pathlib.Path, **options: typing.Any
) -> pd.DataFrame:
    """Load a Parquet file into a :class:`~pandas.DataFrame`."""
    options = dict(engine="pyarrow") | options
    return pd.read_parquet(path, **options)


@DumperRegistry.register(".parquet", pd.DataFrame)
def dump_parquet_from_pandas(
    obj: pd.DataFrame, path: pathlib.Path, **options: typing.Any
) -> None:
    """Write a :class:`~pandas.DataFrame` to a Parquet file."""
    options = dict(engine="pyarrow") | options
    obj.to_parquet(path, **options)


@LoaderRegistry.register(".json", object)
def load_json(path: pathlib.Path, **options: typing.Any) -> object:
    """Load a JSON file and return the deserialised Python object."""
    with open(path, "r") as f:
        return json.load(f, **options)


@DumperRegistry.register(".json", object)
def dump_json(obj: object, path: pathlib.Path, **options: typing.Any) -> None:
    """Serialise a Python object to a JSON file."""
    with open(path, "w") as f:
        json.dump(obj, f, **options)


@LoaderRegistry.register(".yaml", object)
def load_yaml(path: pathlib.Path, **options: typing.Any) -> object:
    """Load a YAML file (using ``SafeLoader`` by default)."""
    options = dict(Loader=yaml.SafeLoader) | options
    with open(path, "r") as f:
        return yaml.load(f, **options)


@DumperRegistry.register(".yaml", object)
def dump_yaml(obj: object, path: pathlib.Path, **options: typing.Any) -> None:
    """Write a Python object to a YAML file (using ``SafeDumper`` by default)."""
    options = dict(Dumper=yaml.SafeDumper) | options
    with open(path, "w") as f:
        yaml.dump(obj, f, **options)


@LoaderRegistry.register(".toml", dict)
def load_toml(path: pathlib.Path, **options: typing.Any) -> dict:
    """Load a TOML file and return a plain ``dict``."""
    with open(path, "rb") as f:
        return tomllib.load(f, **options)


@DumperRegistry.register(".toml", dict)
def dump_toml(obj: dict, path: pathlib.Path, **options: typing.Any) -> None:
    """Write a ``dict`` to a TOML file."""
    with open(path, "wb") as f:
        tomli_w.dump(obj, f, **options)


@LoaderRegistry.register(".pkl", object)
def load_pickle(path: pathlib.Path, **options: typing.Any) -> typing.Any:
    """Deserialise any object from a pickle file (via :mod:`dill`)."""
    with open(path, "rb") as f:
        return dill.load(f, **options)


@DumperRegistry.register(".pkl", object)
def dump_pickle(
    obj: typing.Any, path: pathlib.Path, **options: typing.Any
) -> None:
    """Serialise any object to a pickle file (via :mod:`dill`)."""
    with open(path, "wb") as f:
        dill.dump(obj, f, **options)


IORegistry.register_suffix_alias(suffix=".pkl", suffix_alias=".pickle")
IORegistry.register_suffix_alias(suffix=".yaml", suffix_alias=".yml")
