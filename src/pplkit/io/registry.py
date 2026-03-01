"""Registry-based I/O layer for loading and dumping data by file extension.

This module provides five public functions for working with file-based I/O:

* :func:`register_suffix_alias` – map an alternative file extension to a
  canonical one (e.g. ``.yml`` → ``.yaml``).
* :func:`register_loader` – decorator that registers a loader for a given
  suffix and object type.
* :func:`register_dumper` – decorator that registers a dumper for a given
  suffix and object type.
* :func:`get_loader` – retrieve a loader callable by suffix (and optional
  object type).
* :func:`get_dumper` – retrieve a dumper callable by suffix and object type.

Loader/dumper functions for CSV, Pickle, YAML, Parquet, JSON, and TOML are
registered at module import time, so callers can immediately do:

>>> loader = get_loader(".json")
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
    def __call__(self, path: pathlib.Path, **options: typing.Any) -> T:
        """Callable protocol for a *loader*: reads a file and returns an object.

        Parameters
        ----------
        path
            Path to the file to read.
        options
            Format-specific keyword arguments forwarded to the underlying reader.

        Returns
        -------
            The deserialised object.

        """


class Dumper[T](typing.Protocol):
    def __call__(
        self, obj: T, path: pathlib.Path, **options: typing.Any
    ) -> None:
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


class _IORegistry:
    """Base class for suffix-aware I/O registries.

    This is an internal implementation detail.  Users should interact with the
    module-level functions :func:`register_suffix_alias`,
    :func:`register_loader`, :func:`register_dumper`, :func:`get_loader`, and
    :func:`get_dumper` instead.

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
            A decorator that records the wrapped function and updates the
            internal handler registry.

        Examples
        --------
        >>> @register_loader(".csv", pd.DataFrame)
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
        return cls._suffix_aliases.get(suffix, suffix)

    @classmethod
    def _register_obj_type(cls, suffix: Suffix, obj_type: type) -> None:
        obj_types = cls._obj_types.get(suffix, [])
        if obj_type not in obj_types:
            if obj_type is object:
                obj_types.append(obj_type)
            else:
                obj_types.insert(0, obj_type)
        cls._obj_types[suffix] = obj_types

    @classmethod
    def _resolve_obj_type(cls, suffix: Suffix, obj_type: type | None) -> type:
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
        suffix = cls._resolve_suffix(suffix)
        obj_type = cls._resolve_obj_type(suffix, obj_type)
        return cls._handlers[(suffix, obj_type)]


class _LoaderRegistry(_IORegistry):
    """Registry of loader functions (file → Python object).

    This is an internal implementation detail.  Use the module-level
    functions :func:`register_loader` and :func:`get_loader` instead.

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
            A callable ``(path, **options) -> obj``.

        """
        return cls._get_handler(suffix=suffix, obj_type=obj_type)


class _DumperRegistry(_IORegistry):
    """Registry of dumper functions (Python object → file).

    This is an internal implementation detail.  Use the module-level
    functions :func:`register_dumper` and :func:`get_dumper` instead.

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
            A callable ``(obj, path, **options) -> None``.

        """
        return cls._get_handler(suffix, obj_type)


# Public API – module-level convenience functions
def register_suffix_alias(suffix: Suffix, suffix_alias: SuffixAlias) -> None:
    """Register an alternative extension that maps to a canonical suffix.

    After registration, any call to :func:`get_loader` or :func:`get_dumper`
    with *suffix_alias* will resolve to the handlers registered for *suffix*.

    Parameters
    ----------
    suffix
        The canonical suffix, e.g. ``".pkl"``.
    suffix_alias
        The alternative extension, e.g. ``".pickle"``.

    Examples
    --------
    >>> register_suffix_alias(suffix=".yaml", suffix_alias=".conf")

    """
    _IORegistry.register_suffix_alias(suffix, suffix_alias)


def register_loader(
    suffix: Suffix, obj_type: type
) -> typing.Callable[[Loader[typing.Any]], Loader[typing.Any]]:
    """Decorator that registers a loader for *suffix* and *obj_type*.

    The decorated function must conform to the :class:`Loader` protocol:
    ``(path, **options) -> obj``.

    Parameters
    ----------
    suffix
        Canonical file extension, e.g. ``".csv"``.
    obj_type
        The Python type this loader produces.

    Returns
    -------
        A decorator that records the wrapped function in the loader registry.

    Examples
    --------
    >>> @register_loader(".csv", pd.DataFrame)
    ... def load_csv(path, **options):
    ...     return pd.read_csv(path, **options)

    """
    return _LoaderRegistry.register(suffix, obj_type)


def register_dumper(
    suffix: Suffix, obj_type: type
) -> typing.Callable[[Dumper[typing.Any]], Dumper[typing.Any]]:
    """Decorator that registers a dumper for *suffix* and *obj_type*.

    The decorated function must conform to the :class:`Dumper` protocol:
    ``(obj, path, **options) -> None``.

    Parameters
    ----------
    suffix
        Canonical file extension, e.g. ``".csv"``.
    obj_type
        The Python type this dumper accepts.

    Returns
    -------
        A decorator that records the wrapped function in the dumper registry.

    Examples
    --------
    >>> @register_dumper(".csv", pd.DataFrame)
    ... def dump_csv(obj, path, **options):
    ...     obj.to_csv(path, **options)

    """
    return _DumperRegistry.register(suffix, obj_type)


def get_loader(
    suffix: Suffix, obj_type: type | None = None
) -> Loader[typing.Any]:
    """Return a loader for *suffix*, optionally narrowed by *obj_type*.

    Parameters
    ----------
    suffix
        File extension (canonical or alias).
    obj_type
        Desired return type.  If ``None``, the default (catch-all) loader
        for *suffix* is returned.

    Returns
    -------
        A callable ``(path, **options) -> obj``.

    Examples
    --------
    >>> loader = get_loader(".json")
    >>> data = loader("path/to/data.json")

    """
    return _LoaderRegistry.get_loader(suffix=suffix, obj_type=obj_type)


def get_dumper(suffix: Suffix, obj_type: type) -> Dumper[typing.Any]:
    """Return a dumper for *suffix* and *obj_type*.

    Parameters
    ----------
    suffix
        File extension (canonical or alias).
    obj_type
        Type of the object to serialise.

    Returns
    -------
        A callable ``(obj, path, **options) -> None``.

    Examples
    --------
    >>> dumper = get_dumper(".json", obj_type=object)
    >>> dumper({"key": "value"}, "path/to/output.json")

    """
    return _DumperRegistry.get_dumper(suffix, obj_type)


# Built-in loaders and dumpers
@register_loader(".csv", pd.DataFrame)
def _load_csv_as_pandas(
    path: pathlib.Path, **options: typing.Any
) -> pd.DataFrame:
    """Load a CSV file into a :class:`~pandas.DataFrame`."""
    return pd.read_csv(path, **options)


@register_dumper(".csv", pd.DataFrame)
def _dump_csv_from_pandas(
    obj: pd.DataFrame, path: pathlib.Path, **options: typing.Any
) -> None:
    """Write a :class:`~pandas.DataFrame` to a CSV file (without index)."""
    options = dict(index=False) | options
    obj.to_csv(path, **options)


@register_loader(".parquet", pd.DataFrame)
def _load_parquet_as_pandas(
    path: pathlib.Path, **options: typing.Any
) -> pd.DataFrame:
    """Load a Parquet file into a :class:`~pandas.DataFrame`."""
    options = dict(engine="pyarrow") | options
    return pd.read_parquet(path, **options)


@register_dumper(".parquet", pd.DataFrame)
def _dump_parquet_from_pandas(
    obj: pd.DataFrame, path: pathlib.Path, **options: typing.Any
) -> None:
    """Write a :class:`~pandas.DataFrame` to a Parquet file."""
    options = dict(engine="pyarrow") | options
    obj.to_parquet(path, **options)


@register_loader(".json", object)
def _load_json(path: pathlib.Path, **options: typing.Any) -> object:
    """Load a JSON file and return the deserialised Python object."""
    with open(path, "r") as f:
        return json.load(f, **options)


@register_dumper(".json", object)
def _dump_json(obj: object, path: pathlib.Path, **options: typing.Any) -> None:
    """Serialise a Python object to a JSON file."""
    with open(path, "w") as f:
        json.dump(obj, f, **options)


@register_loader(".yaml", object)
def _load_yaml(path: pathlib.Path, **options: typing.Any) -> object:
    """Load a YAML file (using ``SafeLoader`` by default)."""
    options = dict(Loader=yaml.SafeLoader) | options
    with open(path, "r") as f:
        return yaml.load(f, **options)


@register_dumper(".yaml", object)
def _dump_yaml(obj: object, path: pathlib.Path, **options: typing.Any) -> None:
    """Write a Python object to a YAML file (using ``SafeDumper`` by default)."""
    options = dict(Dumper=yaml.SafeDumper) | options
    with open(path, "w") as f:
        yaml.dump(obj, f, **options)


@register_loader(".toml", dict)
def _load_toml(path: pathlib.Path, **options: typing.Any) -> dict:
    """Load a TOML file and return a plain ``dict``."""
    with open(path, "rb") as f:
        return tomllib.load(f, **options)


@register_dumper(".toml", dict)
def _dump_toml(obj: dict, path: pathlib.Path, **options: typing.Any) -> None:
    """Write a ``dict`` to a TOML file."""
    with open(path, "wb") as f:
        tomli_w.dump(obj, f, **options)


@register_loader(".pkl", object)
def _load_pickle(path: pathlib.Path, **options: typing.Any) -> typing.Any:
    """Deserialise any object from a pickle file (via :mod:`dill`)."""
    with open(path, "rb") as f:
        return dill.load(f, **options)


@register_dumper(".pkl", object)
def _dump_pickle(
    obj: typing.Any, path: pathlib.Path, **options: typing.Any
) -> None:
    """Serialise any object to a pickle file (via :mod:`dill`)."""
    with open(path, "wb") as f:
        dill.dump(obj, f, **options)


# Built-in suffix aliases
register_suffix_alias(suffix=".pkl", suffix_alias=".pickle")
register_suffix_alias(suffix=".yaml", suffix_alias=".yml")
