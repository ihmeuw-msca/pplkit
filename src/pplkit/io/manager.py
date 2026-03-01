"""High-level I/O manager for reading and writing files by directory key.

:class:`IOManager` pairs named directory paths with the registry-based
I/O layer in :mod:`pplkit.io.registry` so that callers can load and dump
data with a single call, e.g.

>>> iom = IOManager(raw="/data/raw", output="/data/output")
>>> iom.dump(my_df, "results.csv", key="output")
>>> df = iom.load("results.csv", key="output")

File format is determined automatically from the file extension.

"""

import os
import pathlib
import typing

from pplkit.io.registry import DumperRegistry, LoaderRegistry

type PathLike = str | os.PathLike[str]
"""A string or :class:`os.PathLike` that can be coerced to a
:class:`~pathlib.Path`.

"""


class IOManager:
    """Manage named directories and automatically read/write data by extension.

    Directories are registered by name and accessed via bracket notation.
    Values are coerced to :class:`~pathlib.Path` on insertion.

    Parameters
    ----------
    paths
        Directories to manage, passed as keyword arguments.  Keys are
        short names (e.g. ``"raw"``, ``"output"``); values are directory
        paths (strings or :class:`~pathlib.Path` objects).

    Examples
    --------
    >>> iom = IOManager(raw="/data/raw", output="/data/output")
    >>> iom["raw"]
    PosixPath('/data/raw')
    >>> iom.dump(obj, "file.csv", key="raw")
    >>> iom.load("file.csv", key="raw")

    """

    def __init__(self, **paths: PathLike) -> None:
        self._paths: dict[str, pathlib.Path] = {}
        for key, value in paths.items():
            self[key] = value

    def load(
        self,
        sub_path: PathLike,
        key: str | None = None,
        obj_type: type | None = None,
        suffix: str | None = None,
        **options: typing.Any,
    ) -> typing.Any:
        """Load data from a file.

        The file format is inferred from the suffix of *sub_path* (or from
        the explicit *suffix* override).

        Parameters
        ----------
        sub_path
            Relative path to the file under the registered directory.  If
            *key* is ``None``, this is used as the full/absolute path.
        key
            Name of a registered directory.  If ``None``, *sub_path* is
            used as-is.
        obj_type
            Desired Python type for the loaded data.  When ``None`` the
            default loader for the suffix is used (e.g. ``object`` for
            JSON, ``pd.DataFrame`` for CSV).
        suffix
            Override the file suffix used for format resolution.  When
            ``None``, the suffix is taken from *sub_path*.
        options
            Extra keyword arguments forwarded to the underlying loader.

        Returns
        -------
        Any
            Data loaded from the given path.

        Raises
        ------
        KeyError
            If *key* is not ``None`` and has not been registered.
        FileNotFoundError
            If the resolved path does not exist on disk.

        """
        path = self[key] / sub_path
        loader = LoaderRegistry.get_loader(
            suffix=suffix or path.suffix, obj_type=obj_type
        )
        return loader(path, **options)

    def dump(
        self,
        obj: typing.Any,
        sub_path: PathLike,
        key: str | None = None,
        mkdir: bool = True,
        suffix: str | None = None,
        **options: typing.Any,
    ) -> None:
        """Dump data to a file.

        The file format is inferred from the suffix of *sub_path* (or from
        the explicit *suffix* override).

        Parameters
        ----------
        obj
            Data object to write.
        sub_path
            Relative path to the file under the registered directory.  If
            *key* is ``None``, this is used as the full/absolute path.
        key
            Name of a registered directory.  If ``None``, *sub_path* is
            used as-is.
        mkdir
            If ``True`` (the default), automatically create the parent
            directory and any missing ancestors.
        suffix
            Override the file suffix used for format resolution.  When
            ``None``, the suffix is taken from *sub_path*.
        options
            Extra keyword arguments forwarded to the underlying dumper.

        Raises
        ------
        KeyError
            If *key* is not ``None`` and has not been registered.

        """
        path = self[key] / sub_path
        if mkdir:
            path.parent.mkdir(parents=True, exist_ok=True)
        dumper = DumperRegistry.get_dumper(
            suffix=suffix or path.suffix, obj_type=type(obj)
        )
        dumper(obj, path, **options)

    def __getitem__(self, key: str | None) -> pathlib.Path:
        """Return the path registered under *key*.

        If *key* is ``None`` an empty :class:`~pathlib.Path` is returned,
        which lets callers pass absolute sub-paths through
        :meth:`load`/:meth:`dump` without a registered directory.

        Parameters
        ----------
        key
            Registered directory name, or ``None``.

        Returns
        -------
        pathlib.Path
            The registered directory path (or ``Path()`` when *key* is
            ``None``).

        Raises
        ------
        KeyError
            If *key* is not ``None`` and has not been registered.

        """
        if key is None:
            return pathlib.Path()
        return self._paths[key]

    def __setitem__(self, key: str, value: PathLike) -> None:
        """Register or overwrite the directory path for *key*.

        *value* is coerced to :class:`~pathlib.Path`.

        Parameters
        ----------
        key
            Directory name.  Must be a ``str``.
        value
            Directory path (``str`` or path-like).

        Raises
        ------
        TypeError
            If *key* is not a ``str``.

        """
        if not isinstance(key, str):
            raise TypeError(
                f"IOManager key must be a 'str', not {type(key).__name__!r}"
            )
        self._paths[key] = pathlib.Path(value)

    def __delitem__(self, key: str) -> None:
        """Remove the directory registered under *key*.

        Raises
        ------
        KeyError
            If *key* has not been registered.

        """
        del self._paths[key]

    def __len__(self) -> int:
        """Return the number of registered directories."""
        return len(self._paths)

    def __repr__(self) -> str:
        """Return a concise, eval-style representation of the manager.

        Examples
        --------
        >>> IOManager(raw='/data/raw', output='/data/output')
        IOManager(raw='/data/raw', output='/data/output')

        """
        items = ", ".join(
            f"{key}='{value}'" for key, value in self._paths.items()
        )
        return f"{type(self).__name__}({items})"
