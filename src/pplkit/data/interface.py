import os
import pathlib
import typing

from pplkit.data.io import SUFFIX_TO_IO

type PathLike = str | os.PathLike[str]


class DataInterface:
    """Data interface that manages named directories and automatically reads
    and writes data based on file extension.

    Directories are registered by name and accessed via bracket notation.
    Values are coerced to ``pathlib.Path`` on insertion.

    Parameters
    ----------
    paths
        Directories to manage, passed as keyword arguments.

    Examples
    --------
    >>> dataif = DataInterface(raw="/data/raw", output="/data/output")
    >>> dataif["raw"]
    PosixPath('/data/raw')
    >>> dataif.dump(obj, "file.csv", key="raw")
    >>> dataif.load("file.csv", key="raw")

    """

    def __init__(self, **paths: PathLike) -> None:
        self._paths: dict[str, pathlib.Path] = {}
        for key, value in paths.items():
            self[key] = value

    def __getitem__(self, key: str | None) -> pathlib.Path:
        if key is None:
            return pathlib.Path()
        return self._paths[key]

    def __setitem__(self, key: str, value: PathLike) -> None:
        if not isinstance(key, str):
            raise TypeError(
                f"DataInterface key must be a 'str', not {type(key).__name__!r}"
            )
        self._paths[key] = pathlib.Path(value)

    def __delitem__(self, key: str) -> None:
        del self._paths[key]

    def __len__(self) -> int:
        return len(self._paths)

    def load(
        self,
        sub_path: PathLike,
        key: str | None = None,
        **options: typing.Any,
    ) -> typing.Any:
        """Load data from a file. The file format is inferred from the suffix.

        Parameters
        ----------
        sub_path
            Relative path to the file under the registered directory. If
            ``key`` is ``None``, this is used as the full path.
        key
            Name of a registered directory. If ``None``, ``sub_path`` is
            used as-is.
        options
            Extra keyword arguments passed to the underlying ``IO`` class.

        Returns
        -------
        Any
            Data loaded from the given path.

        """
        path = self[key] / sub_path
        return SUFFIX_TO_IO[path.suffix].load(path, **options)

    def dump(
        self,
        obj: typing.Any,
        sub_path: PathLike,
        key: str | None = None,
        mkdir: bool = True,
        **options: typing.Any,
    ) -> None:
        """Dump data to a file. The file format is inferred from the suffix.

        Parameters
        ----------
        obj
            Data object to write.
        sub_path
            Relative path to the file under the registered directory. If
            ``key`` is ``None``, this is used as the full path.
        key
            Name of a registered directory. If ``None``, ``sub_path`` is
            used as-is.
        mkdir
            If ``True``, automatically create the parent directory. Default
            is ``True``.
        options
            Extra keyword arguments passed to the underlying ``IO`` class.

        """
        path = self[key] / sub_path
        SUFFIX_TO_IO[path.suffix].dump(obj, path, mkdir=mkdir, **options)

    def __repr__(self) -> str:
        items = ", ".join(
            f"{key}='{value}'" for key, value in self._paths.items()
        )
        return f"{type(self).__name__}({items})"
