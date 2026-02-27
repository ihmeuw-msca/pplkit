import abc
import json
import pathlib
import tomllib
import typing

import dill
import pandas as pd
import tomli_w
import yaml


class DataIO(abc.ABC):
    """Bridge class that unifies the file I/O for different data types."""

    fextns: tuple[str, ...] = ("",)
    """The file extensions. When loading a file, it will be used to check if
    the file extension matches.

    """
    dtypes: tuple[type, ...] = (object,)
    """The data types. When dumping the data, it will be used to check if the
    data type matches.

    """

    @classmethod
    @abc.abstractmethod
    def _load(cls, fpath: pathlib.Path, **options: typing.Any) -> typing.Any:
        pass

    @classmethod
    @abc.abstractmethod
    def _dump(
        cls,
        obj: typing.Any,
        fpath: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        pass

    @classmethod
    def load(
        cls, fpath: str | pathlib.Path, **options: typing.Any
    ) -> typing.Any:
        """Load data from given path.

        Parameters
        ----------
        fpath
            Provided file path.
        options
            Extra arguments for the load function.

        Raises
        ------
        ValueError
            Raised when the file extension doesn't match.

        Returns
        -------
        Any
            Data loaded from the given path.

        """
        fpath = pathlib.Path(fpath)
        if fpath.suffix not in cls.fextns:
            raise ValueError(f"File extension must be in {cls.fextns}.")
        return cls._load(fpath, **options)

    @classmethod
    def dump(
        cls,
        obj: typing.Any,
        fpath: str | pathlib.Path,
        mkdir: bool = True,
        **options: typing.Any,
    ) -> None:
        """Dump data to given path.

        Parameters
        ----------
        obj
            Provided data object.
        fpath
            Provided file path.
        mkdir
            If true, it will automatically create the parent directory. The
            default is true.
        options
            Extra arguments for the dump function.

        Raises
        ------
        TypeError
            Raised when the given data object type doesn't match.

        """
        fpath = pathlib.Path(fpath)
        if not isinstance(obj, cls.dtypes):
            raise TypeError(f"Data must be an instance of {cls.dtypes}.")
        if mkdir:
            fpath.parent.mkdir(parents=True, exist_ok=True)
        cls._dump(obj, fpath, **options)


class CSVIO(DataIO):
    fextns: tuple[str, ...] = (".csv",)
    dtypes: tuple[type, ...] = (pd.DataFrame,)

    @classmethod
    def _load(cls, fpath: pathlib.Path, **options: typing.Any) -> pd.DataFrame:
        return pd.read_csv(fpath, **options)

    @classmethod
    def _dump(
        cls,
        obj: pd.DataFrame,
        fpath: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        options = dict(index=False) | options
        obj.to_csv(fpath, **options)


class PickleIO(DataIO):
    fextns: tuple[str, ...] = (".pkl", ".pickle")

    @classmethod
    def _load(cls, fpath: pathlib.Path, **options: typing.Any) -> typing.Any:
        with open(fpath, "rb") as f:
            return dill.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: typing.Any,
        fpath: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        with open(fpath, "wb") as f:
            dill.dump(obj, f, **options)


class YAMLIO(DataIO):
    fextns: tuple[str, ...] = (".yml", ".yaml")
    dtypes: tuple[type, ...] = (dict, list)

    @classmethod
    def _load(cls, fpath: pathlib.Path, **options: typing.Any) -> dict | list:
        options = dict(Loader=yaml.SafeLoader) | options
        with open(fpath, "r") as f:
            return yaml.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: dict | list,
        fpath: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        options = dict(Dumper=yaml.SafeDumper) | options
        with open(fpath, "w") as f:
            yaml.dump(obj, f, **options)


class ParquetIO(DataIO):
    fextns: tuple[str, ...] = (".parquet",)
    dtypes: tuple[type, ...] = (pd.DataFrame,)

    @classmethod
    def _load(cls, fpath: pathlib.Path, **options: typing.Any) -> pd.DataFrame:
        options = dict(engine="pyarrow") | options
        return pd.read_parquet(fpath, **options)

    @classmethod
    def _dump(
        cls,
        obj: pd.DataFrame,
        fpath: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        options = dict(engine="pyarrow") | options
        obj.to_parquet(fpath, **options)


class JSONIO(DataIO):
    fextns: tuple[str, ...] = (".json",)
    dtypes: tuple[type, ...] = (dict, list)

    @classmethod
    def _load(cls, fpath: pathlib.Path, **options: typing.Any) -> dict | list:
        with open(fpath, "r") as f:
            return json.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: dict | list,
        fpath: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        with open(fpath, "w") as f:
            json.dump(obj, f, **options)


class TOMLIO(DataIO):
    fextns: tuple[str, ...] = (".toml",)
    dtypes: tuple[type, ...] = (dict,)

    @classmethod
    def _load(cls, fpath: pathlib.Path, **options: typing.Any) -> dict:
        with open(fpath, "rb") as f:
            return tomllib.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: dict,
        fpath: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        with open(fpath, "wb") as f:
            tomli_w.dump(obj, f, **options)


_dataio_list: list[type[DataIO]] = [
    CSVIO,
    YAMLIO,
    PickleIO,
    ParquetIO,
    JSONIO,
    TOMLIO,
]


dataio_dict: dict[str, type[DataIO]] = {
    fextn: dataio for dataio in _dataio_list for fextn in dataio.fextns
}
"""Data IO classes, organized in a dictionary with key as the file
extensions for each :class:`DataIO` class.

"""
