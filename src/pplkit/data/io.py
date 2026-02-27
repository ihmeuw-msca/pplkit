import abc
import json
import pathlib
import tomllib
import typing

import dill
import pandas as pd
import tomli_w
import yaml


class IO(abc.ABC):
    """Bridge class that unifies the file I/O for different data types."""

    @classmethod
    @abc.abstractmethod
    def _load(cls, path: pathlib.Path, **options: typing.Any) -> typing.Any:
        pass

    @classmethod
    @abc.abstractmethod
    def _dump(
        cls,
        obj: typing.Any,
        path: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        pass

    @classmethod
    def load(
        cls, path: str | pathlib.Path, **options: typing.Any
    ) -> typing.Any:
        """Load data from given path.

        Parameters
        ----------
        path
            Provided file path.
        options
            Extra arguments for the load function.

        Returns
        -------
        Any
            Data loaded from the given path.

        """
        path = pathlib.Path(path)
        return cls._load(path, **options)

    @classmethod
    def dump(
        cls,
        obj: typing.Any,
        path: str | pathlib.Path,
        mkdir: bool = True,
        **options: typing.Any,
    ) -> None:
        """Dump data to given path.

        Parameters
        ----------
        obj
            Provided data object.
        path
            Provided file path.
        mkdir
            If true, it will automatically create the parent directory. The
            default is true.
        options
            Extra arguments for the dump function.

        """
        path = pathlib.Path(path)
        if mkdir:
            path.parent.mkdir(parents=True, exist_ok=True)
        cls._dump(obj, path, **options)


class CSVIO(IO):
    @classmethod
    def _load(cls, path: pathlib.Path, **options: typing.Any) -> pd.DataFrame:
        return pd.read_csv(path, **options)

    @classmethod
    def _dump(
        cls,
        obj: pd.DataFrame,
        path: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        options = dict(index=False) | options
        obj.to_csv(path, **options)


class PickleIO(IO):
    @classmethod
    def _load(cls, path: pathlib.Path, **options: typing.Any) -> typing.Any:
        with open(path, "rb") as f:
            return dill.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: typing.Any,
        path: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        with open(path, "wb") as f:
            dill.dump(obj, f, **options)


class YAMLIO(IO):
    @classmethod
    def _load(cls, path: pathlib.Path, **options: typing.Any) -> dict | list:
        options = dict(Loader=yaml.SafeLoader) | options
        with open(path, "r") as f:
            return yaml.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: dict | list,
        path: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        options = dict(Dumper=yaml.SafeDumper) | options
        with open(path, "w") as f:
            yaml.dump(obj, f, **options)


class ParquetIO(IO):
    @classmethod
    def _load(cls, path: pathlib.Path, **options: typing.Any) -> pd.DataFrame:
        options = dict(engine="pyarrow") | options
        return pd.read_parquet(path, **options)

    @classmethod
    def _dump(
        cls,
        obj: pd.DataFrame,
        path: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        options = dict(engine="pyarrow") | options
        obj.to_parquet(path, **options)


class JSONIO(IO):
    @classmethod
    def _load(cls, path: pathlib.Path, **options: typing.Any) -> dict | list:
        with open(path, "r") as f:
            return json.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: dict | list,
        path: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        with open(path, "w") as f:
            json.dump(obj, f, **options)


class TOMLIO(IO):
    @classmethod
    def _load(cls, path: pathlib.Path, **options: typing.Any) -> dict:
        with open(path, "rb") as f:
            return tomllib.load(f, **options)

    @classmethod
    def _dump(
        cls,
        obj: dict,
        path: pathlib.Path,
        **options: typing.Any,
    ) -> None:
        with open(path, "wb") as f:
            tomli_w.dump(obj, f, **options)


SUFFIX_TO_IO: dict[str, type[IO]] = {
    ".csv": CSVIO,
    ".pkl": PickleIO,
    ".pickle": PickleIO,
    ".yml": YAMLIO,
    ".yaml": YAMLIO,
    ".parquet": ParquetIO,
    ".json": JSONIO,
    ".toml": TOMLIO,
}
"""Mapping from file suffix to the corresponding :class:`IO` class.

"""
