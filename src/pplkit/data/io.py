import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Tuple, Type

import dill
import pandas as pd
import yaml


class DataIO(ABC):
    """Bridge class that unifies the file I/O for different data types.

    """

    fextns: Tuple[str] = ("",)
    """The file extensions. When loading a file, it will be used to check if
    the file extension matches.

    """
    dtypes: Tuple[Type] = (object,)
    """The data types. When dumping the data, it will be used to check if the
    data type matches.

    """

    @abstractmethod
    def _load(self, fpath: Path, **options) -> Any:
        pass

    @abstractmethod
    def _dump(self, obj: Any, fpath: Path, **options):
        pass

    def load(self, fpath: str | Path, **options) -> Any:
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
        fpath = Path(fpath)
        if fpath.suffix not in self.fextns:
            raise ValueError(f"File extension must be in {self.fextns}.")
        return self._load(fpath, **options)

    def dump(self, obj: Any, fpath: str | Path, mkdir: bool = True, **options):
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
        fpath = Path(fpath)
        if not isinstance(obj, self.dtypes):
            raise TypeError(f"Data must be an instance of {self.dtypes}.")
        if mkdir:
            fpath.parent.mkdir(parents=True, exist_ok=True)
        self._dump(obj, fpath, **options)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(fextns={self.fextns})"


class CSVIO(DataIO):

    fextns: Tuple[str] = (".csv",)
    dtypes: Tuple[Type] = (pd.DataFrame,)

    def _load(self, fpath: Path, **options) -> pd.DataFrame:
        return pd.read_csv(fpath, **options)

    def _dump(self, obj: pd.DataFrame, fpath: Path, **options):
        options = dict(index=False) | options
        obj.to_csv(fpath, **options)


class PickleIO(DataIO):

    fextns: Tuple[str] = (".pkl", ".pickle")

    def _load(self, fpath: Path, **options) -> Any:
        with open(fpath, "rb") as f:
            return dill.load(f, **options)

    def _dump(self, obj: Any, fpath: Path, **options):
        with open(fpath, "wb") as f:
            return dill.dump(obj, f, **options)


class YAMLIO(DataIO):

    fextns: Tuple[str] = (".yml", ".yaml")
    dtypes: Tuple[Type] = (dict, list)

    def _load(self, fpath: Path, **options) -> Dict | List:
        options = dict(Loader=yaml.SafeLoader) | options
        with open(fpath, "r") as f:
            return yaml.load(f, **options)

    def _dump(self, obj: Dict | List, fpath: Path, **options):
        options = dict(Dumper=yaml.SafeDumper) | options
        with open(fpath, "w") as f:
            return yaml.dump(obj, f, **options)


class ParquetIO(DataIO):

    fextns: Tuple[str] = (".parquet",)
    dtypes: Tuple[Type] = (pd.DataFrame,)

    def _load(self, fpath: Path, **options) -> pd.DataFrame:
        options = dict(engine="pyarrow") | options
        return pd.read_parquet(fpath, **options)

    def _dump(self, obj: pd.DataFrame, fpath: Path, **options):
        options = dict(engine="pyarrow") | options
        obj.to_parquet(fpath, **options)


class JSONIO(DataIO):

    fextns: Tuple[str] = (".json",)
    dtypes: Tuple[Type] = (dict, list)

    def _load(self, fpath: Path, **options) -> Dict | List:
        with open(fpath, "r") as f:
            return json.load(f, **options)

    def _dump(self, obj: Dict | List, fpath: Path, **options):
        with open(fpath, "w") as f:
            json.dump(obj, f, **options)


data_io_instances: List[DataIO] = [
    CSVIO(),
    YAMLIO(),
    PickleIO(),
    ParquetIO(),
    JSONIO(),
]
"""Instances of all the data ios. These are singletons and module variables.

:meta hide-value:

"""


data_io_dict: Dict[str, DataIO] = {
    fextn: data_io
    for data_io in data_io_instances for fextn in data_io.fextns
}
"""Instances of data ios, organized in a dictionary with key as the file
extensions for each :py:class:`DataIO` class.

:meta hide-value:

"""
