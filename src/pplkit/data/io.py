"""
Data IO
=======

Data classes that in charges of reading and writing data with different formats.
"""
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Tuple, Type

import dill
import pandas as pd
import yaml


class DataIO(ABC):

    fextns: Tuple[str] = ("",)
    dtypes: Tuple[Type] = (object,)

    @abstractmethod
    def _load(self, fpath: str | Path, **options) -> Any:
        pass

    @abstractmethod
    def _dump(self, obj: Any, fpath: str | Path, **options):
        pass

    def load(self, fpath: str | Path, **options) -> Any:
        if Path(fpath).suffix not in self.fextns:
            raise ValueError(f"File extension must be in {self.fextns}.")
        return self._load(fpath, **options)

    def dump(self, obj: Any, fpath: str | Path, **options):
        if not isinstance(obj, self.dtypes):
            raise TypeError(f"Data must be an instance of {self.dtypes}.")
        self._dump(obj, fpath, **options)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(fextns={self.fextns})"


class CSVIO(DataIO):

    fextns: Tuple[str] = (".csv",)
    dtypes: Tuple[Type] = (pd.DataFrame,)

    def _load(self, fpath: str | Path, **options) -> pd.DataFrame:
        return pd.read_csv(fpath, **options)

    def _dump(self, obj: pd.DataFrame, fpath: str | Path, **options):
        options = dict(index=False) | options
        obj.to_csv(fpath, **options)


class PickleIO(DataIO):

    fextns: Tuple[str] = (".pkl", ".pickle")

    def _load(self, fpath: str | Path, **options) -> Any:
        with open(fpath, "rb") as f:
            return dill.load(f, **options)

    def _dump(self, obj: Any, fpath: str | Path, **options):
        with open(fpath, "wb") as f:
            return dill.dump(obj, f, **options)


class YAMLIO(DataIO):

    fextns: Tuple[str] = (".yml", ".yaml")
    dtypes: Tuple[Type] = (dict, list)

    def _load(self, fpath: str | Path, **options) -> Dict | List:
        options = dict(Loader=yaml.SafeLoader) | options
        with open(fpath, "r") as f:
            return yaml.load(f, **options)

    def _dump(self, obj: Dict | List, fpath: str | Path, **options):
        options = dict(Dumper=yaml.SafeDumper) | options
        with open(fpath, "w") as f:
            return yaml.dump(obj, f, **options)


class ParquetIO(DataIO):

    fextns: Tuple[str] = (".parquet",)
    dtypes: Tuple[Type] = (pd.DataFrame,)

    def _load(self, fpath: str | Path, **options) -> pd.DataFrame:
        options = dict(engine="pyarrow") | options
        return pd.read_parquet(fpath, **options)

    def _dump(self, obj: pd.DataFrame, fpath: str | Path, **options):
        options = dict(engine="pyarrow") | options
        obj.to_parquet(fpath, **options)


class JSONIO(DataIO):

    fextns: Tuple[str] = (".json",)
    dtypes: Tuple[Type] = (dict, list)

    def _load(self, fpath: str | Path, **options) -> Dict | List:
        with open(fpath, "r") as f:
            return json.load(f, **options)

    def _dump(self, obj: Dict | List, fpath: str | Path, **options):
        with open(fpath, "w") as f:
            json.dump(obj, f, **options)


data_io_instances = [
    CSVIO(),
    YAMLIO(),
    PickleIO(),
    ParquetIO(),
    JSONIO(),
]


data_io_dict = {}
for data_io in data_io_instances:
    data_io_dict.update({fextn: data_io for fextn in data_io.fextns})
