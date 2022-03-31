"""
Data Interface
==============

Manage the data system and provide port for loading and writing files.
"""
from functools import partial
from pathlib import Path
from typing import Any, Dict

from pplkit.data.io import DataIO, data_io_dict


class DataInterface:

    data_io_dict: Dict[str, DataIO] = data_io_dict

    def __init__(self, **dirs: str | Path):
        for key, value in dirs.items():
            setattr(self, key, Path(value))
            setattr(self, f"load_{key}", partial(self.load, key=key))
            setattr(self, f"dump_{key}", partial(self.dump, key=key))
        self.keys = list(dirs.keys())

    def get_fpath(self, *fparts: str, key: str = "") -> Path:
        return getattr(self, key, Path(".")) / "/".join(map(str, fparts))

    def load(self, *fparts: str, key: str = "", **options) -> Any:
        fpath = self.get_fpath(*fparts, key=key)
        return self.data_io_dict[fpath.suffix].load(fpath, **options)

    def dump(self, obj: Any, *fparts: str,
             key: str = "", mkdir: bool = True, **options):
        fpath = self.get_fpath(*fparts, key=key)
        self.data_io_dict[fpath.suffix].dump(obj, fpath, mkdir=mkdir, **options)

    def __repr__(self) -> str:
        expr = f"{type(self).__name__}(\n"
        for key in self.keys:
            expr += f"    {key}={getattr(self, key)},\n"
        expr += ")"
        return expr
