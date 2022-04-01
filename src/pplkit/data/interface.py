from functools import partial
from pathlib import Path
from typing import Any, Dict, Tuple

from pplkit.data.io import DataIO, data_io_dict


class DataInterface:
    """Data interface that store important directories and automatically read
    and write data to the stored directories based on their data types.

    Parameters
    ----------
    dirs
        Directories to manage with directory's name as the name of the keyword
        argument's name and directory's path as the value of the keyword
        argument's value.

    """

    data_io_dict: Dict[str, DataIO] = data_io_dict
    """A dictionary that maps the file extensions to the corresponding data io
    class. This is a module-level variable from
    :py:data:`pplkit.data.io.data_io_dict`.

    :meta hide-value:

    """

    def __init__(self, **dirs: Dict[str, str | Path]):
        for key, value in dirs.items():
            setattr(self, key, Path(value))
            setattr(self, f"load_{key}", partial(self.load, key=key))
            setattr(self, f"dump_{key}", partial(self.dump, key=key))
        self.keys = list(dirs.keys())

    def get_fpath(self, *fparts: Tuple[str, ...], key: str = "") -> Path:
        """Get the file path from the name of the directory and the sub-parts
        under the directory.

        Parameters
        ----------
        fparts
            Subdirectories or the file name.
        key
            The name of the directory stored in the class.

        """
        return getattr(self, key, Path(".")) / "/".join(map(str, fparts))

    def load(self, *fparts: Tuple[str, ...], key: str = "",
             **options: Dict[str, Any]) -> Any:
        """Load data from given directory.

        Parameters
        ----------
        fparts
            Subdirectories or the file name.
        key
            The name of the directory stored in the class.
        options
            Extra arguments for the load function.

        Returns
        -------
        Any
            Data loaded from the given path.

        """
        fpath = self.get_fpath(*fparts, key=key)
        return self.data_io_dict[fpath.suffix].load(fpath, **options)

    def dump(self, obj: Any, *fparts: str, key: str = "",
             mkdir: bool = True, **options: Dict[str, Any]):
        """Dump data to the given directory.

        Parameters
        ----------
        obj
            Provided data object.
        fparts
            Subdirectories or the file name.
        key
            The name of the directory stored in the class.
        mkdir
            If true, it will automatically create the parent directory. The
            default is true.
        options
            Extra arguments for the dump function.

        """
        fpath = self.get_fpath(*fparts, key=key)
        self.data_io_dict[fpath.suffix].dump(obj, fpath, mkdir=mkdir, **options)

    def __repr__(self) -> str:
        expr = f"{type(self).__name__}(\n"
        for key in self.keys:
            expr += f"    {key}={getattr(self, key)},\n"
        expr += ")"
        return expr
