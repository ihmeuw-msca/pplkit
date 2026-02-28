import collections
import os
import pathlib

type PathLike = str | os.PathLike[str]


class PathRegistry(collections.abc.MutableMapping[str, pathlib.Path]):
    def __init__(self, **data: PathLike) -> None:
        self._data = {}
        for key, value in data.items():
            self[key] = value

    def __getitem__(self, key: str) -> pathlib.Path:
        return self._data[key]

    def __setitem__(self, key: str, value: PathLike) -> None:
        if not isinstance(key, str):
            raise TypeError(
                f"PathRegistry key must be a 'str', not {type(key).__name__!r}"
            )
        self._data[key] = pathlib.Path(value)

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def __iter__(self) -> collections.Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        items = ", ".join(
            [f"{key}='{value}'" for key, value in self._data.items()]
        )
        return f"{type(self).__name__}({items})"
