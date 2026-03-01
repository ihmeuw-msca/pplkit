import pathlib

import numpy as np
import pandas as pd
import pytest

from pplkit.io import IOManager


@pytest.fixture
def data() -> dict[str, list[int]]:
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


class TestInit:
    def test_empty(self) -> None:
        iom = IOManager()
        assert len(iom) == 0

    def test_kwargs(self) -> None:
        iom = IOManager(raw="/data/raw", output=pathlib.Path("/data/output"))
        assert len(iom) == 2
        assert iom["raw"] == pathlib.Path("/data/raw")
        assert iom["output"] == pathlib.Path("/data/output")

    def test_values_are_path_instances(self) -> None:
        iom = IOManager(raw="/data/raw")
        assert isinstance(iom["raw"], pathlib.Path)


class TestGetItem:
    def test_existing_key(self) -> None:
        iom = IOManager(raw="/data/raw")
        assert iom["raw"] == pathlib.Path("/data/raw")

    def test_missing_key(self) -> None:
        iom = IOManager()
        with pytest.raises(KeyError):
            iom["missing"]

    def test_none_key(self) -> None:
        iom = IOManager()
        assert iom[None] == pathlib.Path()


class TestSetItem:
    def test_str_value(self) -> None:
        iom = IOManager()
        iom["raw"] = "/data/raw"
        assert iom["raw"] == pathlib.Path("/data/raw")
        assert isinstance(iom["raw"], pathlib.Path)

    def test_path_value(self) -> None:
        iom = IOManager()
        iom["raw"] = pathlib.Path("/data/raw")
        assert iom["raw"] == pathlib.Path("/data/raw")

    def test_overwrite(self) -> None:
        iom = IOManager(raw="/data/raw")
        iom["raw"] = "/data/raw_v2"
        assert iom["raw"] == pathlib.Path("/data/raw_v2")
        assert len(iom) == 1

    def test_invalid_key_type(self) -> None:
        iom = IOManager()
        with pytest.raises(TypeError, match="must be a"):
            iom[123] = "/data/raw"  # type: ignore[index]

    def test_invalid_value_type(self) -> None:
        iom = IOManager()
        with pytest.raises(TypeError):
            iom["raw"] = 123  # type: ignore[assignment]


class TestDelItem:
    def test_existing_key(self) -> None:
        iom = IOManager(raw="/data/raw", output="/data/output")
        del iom["raw"]
        assert len(iom) == 1

    def test_missing_key(self) -> None:
        iom = IOManager()
        with pytest.raises(KeyError):
            del iom["missing"]


class TestLen:
    def test_len(self) -> None:
        assert len(IOManager()) == 0
        assert len(IOManager(a="/a", b="/b", c="/c")) == 3


class TestRepr:
    def test_empty(self) -> None:
        assert repr(IOManager()) == "IOManager()"

    def test_nonempty(self) -> None:
        iom = IOManager(raw="/data/raw")
        result = repr(iom)
        assert "IOManager(" in result
        assert "raw=" in result


@pytest.mark.parametrize(
    "fextn", [".json", ".yaml", ".pkl", ".csv", ".parquet", ".toml"]
)
class TestLoadDump:
    def test_with_key(
        self,
        data: dict[str, list[int]],
        fextn: str,
        tmp_path: pathlib.Path,
    ) -> None:
        iom = IOManager(tmp=tmp_path)
        obj = pd.DataFrame(data) if fextn in [".csv", ".parquet"] else data
        iom.dump(obj, "data" + fextn, key="tmp")
        loaded_data = iom.load("data" + fextn, key="tmp")

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])

    def test_without_key(
        self,
        data: dict[str, list[int]],
        fextn: str,
        tmp_path: pathlib.Path,
    ) -> None:
        iom = IOManager()
        fpath = tmp_path / ("data" + fextn)
        obj = pd.DataFrame(data) if fextn in [".csv", ".parquet"] else data
        iom.dump(obj, fpath)
        loaded_data = iom.load(fpath)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])
