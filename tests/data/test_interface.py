import pathlib

import numpy as np
import pandas as pd
import pytest

from pplkit.data import DataInterface


@pytest.fixture
def data() -> dict[str, list[int]]:
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


class TestInit:
    def test_empty(self) -> None:
        di = DataInterface()
        assert len(di) == 0

    def test_kwargs(self) -> None:
        di = DataInterface(raw="/data/raw", output=pathlib.Path("/data/output"))
        assert len(di) == 2
        assert di["raw"] == pathlib.Path("/data/raw")
        assert di["output"] == pathlib.Path("/data/output")

    def test_values_are_path_instances(self) -> None:
        di = DataInterface(raw="/data/raw")
        assert isinstance(di["raw"], pathlib.Path)


class TestGetItem:
    def test_existing_key(self) -> None:
        di = DataInterface(raw="/data/raw")
        assert di["raw"] == pathlib.Path("/data/raw")

    def test_missing_key(self) -> None:
        di = DataInterface()
        with pytest.raises(KeyError):
            di["missing"]

    def test_none_key(self) -> None:
        di = DataInterface()
        assert di[None] == pathlib.Path()


class TestSetItem:
    def test_str_value(self) -> None:
        di = DataInterface()
        di["raw"] = "/data/raw"
        assert di["raw"] == pathlib.Path("/data/raw")
        assert isinstance(di["raw"], pathlib.Path)

    def test_path_value(self) -> None:
        di = DataInterface()
        di["raw"] = pathlib.Path("/data/raw")
        assert di["raw"] == pathlib.Path("/data/raw")

    def test_overwrite(self) -> None:
        di = DataInterface(raw="/data/raw")
        di["raw"] = "/data/raw_v2"
        assert di["raw"] == pathlib.Path("/data/raw_v2")
        assert len(di) == 1

    def test_invalid_key_type(self) -> None:
        di = DataInterface()
        with pytest.raises(TypeError, match="must be a"):
            di[123] = "/data/raw"  # type: ignore[index]

    def test_invalid_value_type(self) -> None:
        di = DataInterface()
        with pytest.raises(TypeError):
            di["raw"] = 123  # type: ignore[assignment]


class TestDelItem:
    def test_existing_key(self) -> None:
        di = DataInterface(raw="/data/raw", output="/data/output")
        del di["raw"]
        assert len(di) == 1

    def test_missing_key(self) -> None:
        di = DataInterface()
        with pytest.raises(KeyError):
            del di["missing"]


class TestLen:
    def test_len(self) -> None:
        assert len(DataInterface()) == 0
        assert len(DataInterface(a="/a", b="/b", c="/c")) == 3


class TestRepr:
    def test_empty(self) -> None:
        assert repr(DataInterface()) == "DataInterface()"

    def test_nonempty(self) -> None:
        di = DataInterface(raw="/data/raw")
        result = repr(di)
        assert "DataInterface(" in result
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
        di = DataInterface(tmp=tmp_path)
        obj = pd.DataFrame(data) if fextn in [".csv", ".parquet"] else data
        di.dump(obj, "data" + fextn, key="tmp")
        loaded_data = di.load("data" + fextn, key="tmp")

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])

    def test_without_key(
        self,
        data: dict[str, list[int]],
        fextn: str,
        tmp_path: pathlib.Path,
    ) -> None:
        di = DataInterface()
        fpath = tmp_path / ("data" + fextn)
        obj = pd.DataFrame(data) if fextn in [".csv", ".parquet"] else data
        di.dump(obj, fpath)
        loaded_data = di.load(fpath)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])
