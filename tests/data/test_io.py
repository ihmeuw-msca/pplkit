import pathlib

import numpy as np
import pandas as pd
import pytest

from pplkit.data.io import CSVIO, JSONIO, TOMLIO, YAMLIO, ParquetIO, PickleIO


@pytest.fixture
def data() -> dict[str, list[int]]:
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


def test_csvio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    df = pd.DataFrame(data)
    port = CSVIO()
    port.dump(df, tmp_path / "file.csv")
    loaded_data = port.load(tmp_path / "file.csv")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_jsonio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    port = JSONIO()
    port.dump(data, tmp_path / "file.json")
    loaded_data = port.load(tmp_path / "file.json")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_yamlio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    port = YAMLIO()
    port.dump(data, tmp_path / "file.yaml")
    loaded_data = port.load(tmp_path / "file.yaml")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_parquetio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    df = pd.DataFrame(data)
    port = ParquetIO()
    port.dump(df, tmp_path / "file.parquet")
    loaded_data = port.load(tmp_path / "file.parquet")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_pickleio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    port = PickleIO()
    port.dump(data, tmp_path / "file.pkl")
    loaded_data = port.load(tmp_path / "file.pkl")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_tomlio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    port = TOMLIO()
    port.dump(data, tmp_path / "file.toml")
    loaded_data = port.load(tmp_path / "file.toml")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_load_invalid_extension(tmp_path: pathlib.Path) -> None:
    port = CSVIO()
    fpath = tmp_path / "file.txt"
    fpath.touch()
    with pytest.raises(ValueError, match="File extension must be in"):
        port.load(fpath)


def test_dump_invalid_type(tmp_path: pathlib.Path) -> None:
    port = CSVIO()
    with pytest.raises(TypeError, match="Data must be an instance of"):
        port.dump({"a": 1}, tmp_path / "file.csv")


def test_load_missing_file(tmp_path: pathlib.Path) -> None:
    port = JSONIO()
    with pytest.raises(FileNotFoundError):
        port.load(tmp_path / "nonexistent.json")
