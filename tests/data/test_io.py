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
    CSVIO.dump(df, tmp_path / "file.csv")
    loaded_data = CSVIO.load(tmp_path / "file.csv")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_jsonio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    JSONIO.dump(data, tmp_path / "file.json")
    loaded_data = JSONIO.load(tmp_path / "file.json")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_yamlio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    YAMLIO.dump(data, tmp_path / "file.yaml")
    loaded_data = YAMLIO.load(tmp_path / "file.yaml")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_parquetio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    df = pd.DataFrame(data)
    ParquetIO.dump(df, tmp_path / "file.parquet")
    loaded_data = ParquetIO.load(tmp_path / "file.parquet")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_pickleio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    PickleIO.dump(data, tmp_path / "file.pkl")
    loaded_data = PickleIO.load(tmp_path / "file.pkl")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_tomlio(data: dict[str, list[int]], tmp_path: pathlib.Path) -> None:
    TOMLIO.dump(data, tmp_path / "file.toml")
    loaded_data = TOMLIO.load(tmp_path / "file.toml")

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_load_missing_file(tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError):
        JSONIO.load(tmp_path / "nonexistent.json")
