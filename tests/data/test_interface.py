import numpy as np
import pandas as pd
import pytest

from pplkit.data import DataInterface


@pytest.fixture
def data():
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


@pytest.mark.parametrize(
    "fextn", [".json", ".yaml", ".pkl", ".csv", ".parquet", ".toml"]
)
def test_data_interface(data, fextn, tmp_path):
    dataif = DataInterface(tmp=tmp_path)
    if fextn in [".csv", ".parquet"]:
        data = pd.DataFrame(data)
    dataif.dump_tmp(data, "data" + fextn)
    loaded_data = dataif.load_tmp("data" + fextn)

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])


def test_add_dir(tmp_path):
    dataif = DataInterface()
    assert len(dataif.keys) == 0
    dataif.add_dir("tmp", tmp_path)
    assert len(dataif.keys) == 1
    assert hasattr(dataif, "tmp")
    assert hasattr(dataif, "load_tmp")
    assert hasattr(dataif, "dump_tmp")


def test_add_dir_exist_ok(tmp_path):
    dataif = DataInterface(tmp=tmp_path)
    with pytest.raises(ValueError):
        dataif.add_dir("tmp", tmp_path)
    dataif.add_dir("tmp", tmp_path, exist_ok=True)


def test_remove_dir(tmp_path):
    dataif = DataInterface(tmp=tmp_path)
    assert len(dataif.keys) == 1
    dataif.remove_dir("tmp")
    assert len(dataif.keys) == 0
