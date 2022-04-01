import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pplkit.data.interface import DataInterface

tmpdir = Path(__file__).parents[1] / "tmp"


@pytest.fixture
def data():
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


@pytest.fixture(scope="class", autouse=True)
def rm_tmpdir_after_tests():
    yield
    shutil.rmtree(tmpdir)


@pytest.mark.parametrize(
    "fextn", [".json", ".yaml", ".pkl", ".csv", ".parquet"]
)
def test_data_interface(data, fextn):
    dataif = DataInterface(tmp=tmpdir)
    if fextn in [".csv", ".parquet"]:
        data = pd.DataFrame(data)
    dataif.dump_tmp(data, "data" + fextn)
    loaded_data = dataif.load_tmp("data" + fextn)

    for key in ["a", "b"]:
        assert np.allclose(data[key], loaded_data[key])
