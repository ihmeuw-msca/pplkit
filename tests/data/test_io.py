import pathlib

import numpy as np
import pandas as pd
import pytest

from pplkit.data.io import (
    get_default_loader,
    get_dumper,
    get_dumper_obj_types,
    get_loader,
    get_loader_obj_types,
    resolve_obj_type,
    resolve_suffix,
)


@pytest.fixture
def data() -> dict[str, list[int]]:
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


class TestSuffixAlias:
    def test_resolve_known_alias(self) -> None:
        assert resolve_suffix(".pickle") == ".pkl"
        assert resolve_suffix(".yml") == ".yaml"

    def test_resolve_canonical(self) -> None:
        assert resolve_suffix(".csv") == ".csv"
        assert resolve_suffix(".json") == ".json"

    def test_resolve_unknown(self) -> None:
        assert resolve_suffix(".xyz") == ".xyz"


class TestResolveObjType:
    def test_exact_match(self) -> None:
        assert resolve_obj_type(dict, [dict, object]) is dict

    def test_subclass_fallback(self) -> None:
        assert resolve_obj_type(dict, [object]) is object

    def test_no_match(self) -> None:
        with pytest.raises(TypeError, match="Cannot resolve"):
            resolve_obj_type(int, [pd.DataFrame])


class TestGetLoader:
    def test_get_default_loader(self) -> None:
        loader = get_default_loader(".csv")
        assert callable(loader)

    def test_get_loader_by_type(self) -> None:
        loader = get_loader(".csv", pd.DataFrame)
        assert callable(loader)

    def test_get_loader_missing_key(self) -> None:
        with pytest.raises(KeyError):
            get_loader(".csv", dict)

    def test_get_loader_obj_types(self) -> None:
        obj_types = get_loader_obj_types(".csv")
        assert pd.DataFrame in obj_types

    def test_get_loader_obj_types_with_object(self) -> None:
        obj_types = get_loader_obj_types(".json")
        assert object in obj_types


@pytest.mark.parametrize(
    "suffix", [".csv", ".pkl", ".yaml", ".parquet", ".json", ".toml"]
)
class TestLoaderDumperRoundTrip:
    def test_round_trip(
        self,
        data: dict[str, list[int]],
        suffix: str,
        tmp_path: pathlib.Path,
    ) -> None:
        path = tmp_path / f"data{suffix}"
        obj = pd.DataFrame(data) if suffix in [".csv", ".parquet"] else data

        obj_type = resolve_obj_type(type(obj), get_dumper_obj_types(suffix))
        dumper = get_dumper(suffix, obj_type)
        dumper(obj, path)

        loader = get_default_loader(suffix)
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])


class TestSuffixAliasRoundTrip:
    def test_pickle_alias(
        self, data: dict[str, list[int]], tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "data.pickle"
        resolved = resolve_suffix(path.suffix)

        dumper = get_dumper(resolved, object)
        dumper(data, path)

        loader = get_default_loader(resolved)
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])

    def test_yml_alias(
        self, data: dict[str, list[int]], tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "data.yml"
        resolved = resolve_suffix(path.suffix)

        dumper = get_dumper(resolved, object)
        dumper(data, path)

        loader = get_default_loader(resolved)
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])


class TestLoadMissingFile:
    def test_missing_file(self, tmp_path: pathlib.Path) -> None:
        loader = get_default_loader(".json")
        with pytest.raises(FileNotFoundError):
            loader(tmp_path / "nonexistent.json")
