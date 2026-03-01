import pathlib

import numpy as np
import pandas as pd
import pytest

from pplkit.io import (
    get_dumper,
    get_loader,
    register_dumper,
    register_loader,
    register_suffix_alias,
)


@pytest.fixture
def data() -> dict[str, list[int]]:
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


# ---------------------------------------------------------------------------
# get_loader
# ---------------------------------------------------------------------------


class TestGetLoader:
    """Tests for ``get_loader``."""

    def test_get_default_loader(self) -> None:
        loader = get_loader(".csv")
        assert callable(loader)

    def test_get_loader_by_obj_type(self) -> None:
        loader = get_loader(".csv", obj_type=pd.DataFrame)
        assert callable(loader)

    def test_get_loader_missing_obj_type_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot resolve"):
            get_loader(".csv", obj_type=int)

    def test_get_loader_json_default(self) -> None:
        loader = get_loader(".json")
        assert callable(loader)

    def test_get_loader_via_alias_yml(self) -> None:
        loader = get_loader(".yml")
        assert callable(loader)

    def test_get_loader_via_alias_pickle(self) -> None:
        loader = get_loader(".pickle")
        assert callable(loader)


# ---------------------------------------------------------------------------
# get_dumper
# ---------------------------------------------------------------------------


class TestGetDumper:
    """Tests for ``get_dumper``."""

    def test_get_dumper(self) -> None:
        dumper = get_dumper(".csv", pd.DataFrame)
        assert callable(dumper)

    def test_get_dumper_via_subclass_resolution(self) -> None:
        dumper = get_dumper(".pkl", dict)
        assert callable(dumper)

    def test_get_dumper_missing_obj_type_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot resolve"):
            get_dumper(".csv", int)

    def test_get_dumper_via_alias_yml(self) -> None:
        dumper = get_dumper(".yml", object)
        assert callable(dumper)


# ---------------------------------------------------------------------------
# register_loader / register_dumper
# ---------------------------------------------------------------------------


class TestRegisterHandlers:
    """Tests for ``register_loader`` and ``register_dumper``."""

    def test_register_and_retrieve_custom_loader(
        self, tmp_path: pathlib.Path
    ) -> None:
        @register_loader(".npy", object)
        def _load_npy(path: pathlib.Path, **options):
            return np.load(path, **options)

        loader = get_loader(".npy")
        assert callable(loader)

        path = tmp_path / "test.npy"
        np.save(path, np.array([1, 2, 3]))
        result = loader(path)
        assert np.allclose(result, [1, 2, 3])

    def test_register_and_retrieve_custom_dumper(
        self, tmp_path: pathlib.Path
    ) -> None:
        @register_dumper(".npy", object)
        def _dump_npy(obj, path: pathlib.Path, **options):
            np.save(path, obj, **options)

        dumper = get_dumper(".npy", object)
        assert callable(dumper)

        path = tmp_path / "test.npy"
        dumper(np.array([4, 5, 6]), path)
        assert np.allclose(np.load(path), [4, 5, 6])


# ---------------------------------------------------------------------------
# register_suffix_alias
# ---------------------------------------------------------------------------


class TestRegisterSuffixAlias:
    """Tests for ``register_suffix_alias``."""

    def test_custom_alias(self, tmp_path: pathlib.Path) -> None:
        register_suffix_alias(suffix=".yaml", suffix_alias=".conf")

        dumper = get_dumper(".conf", object)
        loader = get_loader(".conf")

        path = tmp_path / "settings.conf"
        dumper({"key": "value"}, path)
        loaded = loader(path)
        assert loaded == {"key": "value"}


# ---------------------------------------------------------------------------
# Round-trip integration tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "suffix", [".csv", ".pkl", ".yaml", ".parquet", ".json", ".toml"]
)
class TestLoaderDumperRoundTrip:
    """Dump then load for every registered format and verify data integrity."""

    def test_round_trip(
        self,
        data: dict[str, list[int]],
        suffix: str,
        tmp_path: pathlib.Path,
    ) -> None:
        path = tmp_path / f"data{suffix}"
        obj = pd.DataFrame(data) if suffix in {".csv", ".parquet"} else data

        dumper = get_dumper(suffix, type(obj))
        dumper(obj, path)

        loader = get_loader(suffix)
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])


class TestSuffixAliasRoundTrip:
    """Verify that suffix aliases work end-to-end through dump/load."""

    def test_pickle_alias(
        self, data: dict[str, list[int]], tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "data.pickle"

        dumper = get_dumper(".pickle", object)
        dumper(data, path)

        loader = get_loader(".pickle")
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])

    def test_yml_alias(
        self, data: dict[str, list[int]], tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "data.yml"

        dumper = get_dumper(".yml", object)
        dumper(data, path)

        loader = get_loader(".yml")
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestLoadMissingFile:
    """Loading a nonexistent file should surface a ``FileNotFoundError``."""

    def test_missing_file(self, tmp_path: pathlib.Path) -> None:
        loader = get_loader(".json")
        with pytest.raises(FileNotFoundError):
            loader(tmp_path / "nonexistent.json")
