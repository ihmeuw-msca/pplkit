import pathlib

import numpy as np
import pandas as pd
import pytest

from pplkit.data.io import (
    DumperRegistry,
    IORegistry,
    LoaderRegistry,
)


@pytest.fixture
def data() -> dict[str, list[int]]:
    return {"a": [1, 2, 3], "b": [4, 5, 6]}


# ---------------------------------------------------------------------------
# Suffix alias resolution
# ---------------------------------------------------------------------------


class TestSuffixAlias:
    """Tests for ``IORegistry._resolve_suffix`` and alias registration."""

    def test_resolve_known_alias_pickle(self) -> None:
        assert IORegistry._resolve_suffix(".pickle") == ".pkl"

    def test_resolve_known_alias_yml(self) -> None:
        assert IORegistry._resolve_suffix(".yml") == ".yaml"

    def test_resolve_canonical_suffix(self) -> None:
        assert IORegistry._resolve_suffix(".csv") == ".csv"
        assert IORegistry._resolve_suffix(".json") == ".json"

    def test_resolve_unknown_suffix_returns_as_is(self) -> None:
        assert IORegistry._resolve_suffix(".xyz") == ".xyz"


# ---------------------------------------------------------------------------
# Object-type resolution
# ---------------------------------------------------------------------------


class TestResolveObjType:
    """Tests for ``IORegistry._resolve_obj_type``."""

    def test_exact_match(self) -> None:
        assert IORegistry._resolve_obj_type(dict, [dict, object]) is dict

    def test_subclass_fallback(self) -> None:
        assert IORegistry._resolve_obj_type(dict, [object]) is object

    def test_none_returns_last_registered(self) -> None:
        """When *obj_type* is ``None`` the last (default) entry is returned."""
        assert (
            IORegistry._resolve_obj_type(None, [pd.DataFrame, object]) is object
        )

    def test_no_match_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot resolve"):
            IORegistry._resolve_obj_type(int, [pd.DataFrame])


# ---------------------------------------------------------------------------
# LoaderRegistry
# ---------------------------------------------------------------------------


class TestLoaderRegistry:
    """Tests for ``LoaderRegistry.get_loader``."""

    def test_get_default_loader(self) -> None:
        loader = LoaderRegistry.get_loader(".csv")
        assert callable(loader)

    def test_get_loader_by_obj_type(self) -> None:
        loader = LoaderRegistry.get_loader(".csv", obj_type=pd.DataFrame)
        assert callable(loader)

    def test_get_loader_missing_obj_type_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot resolve"):
            LoaderRegistry.get_loader(".csv", obj_type=int)

    def test_loader_obj_types_csv(self) -> None:
        obj_types = LoaderRegistry._io_obj_types[".csv"]
        assert pd.DataFrame in obj_types

    def test_loader_obj_types_json_has_object(self) -> None:
        obj_types = LoaderRegistry._io_obj_types[".json"]
        assert object in obj_types


# ---------------------------------------------------------------------------
# DumperRegistry
# ---------------------------------------------------------------------------


class TestDumperRegistry:
    """Tests for ``DumperRegistry.get_dumper``."""

    def test_get_dumper(self) -> None:
        dumper = DumperRegistry.get_dumper(".csv", pd.DataFrame)
        assert callable(dumper)

    def test_get_dumper_via_subclass_resolution(self) -> None:
        dumper = DumperRegistry.get_dumper(".pkl", dict)
        assert callable(dumper)

    def test_get_dumper_missing_obj_type_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot resolve"):
            DumperRegistry.get_dumper(".csv", int)


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

        dumper = DumperRegistry.get_dumper(suffix, type(obj))
        dumper(obj, path)

        loader = LoaderRegistry.get_loader(suffix)
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])


class TestSuffixAliasRoundTrip:
    """Verify that suffix aliases work end-to-end through dump/load."""

    def test_pickle_alias(
        self, data: dict[str, list[int]], tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "data.pickle"
        resolved = IORegistry._resolve_suffix(path.suffix)

        dumper = DumperRegistry.get_dumper(resolved, object)
        dumper(data, path)

        loader = LoaderRegistry.get_loader(resolved)
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])

    def test_yml_alias(
        self, data: dict[str, list[int]], tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "data.yml"
        resolved = IORegistry._resolve_suffix(path.suffix)

        dumper = DumperRegistry.get_dumper(resolved, object)
        dumper(data, path)

        loader = LoaderRegistry.get_loader(resolved)
        loaded_data = loader(path)

        for key in ["a", "b"]:
            assert np.allclose(data[key], loaded_data[key])


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestLoadMissingFile:
    """Loading a nonexistent file should surface a ``FileNotFoundError``."""

    def test_missing_file(self, tmp_path: pathlib.Path) -> None:
        loader = LoaderRegistry.get_loader(".json")
        with pytest.raises(FileNotFoundError):
            loader(tmp_path / "nonexistent.json")
