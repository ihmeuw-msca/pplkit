import pathlib

import pytest

from pplkit.data.path_registry import PathRegistry


class TestInit:
    def test_empty(self) -> None:
        reg = PathRegistry()
        assert len(reg) == 0

    def test_kwargs(self) -> None:
        reg = PathRegistry(raw="/data/raw", output=pathlib.Path("/data/output"))
        assert len(reg) == 2
        assert reg["raw"] == pathlib.Path("/data/raw")
        assert reg["output"] == pathlib.Path("/data/output")

    def test_values_are_path_instances(self) -> None:
        reg = PathRegistry(raw="/data/raw")
        assert isinstance(reg["raw"], pathlib.Path)


class TestGetItem:
    def test_existing_key(self) -> None:
        reg = PathRegistry(raw="/data/raw")
        assert reg["raw"] == pathlib.Path("/data/raw")

    def test_missing_key(self) -> None:
        reg = PathRegistry()
        with pytest.raises(KeyError):
            reg["missing"]


class TestSetItem:
    def test_str_value(self) -> None:
        reg = PathRegistry()
        reg["raw"] = "/data/raw"
        assert reg["raw"] == pathlib.Path("/data/raw")
        assert isinstance(reg["raw"], pathlib.Path)

    def test_path_value(self) -> None:
        reg = PathRegistry()
        reg["raw"] = pathlib.Path("/data/raw")
        assert reg["raw"] == pathlib.Path("/data/raw")

    def test_overwrite(self) -> None:
        reg = PathRegistry(raw="/data/raw")
        reg["raw"] = "/data/raw_v2"
        assert reg["raw"] == pathlib.Path("/data/raw_v2")
        assert len(reg) == 1

    def test_invalid_key_type(self) -> None:
        reg = PathRegistry()
        with pytest.raises(TypeError, match="must be a"):
            reg[123] = "/data/raw"  # type: ignore[index]

    def test_invalid_value_type(self) -> None:
        reg = PathRegistry()
        with pytest.raises(TypeError):
            reg["raw"] = 123  # type: ignore[assignment]


class TestDelItem:
    def test_existing_key(self) -> None:
        reg = PathRegistry(raw="/data/raw", output="/data/output")
        del reg["raw"]
        assert "raw" not in reg
        assert len(reg) == 1

    def test_missing_key(self) -> None:
        reg = PathRegistry()
        with pytest.raises(KeyError):
            del reg["missing"]


class TestIterLen:
    def test_iter(self) -> None:
        reg = PathRegistry(raw="/data/raw", output="/data/output")
        assert set(reg) == {"raw", "output"}

    def test_len(self) -> None:
        assert len(PathRegistry()) == 0
        assert len(PathRegistry(a="/a", b="/b", c="/c")) == 3


class TestRepr:
    def test_empty(self) -> None:
        assert repr(PathRegistry()) == "PathRegistry()"

    def test_nonempty(self) -> None:
        reg = PathRegistry(raw="/data/raw")
        result = repr(reg)
        assert "PathRegistry(" in result
        assert "raw=" in result
