import json
import pathlib
import tomllib
import typing

import dill
import pandas as pd
import tomli_w
import yaml

type Suffix = str
type SuffixAlias = str


class Loader[T](typing.Protocol):
    def __call__(self, path: pathlib.Path, **options: typing.Any) -> T: ...


class Dumper[T](typing.Protocol):
    def __call__(
        self, obj: T, path: pathlib.Path, **options: typing.Any
    ) -> None: ...


_loaders: dict[tuple[Suffix, type], Loader[typing.Any]] = {}
_dumpers: dict[tuple[Suffix, type], Dumper[typing.Any]] = {}
_loader_obj_types: dict[Suffix, list[type]] = {}
_dumper_obj_types: dict[Suffix, list[type]] = {}
_suffix_aliases: dict[SuffixAlias, Suffix] = {}


def register_suffix_alias(suffix: Suffix, suffix_alias: SuffixAlias) -> None:
    _suffix_aliases[suffix_alias] = suffix


def resolve_suffix(suffix: Suffix) -> str:
    return _suffix_aliases.get(suffix, suffix)


def _register_obj_type(
    suffix: Suffix,
    obj_type: type,
    object_type_registry: dict[Suffix, list[type]],
) -> None:
    obj_types = object_type_registry.get(suffix, [])
    if obj_type not in obj_types:
        if obj_type is object:
            obj_types.append(obj_type)
        else:
            obj_types.insert(0, obj_type)
    object_type_registry[suffix] = obj_types


def resolve_obj_type(obj_type: type, registered_obj_types: list[type]) -> type:
    if obj_type in registered_obj_types:
        return obj_type
    for registered_obj_type in registered_obj_types:
        if issubclass(obj_type, registered_obj_type):
            return registered_obj_type
    raise TypeError(
        f"Cannot resolve {obj_type.__name__!r} from "
        f"{[t.__name__ for t in registered_obj_types]}"
    )


def get_default_loader(suffix: Suffix) -> Loader[typing.Any]:
    default_obj_type = _loader_obj_types[suffix][-1]
    return _loaders[(suffix, default_obj_type)]


def get_loader(suffix: Suffix, obj_type: type) -> Loader[typing.Any]:
    return _loaders[(suffix, obj_type)]


def get_dumper(suffix: Suffix, obj_type: type) -> Dumper[typing.Any]:
    return _dumpers[(suffix, obj_type)]


def get_loader_obj_types(suffix: Suffix) -> list[type]:
    return _loader_obj_types[suffix]


def get_dumper_obj_types(suffix: Suffix) -> list[type]:
    return _dumper_obj_types[suffix]


def register_loader(
    suffix: Suffix, obj_type: type
) -> typing.Callable[[Loader[typing.Any]], Loader[typing.Any]]:
    def register_loader_decorator(
        loader: Loader[typing.Any],
    ) -> Loader[typing.Any]:
        _loaders[(suffix, obj_type)] = loader
        _register_obj_type(suffix, obj_type, _loader_obj_types)
        return loader

    return register_loader_decorator


def register_dumper(
    suffix: Suffix, obj_type: type
) -> typing.Callable[[Dumper[typing.Any]], Dumper[typing.Any]]:
    def register_dumper_decorator(
        dumper: Dumper[typing.Any],
    ) -> Dumper[typing.Any]:
        _dumpers[(suffix, obj_type)] = dumper
        _register_obj_type(suffix, obj_type, _dumper_obj_types)
        return dumper

    return register_dumper_decorator


@register_loader(".csv", pd.DataFrame)
def load_csv_as_pandas(
    path: pathlib.Path, **options: typing.Any
) -> pd.DataFrame:
    return pd.read_csv(path, **options)


@register_dumper(".csv", pd.DataFrame)
def dump_csv_from_pandas(
    obj: pd.DataFrame, path: pathlib.Path, **options: typing.Any
) -> None:
    options = dict(index=False) | options
    obj.to_csv(path, **options)


@register_loader(".pkl", object)
def load_pickle(path: pathlib.Path, **options: typing.Any) -> typing.Any:
    with open(path, "rb") as f:
        return dill.load(f, **options)


@register_dumper(".pkl", object)
def dump_pickle(
    obj: typing.Any, path: pathlib.Path, **options: typing.Any
) -> None:
    with open(path, "wb") as f:
        dill.dump(obj, f, **options)


@register_loader(".yaml", object)
def load_yaml(path: pathlib.Path, **options: typing.Any) -> object:
    options = dict(Loader=yaml.SafeLoader) | options
    with open(path, "r") as f:
        return yaml.load(f, **options)


@register_dumper(".yaml", object)
def dump_yaml(obj: object, path: pathlib.Path, **options: typing.Any) -> None:
    options = dict(Dumper=yaml.SafeDumper) | options
    with open(path, "w") as f:
        yaml.dump(obj, f, **options)


@register_loader(".parquet", pd.DataFrame)
def load_parquet_as_pandas(
    path: pathlib.Path, **options: typing.Any
) -> pd.DataFrame:
    options = dict(engine="pyarrow") | options
    return pd.read_parquet(path, **options)


@register_dumper(".parquet", pd.DataFrame)
def dump_parquet_from_pandas(
    obj: pd.DataFrame, path: pathlib.Path, **options: typing.Any
) -> None:
    options = dict(engine="pyarrow") | options
    obj.to_parquet(path, **options)


@register_loader(".json", object)
def load_json(path: pathlib.Path, **options: typing.Any) -> object:
    with open(path, "r") as f:
        return json.load(f, **options)


@register_dumper(".json", object)
def dump_json(obj: object, path: pathlib.Path, **options: typing.Any) -> None:
    with open(path, "w") as f:
        json.dump(obj, f, **options)


@register_loader(".toml", dict)
def load_toml(path: pathlib.Path, **options: typing.Any) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f, **options)


@register_dumper(".toml", dict)
def dump_toml(obj: dict, path: pathlib.Path, **options: typing.Any) -> None:
    with open(path, "wb") as f:
        tomli_w.dump(obj, f, **options)


register_suffix_alias(suffix=".pkl", suffix_alias=".pickle")
register_suffix_alias(suffix=".yaml", suffix_alias=".yml")
