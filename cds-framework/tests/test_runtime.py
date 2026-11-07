from unittest.mock import MagicMock

import pytest

from cds import ConfigurationError, RuntimeConfiguration
from cds.runtime import environment_values, widget_values


def test_first_source_takes_precedence_and_keys_are_case_insensitive() -> None:
    runtime = RuntimeConfiguration.from_sources({"Environment": "prod"}, {"environment": "nprod", "roots": "/a"})

    assert runtime["ENVIRONMENT"] == "prod"
    assert runtime.get("roots") == "/a"
    assert set(runtime) == {"environment", "roots"}


def test_empty_values_do_not_hide_lower_sources() -> None:
    runtime = RuntimeConfiguration.from_sources({"environment": ""}, None, {"environment": "nprod"})

    assert runtime.require("environment") == "nprod"


def test_typed_accessors() -> None:
    runtime = RuntimeConfiguration({"workers": "8", "dry_run": "Yes", "tables": " a, ,b ", "flag": True})

    assert runtime.get_int("workers") == 8
    assert runtime.get_int("missing", 3) == 3
    assert runtime.get_bool("dry_run") is True
    assert runtime.get_bool("flag") is True
    assert runtime.get_bool("missing") is False
    assert runtime.get_list("tables") == ["a", "b"]
    assert runtime.get_list("missing") == []


@pytest.mark.parametrize(
    ("method", "value"),
    [("get_int", "eight"), ("get_bool", "maybe")],
)
def test_invalid_values_raise(method: str, value: str) -> None:
    with pytest.raises(ConfigurationError, match="key"):
        getattr(RuntimeConfiguration({"key": value}), method)("key")


def test_require_names_the_environment_variable() -> None:
    with pytest.raises(ConfigurationError, match="CDS_ROOTS"):
        RuntimeConfiguration().require("roots")


def test_with_values_overrides() -> None:
    runtime = RuntimeConfiguration({"environment": "nprod", "roots": "/a"}).with_values(environment="prod")

    assert dict(runtime) == {"environment": "prod", "roots": "/a"}


def test_environment_values_use_the_prefix() -> None:
    values = environment_values({"CDS_ENVIRONMENT": "prod", "CDS_": "ignored", "PATH": "/bin"})

    assert values == {"ENVIRONMENT": "prod"}


def test_widget_values() -> None:
    dbutils = MagicMock()
    dbutils.widgets.getAll.return_value = {"environment": "prod"}

    assert widget_values(dbutils) == {"environment": "prod"}
    assert widget_values(None) == {}
    dbutils.widgets.getAll.side_effect = AttributeError("no widgets")
    assert widget_values(dbutils) == {}
