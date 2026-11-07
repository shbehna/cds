import pytest

from cds import (
    ConfigurationError,
    ContractConfig,
    DataProductConfig,
    GroupConfig,
    NamingSettings,
    QualitySettings,
    ScdSettings,
    SchemaSettings,
)
from cds.config import resolve_settings, resolved


def test_defaults_apply_without_configuration() -> None:
    settings = resolve_settings(ScdSettings, None)

    assert settings.table == "{table}_history"
    assert settings.on_duplicate_keys == "fail"


def test_most_specific_level_wins_attribute_by_attribute() -> None:
    group = GroupConfig(name="g", naming=NamingSettings(catalog="{environment}_catalog", schema="{group}"))
    data_product = DataProductConfig(name="d", naming=NamingSettings(schema="{data_product}"))
    contract = ContractConfig(name="c")

    settings = resolve_settings(NamingSettings, contract, data_product, group)

    assert settings == NamingSettings(catalog="{environment}_catalog", schema="{data_product}")


def test_mappings_are_merged_key_by_key() -> None:
    group = GroupConfig(name="g", quality=QualitySettings(criticality={"required": "warn", "enum": "warn"}))
    contract = ContractConfig(name="c", quality=QualitySettings(criticality={"enum": "error"}))

    criticality = resolved(resolve_settings(QualitySettings, contract, group).criticality)

    assert criticality["required"] == "warn"
    assert criticality["enum"] == "error"
    assert criticality["foreign_key"] == "error"


def test_settings_can_be_mappings() -> None:
    config = ContractConfig(name="c", scd={"table": "{table}_versions"})  # type: ignore[arg-type]

    assert resolve_settings(ScdSettings, config).table == "{table}_versions"


def test_invalid_settings() -> None:
    with pytest.raises(ConfigurationError, match="on_breaking_change"):
        SchemaSettings(on_breaking_change="drop")
    with pytest.raises(ConfigurationError, match="unknown check kind"):
        QualitySettings(criticality={"nulls": "warn"})
    with pytest.raises(ConfigurationError, match="'error' or 'warn'"):
        QualitySettings(criticality={"required": "fatal"})
    with pytest.raises(ConfigurationError, match="must be a ScdSettings"):
        resolve_settings(ScdSettings, ContractConfig(name="c", scd="history"))  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="Invalid scd settings"):
        resolve_settings(ScdSettings, ContractConfig(name="c", scd={"unknown": 1}))  # type: ignore[arg-type]


def test_subclasses_keep_their_class_attributes() -> None:
    class EnterpriseConfig(ContractConfig):
        name = "enterprise"
        scd = ScdSettings(enabled=False)

        def transform(self, spark: str) -> str:
            return spark

    config = EnterpriseConfig(description="Enterprises")

    assert config.name == "enterprise"
    assert config.description == "Enterprises"
    assert resolve_settings(ScdSettings, config).enabled is False
    assert config.transform("spark") == "spark"


def test_duck_typed_configurations() -> None:
    class Plain:
        name = "c"
        quality = {"enabled": False}

    assert resolve_settings(QualitySettings, Plain()).enabled is False
    assert QualitySettings(errors_column="e", warnings_column="w").result_columns == ("e", "w")
