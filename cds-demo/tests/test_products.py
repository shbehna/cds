"""Tests of the data products of the demo, without Spark."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

from cds import ContractInjection, DataContext, RuntimeConfiguration, ScdSettings, ServiceContext
from cds.cli import main
from cds.services.checks import build_checks
from cds.services.scd import history_table_name
from cds.services.transformation import conform_to_contract, find_transformation

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

PRODUCTS = Path(__file__).resolve().parents[1] / "products"

# Header of each file of the REQ data set, as published (the files start with a byte order mark).
SOURCE_HEADERS = {
    "Entreprise.csv": (
        "NEQ,IND_FAIL,DAT_IMMAT,COD_REGIM_JURI,COD_INTVAL_EMPLO_QUE,DAT_CESS_PREVU,COD_STAT_IMMAT,COD_FORME_JURI,"
        "DAT_STAT_IMMAT,COD_REGIM_JURI_CONSTI,DAT_DEPO_DECLR,AN_DECL,AN_PROD,DAT_LIMIT_PROD,AN_PROD_PRE,"
        "DAT_LIMIT_PROD_PRE,DAT_MAJ_INDEX_NOM,COD_ACT_ECON_CAE,NO_ACT_ECON_ASSUJ,DESC_ACT_ECON_ASSUJ,"
        "COD_ACT_ECON_CAE2,NO_ACT_ECON_ASSUJ2,DESC_ACT_ECON_ASSUJ2,NOM_LOCLT_CONSTI,DAT_CONSTI,"
        "IND_CONVEN_UNMN_ACTNR,IND_RET_TOUT_POUVR,IND_LIMIT_RESP,DAT_DEB_RESP,DAT_FIN_RESP,OBJET_SOC,"
        "NO_MTR_VOLONT,ADR_DOMCL_ADR_DISP,ADR_DOMCL_LIGN1_ADR,ADR_DOMCL_LIGN2_ADR,ADR_DOMCL_LIGN3_ADR,"
        "ADR_DOMCL_LIGN4_ADR"
    ),
    "Etablissements.csv": (
        "NEQ,NO_SUF_ETAB,IND_ETAB_PRINC,IND_SALON_BRONZ,IND_VENTE_TABAC_DETL,IND_DISP,LIGN1_ADR,LIGN2_ADR,"
        "LIGN3_ADR,LIGN4_ADR,COD_ACT_ECON,DESC_ACT_ECON_ETAB,NO_ACT_ECON_ETAB,COD_ACT_ECON2,DESC_ACT_ECON_ETAB2,"
        "NO_ACT_ECON_ETAB2,NOM_ETAB"
    ),
    "Nom.csv": "NEQ,NOM_ASSUJ,NOM_ASSUJ_LANG_ETRNG,STAT_NOM,TYP_NOM_ASSUJ,DAT_INIT_NOM_ASSUJ,DAT_FIN_NOM_ASSUJ",
    "FusionScissions.csv": (
        "NEQ,NEQ_ASSUJ_REL,DENOMN_SOC,COD_RELA_ASSUJ,DAT_EFCTVT,IND_DISP,LIGN1_ADR,LIGN2_ADR,LIGN3_ADR,LIGN4_ADR"
    ),
    "ContinuationsTransformations.csv": "NEQ,COD_TYP_CHANG,COD_REGIM_JURI,AUTR_REGIM_JURI,NOM_LOCLT,DAT_EFCTVT",
    "DomaineValeur.csv": "TYP_DOM_VAL,COD_DOM_VAL,VAL_DOM_FRAN",
}
TABLES = [
    "continuation_transformation",
    "domaine_valeur",
    "entreprise",
    "etablissement",
    "fusion_scission",
    "nom_entreprise",
]


class Frame:
    """A DataFrame keeping its columns and the SQL expressions selected from it."""

    def __init__(
        self,
        columns: Iterable[str],
        source_columns: Sequence[str] | None = None,
        expressions: Iterable[str] = (),
        rows: Iterable[dict[str, str]] = (),
    ) -> None:
        self.columns = list(columns)
        self.source_columns = list(source_columns or self.columns)
        self.expressions = list(expressions)
        self.rows = list(rows)

    def toDF(self, *names: str) -> Frame:
        return Frame(names)

    def selectExpr(self, *expressions: str) -> Frame:
        return Frame([alias(expression) for expression in expressions], self.columns, expressions)

    def select(self, *columns: str) -> Frame:
        return self

    def collect(self) -> list[dict[str, str]]:
        return self.rows


def alias(expression: str) -> str:
    """Returns the alias of a ``selectExpr`` expression ending with ``AS `name```."""
    match = re.search(r" AS `([^`]+)`$", expression)
    assert match is not None, f"no alias in {expression!r}"
    return match.group(1)


class Reader:
    def __init__(self) -> None:
        self.paths: list[str] = []

    def csv(self, path: str, **options: object) -> Frame:
        assert options["header"] is True and options["encoding"] == "UTF-8"
        self.paths.append(path)
        columns = SOURCE_HEADERS[path.rsplit("/", 1)[1]].split(",")
        return Frame(["﻿" + columns[0], *columns[1:]])


class Spark:
    def __init__(self, tables: dict[str, Frame] | None = None) -> None:
        self.read = Reader()
        self._tables = tables or {}

    def table(self, name: str) -> Frame:
        return self._tables[name]


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


def service_context(data_context: DataContext, spark: Spark | None = None) -> ServiceContext:
    return ServiceContext(
        spark=cast("SparkSession", spark or Spark()),
        environment="nprod",
        data_context=data_context,
        runtime_configuration=RuntimeConfiguration({"environment": "nprod", "source_path": "/Volumes/ext/req/"}),
    )


def test_layout(data_context: DataContext) -> None:
    [data_product] = data_context.data_products

    assert data_product.qualified_name == "donnees_qc.req"
    assert sorted(table.name for table in data_product.tables) == TABLES
    context = service_context(data_context)
    assert context.full_table_name(data_product.get_table("entreprise")) == "nprod.donnees_qc_req.entreprise"


@pytest.mark.parametrize("table_name", TABLES)
def test_transformations_produce_the_contract_columns(data_context: DataContext, table_name: str) -> None:
    table = data_context.get_table(table_name)
    spark = Spark()
    context = service_context(data_context, spark)
    injection = ContractInjection(context, table=table, contract=table.contract, data_product=table.data_product)
    transformation = find_transformation(table)
    assert transformation is not None

    result = injection.call(transformation)

    [path] = spark.read.paths
    assert path.startswith("/Volumes/ext/req/") and "//" not in path.removeprefix("/")
    referenced = {name for expression in result.expressions for name in re.findall(r"`([^`]+)`(?! *$)", expression)}
    assert referenced <= set(result.source_columns), "unknown source columns"
    unused = set(result.source_columns) - referenced
    assert not unused, f"source columns not mapped: {sorted(unused)}"
    conform_to_contract(result, table)


@pytest.mark.parametrize("table_name", TABLES)
def test_contracts_follow_the_odcs_schema(data_context: DataContext, table_name: str) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((files("open_data_contract_standard") / "schema.json").read_text(encoding="utf-8"))
    document = data_context.get_table(table_name).contract.document
    assert document is not None

    errors = sorted(jsonschema.Draft201909Validator(schema).iter_errors(document.raw), key=str)

    assert not errors, [f"{list(error.absolute_path)}: {error.message}" for error in errors]


def test_contract_defaults_are_applied(data_context: DataContext) -> None:
    document = data_context.get_table("entreprise").contract.document
    assert document is not None

    assert document.tenant == "cds-demo"
    assert document.data_product == "donnees_qc.req"
    assert document.description["purpose"].startswith("Informations générales")
    assert "CC BY-NC-SA 4.0" in document.description["limitations"]


def test_history_settings_combine_the_group_and_the_data_product(data_context: DataContext) -> None:
    table = data_context.get_table("entreprise")

    assert history_table_name(table) == "entreprise_historique"
    assert table.settings(ScdSettings).on_duplicate_keys == "deduplicate"


def test_domain_checks_are_valid_for_dqx(data_context: DataContext) -> None:
    dqx = pytest.importorskip("databricks.labs.dqx.engine")
    rows = [
        {"domaine": "ACT_ECON", "code": "0100"},
        {"domaine": "ACT_ECON", "code": "0110"},
        {"domaine": "REGIM_JURI", "code": "024"},
    ]
    spark = Spark({"nprod.donnees_qc_req.domaine_valeur": Frame(["domaine", "code"], rows=rows)})
    context = service_context(data_context, spark)

    for table in data_context.tables:
        checks = build_checks(table, context).checks
        assert not dqx.DQEngine.validate_checks(checks).has_errors, table.name

    entreprise = {check["name"]: check for check in build_checks(data_context.get_table("entreprise"), context).checks}
    # DQX reads bare strings as SQL: the codes are passed as string literals, not as the numbers 100 and 110.
    arguments = entreprise["code_premiere_activite_is_not_in_domain"]["check"]["arguments"]
    assert arguments["allowed"] == ["'0100'", "'0110'"]


def test_cli_validates_the_products(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", str(PRODUCTS), "--environment", "nprod"]) == 0
    assert "+ entreprise: 37 column(s)" in capsys.readouterr().out
