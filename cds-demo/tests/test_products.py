"""Tests of the data products of the demo, without Spark: PySpark only builds the columns."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

import generate_testdata
import pytest
from pyspark.sql import Column, SparkSession
from pyspark.sql.connect.expressions import ColumnAlias, ColumnReference, Expression

from cds import ContractInjection, DataContext, RuntimeConfiguration, ScdSettings, ServiceContext
from cds.cli import main
from cds.services.checks import build_checks
from cds.services.scd import history_table_name
from cds.services.transformation import conform_to_contract, find_transformation

PRODUCTS = Path(__file__).resolve().parents[1] / "products"
# JSON schema of ODPS v1.1.0, which no Python package publishes (see schemas/README.md).
ODPS_SCHEMA = Path(__file__).parent / "schemas" / "odps-json-schema-v1.1.0.json"

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
    """A DataFrame keeping its columns and the PySpark columns selected from it."""

    def __init__(
        self,
        columns: Iterable[str],
        source_columns: Sequence[str] | None = None,
        selected: Iterable[Column] = (),
        rows: Iterable[dict[str, str]] = (),
    ) -> None:
        self.columns = list(columns)
        self.source_columns = list(source_columns or self.columns)
        self.selected = list(selected)
        self.rows = list(rows)

    def toDF(self, *names: str) -> Frame:
        return Frame(names)

    def select(self, *columns: str | Column) -> Frame:
        if all(isinstance(column, str) for column in columns):
            return self
        assert all(isinstance(column, Column) for column in columns), "select columns, not names"
        selected = cast("list[Column]", list(columns))
        return Frame([alias(column) for column in selected], self.columns, selected)

    def selectExpr(self, *expressions: str) -> Frame:
        # The casts of the transformation service to the contract types.
        return self

    def collect(self) -> list[dict[str, str]]:
        return self.rows


# PySpark (Spark Connect) builds columns without a session: the tests read their
# expression trees to find their names and the source columns they use.
def alias(column: Column) -> str:
    """Returns the name given to a column with ``alias``."""
    expression = cast("Any", column)._expr
    assert isinstance(expression, ColumnAlias), f"{column} has no alias"
    [name] = expression._alias
    return name


def references(column: Column) -> set[str]:
    """Returns the source columns used by a column."""
    found: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, ColumnReference):
            found.add(node._unparsed_identifier)
        elif isinstance(node, Expression):
            for value in vars(node).values():
                walk(value)
        elif isinstance(node, (list, tuple)):
            for item in node:
                walk(item)

    walk(cast("Any", column)._expr)
    return found


class Reader:
    def __init__(self) -> None:
        self.paths: list[str] = []

    def csv(self, path: str, **options: object) -> Frame:
        assert options["header"] is True and options["encoding"] == "UTF-8"
        self.paths.append(path)
        # The files of the synthetic data set have the columns of those of the register.
        columns = SOURCE_HEADERS[path.rsplit("/", 1)[1].replace("_testdata.", ".")].split(",")
        return Frame(["\ufeff" + columns[0], *columns[1:]])


class Spark:
    def __init__(self, tables: dict[str, Frame] | None = None) -> None:
        self.read = Reader()
        self._tables = tables or {}

    def table(self, name: str) -> Frame:
        return self._tables[name]


@pytest.fixture(scope="module")
def data_context() -> DataContext:
    return DataContext.discover(PRODUCTS)


def service_context(
    data_context: DataContext, spark: Spark | None = None, synthetic_data: bool = False
) -> ServiceContext:
    parameters = {"environment": "nprod", "source_path": "/Volumes/ext/req/", "synthetic_data": synthetic_data}
    return ServiceContext(
        spark=cast("SparkSession", spark or Spark()),
        environment="nprod",
        data_context=data_context,
        runtime_configuration=RuntimeConfiguration(parameters),
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
    referenced = set().union(*(references(column) for column in result.selected))
    assert referenced <= set(result.source_columns), (
        f"unknown source columns: {referenced - set(result.source_columns)}"
    )
    unused = set(result.source_columns) - referenced
    assert not unused, f"source columns not mapped: {sorted(unused)}"
    conform_to_contract(result, table)


@pytest.mark.parametrize("table_name", TABLES)
def test_the_synthetic_data_set_replaces_the_files_of_the_register(data_context: DataContext, table_name: str) -> None:
    table = data_context.get_table(table_name)
    spark = Spark()
    context = service_context(data_context, spark, synthetic_data=True)
    injection = ContractInjection(context, table=table, contract=table.contract, data_product=table.data_product)
    transformation = find_transformation(table)
    assert transformation is not None

    injection.call(transformation)

    [path] = spark.read.paths
    folder, name = path.rsplit("/", 1)
    assert folder == "/Volumes/ext/req"
    assert name in {source.file_name for source in generate_testdata.generate()}


def test_the_synthetic_files_have_the_columns_of_the_register() -> None:
    assert {name: ",".join(columns) for name, columns in generate_testdata.HEADERS.items()} == SOURCE_HEADERS


@pytest.mark.parametrize(("synthetic_data", "minimum"), [(False, 1_000_000), (True, 100)])
def test_the_row_count_threshold_depends_on_the_data_set(
    data_context: DataContext, synthetic_data: bool, minimum: int
) -> None:
    rows = [{"domaine": "ACT_ECON", "code": "0100"}, {"domaine": "REGIM_JURI", "code": "024"}]
    spark = Spark({"nprod.donnees_qc_req.domaine_valeur": Frame(["domaine", "code"], rows=rows)})
    context = service_context(data_context, spark, synthetic_data=synthetic_data)

    checks = {check["name"]: check for check in build_checks(data_context.get_table("entreprise"), context).checks}

    assert checks["row_count_minimum"]["criticality"] == "warn"
    assert checks["row_count_minimum"]["check"] == {
        "function": "is_aggr_not_less_than",
        "arguments": {"column": "*", "aggr_type": "count", "limit": minimum},
    }


@pytest.mark.parametrize("table_name", TABLES)
def test_contracts_follow_the_odcs_schema(data_context: DataContext, table_name: str) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((files("open_data_contract_standard") / "schema.json").read_text(encoding="utf-8"))
    document = data_context.get_table(table_name).contract.document
    assert document is not None

    errors = sorted(jsonschema.Draft201909Validator(schema).iter_errors(document.raw), key=str)

    assert not errors, [f"{list(error.absolute_path)}: {error.message}" for error in errors]


def test_the_data_product_follows_the_odps_schema(data_context: DataContext) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(ODPS_SCHEMA.read_text(encoding="utf-8"))
    document = data_context.get_data_product("donnees_qc.req").document
    assert document is not None

    errors = sorted(jsonschema.Draft201909Validator(schema).iter_errors(document.raw), key=str)

    assert not errors, [f"{list(error.absolute_path)}: {error.message}" for error in errors]


def test_the_odps_document_describes_the_data_product(data_context: DataContext) -> None:
    data_product = data_context.get_data_product("donnees_qc.req")
    document = data_context.get_table("entreprise").contract.document
    assert document is not None

    assert (data_product.description or "").startswith("Registre des entreprises du Québec : ")
    assert (document.domain, document.status, document.custom_properties["subdomain"]) == (
        "Économie",
        "active",
        "Registraire des entreprises du Québec",
    )
    assert [channel.channel for channel in document.support] == ["donnees-qc-req"]
    assert [prop.property for prop in document.sla_properties] == ["frequency", "latency", "errorRate", "retention"]


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
