"""Tests of the data products of the demo, without Spark: PySpark only builds the columns.

The tests read what they expect from the contracts and their configurations: changing a
contract does not need changing the tests.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import generate_testdata
import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.connect.expressions import ColumnAlias, ColumnReference, Expression

from cds import ContractInjection, DataContext, QualitySettings, RuntimeConfiguration, ServiceContext, Table
from cds.cli import main
from cds.config import resolved
from cds.odcs import DataContract
from cds.services.checks import ALLOWED_VALUES, build_checks
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
    # The files of the directory of the municipalities. The seats of the councils are
    # numbered con1 to con75 in MUN.csv, con01 to con75 in ARR.csv.
    "MUN.csv": ",".join(
        (
            "mcode,munnom,madr1,madr2,madr3,madr4,mcodpos,mcourriel,mweb,mtel,mfax,mcodedesi,mdes,mgentile,regadm,"
            "divrec,mrc,admregionale,horsmrcautoch,mdatcons,mdatregi,msuperf,mpopul,datelec,electype,delectype,"
            "elecmode,delecmode,divter,mcm,mcirc,msupft,maire",
            *(f"con{seat}" for seat in range(1, 76)),
            "dirgen,dirsecpub,tres,gref,sectres,polic,incen,loisir,trvpub,mesurg,urban,communic,permis,batim,nd",
        )
    ),
    "MRC_CM_Arg.csv": (
        "mrccod,mrcnom,mrcadr1,mrcadr2,mrcadr3,mrcadr4,mrccodp,mrctel,mrcfax,mrccourriel,mrcweb,mrcdateconsti,"
        "mrcpopul,mrcregion,mrccodedesi,mrcdescdesi,msupf,msupft,president,prefet,tres,sectres,amenage,urbanisme"
    ),
    "ARR.csv": ",".join(
        (
            "arrcod,arrnom,arrville,arrcode,arradr1,arradr2,arradr3,arradr4,arrcodp,arrtel,arrfax,arrcourriel,"
            "arrregion,arrmrc,arrhorsmrcautoch,arrcm,arrgentile,arradrinternet,arrdateconsti,arrdateproelec,"
            "president,directeur",
            *(f"con{seat:02d}" for seat in range(1, 76)),
        )
    ),
    "ress.csv": "rescode,resgroupe,rescodetitre,restitre,resentreefonction,resnom,resprenom,resestvacant",
}
# The source columns no table reads, by file: those the publisher does not use.
UNUSED_SOURCE_COLUMNS: dict[str, set[str]] = {"MUN.csv": {"nd"}}
TABLES = sorted(table.name for table in DataContext.discover(PRODUCTS).tables)
# The codes the fake Spark session returns for every query of allowed values.
CODES = ["0110", "0100"]
# A query of the allowed values of a column (allowedValues custom property of the contracts).
ALLOWED_VALUES_QUERY = re.compile(
    r"SELECT DISTINCT `(?P<column>\w+)` AS value FROM `nprod`\.`\w+`\.`\w+` "
    r"WHERE `(?P=column)` IS NOT NULL( AND \(.+\))? LIMIT \d+"
)


class Frame:
    """A DataFrame keeping its columns and the source columns each of them is computed from.

    A frame read from a file has the columns of the file, each computed from itself. A
    selection, or a filter, may only use the columns of its frame: a struct column by its
    fields (``siege.nom``).
    """

    def __init__(
        self,
        columns: Iterable[str],
        lineage: Mapping[str, set[str]] | None = None,
        filtered_by: Iterable[str] = (),
        rows: Iterable[dict[str, str]] = (),
    ) -> None:
        self.columns = list(columns)
        self.lineage = dict(lineage) if lineage is not None else {name: {name} for name in self.columns}
        # The source columns used by the filters of the frame and of the frames it comes from.
        self.filtered_by = set(filtered_by)
        self.rows = list(rows)

    def sources(self, column: Column) -> set[str]:
        """Returns the source columns a column is computed from."""
        found: set[str] = set()
        for name in references(column):
            base = name.split(".", 1)[0]
            assert base in self.lineage, f"unknown column {name}, not in {self.columns}"
            found |= self.lineage[base]
        return found

    @property
    def source_columns(self) -> set[str]:
        """The source columns used by the frame: its columns and its filters."""
        return set().union(self.filtered_by, *self.lineage.values())

    def toDF(self, *names: str) -> Frame:
        return Frame(names)

    def select(self, *columns: str | Column) -> Frame:
        if all(isinstance(column, str) for column in columns):
            return self
        assert all(isinstance(column, Column) for column in columns), "select columns, not names"
        selected = cast("list[Column]", list(columns))
        return Frame(
            [alias(column) for column in selected],
            {alias(column): self.sources(column) for column in selected},
            self.filtered_by,
        )

    def where(self, condition: Column) -> Frame:
        return Frame(self.columns, self.lineage, self.filtered_by | self.sources(condition))

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
        # The files of the synthetic data set have the columns of the published ones.
        columns = SOURCE_HEADERS[source_name(path)].split(",")
        # The files start with a byte order mark.
        return Frame(["\ufeff" + columns[0], *columns[1:]])


def source_name(path: str) -> str:
    """The name of the published file read from a path, or of the file of a synthetic file."""
    return path.rsplit("/", 1)[1].replace(f"{generate_testdata.SUFFIX}.", ".")


class Spark:
    def __init__(self) -> None:
        self.read = Reader()
        self.statements: list[str] = []

    def sql(self, statement: str) -> Frame | Tables:
        """Lists the tables, or returns the codes of a query of allowed values."""
        self.statements.append(statement)
        if statement.startswith("SHOW TABLES IN "):
            return Tables(TABLES)
        assert ALLOWED_VALUES_QUERY.fullmatch(statement), f"unexpected query: {statement}"
        return Frame(["value"], rows=[{"value": code} for code in CODES])


class Tables:
    """The result of SHOW TABLES."""

    def __init__(self, names: Iterable[str]) -> None:
        self.names = list(names)

    def collect(self) -> list[SimpleNamespace]:
        return [SimpleNamespace(tableName=name) for name in self.names]


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
    context = service_context(data_context)

    assert data_context.data_products
    for data_product in data_context.data_products:
        names = [context.full_table_name(table) for table in data_product.tables]
        assert len(set(names)) == len(names)
        assert all(name.startswith(f"nprod.donnees_qc_{data_product.name}.") for name in names)
    names = [context.full_table_name(table) for table in data_context.tables]
    assert len(set(names)) == len(names) == len(TABLES)


def transform(data_context: DataContext, table: Table, synthetic_data: bool = False) -> tuple[Frame, str]:
    """Runs the transformation of a table: returns its result and the path of the file it reads."""
    spark = Spark()
    context = service_context(data_context, spark, synthetic_data)
    injection = ContractInjection(context, table=table, contract=table.contract, data_product=table.data_product)
    transformation = find_transformation(table)
    assert transformation is not None

    result = injection.call(transformation)

    [path] = spark.read.paths
    return result, path


@pytest.mark.parametrize("table_name", TABLES)
def test_transformations_produce_the_contract_columns(data_context: DataContext, table_name: str) -> None:
    table = data_context.get_table(table_name)

    result, path = transform(data_context, table)

    assert path.startswith("/Volumes/ext/req/") and "//" not in path.removeprefix("/")
    conform_to_contract(cast("DataFrame", result), table)


def test_the_transformations_use_every_source_column(data_context: DataContext) -> None:
    """Each column of a file is read by a table, which may share the file with others."""
    used: dict[str, set[str]] = {}
    for table in data_context.tables:
        result, path = transform(data_context, table)
        used.setdefault(source_name(path), set()).update(result.source_columns)

    assert used.keys() == SOURCE_HEADERS.keys()
    for name, header in SOURCE_HEADERS.items():
        unused = set(header.split(",")) - used[name] - UNUSED_SOURCE_COLUMNS.get(name, set())
        assert not unused, f"{name}: source columns not mapped: {sorted(unused)}"
        assert not used[name] & UNUSED_SOURCE_COLUMNS.get(name, set()), name


@pytest.mark.parametrize("table_name", TABLES)
def test_the_synthetic_data_set_replaces_the_published_files(data_context: DataContext, table_name: str) -> None:
    table = data_context.get_table(table_name)

    _, path = transform(data_context, table, synthetic_data=True)

    folder, name = path.rsplit("/", 1)
    assert folder == "/Volumes/ext/req"
    assert name.endswith(f"{generate_testdata.SUFFIX}.csv")
    # The data products of the synthetic data set have a synthetic file for each table.
    if table.data_product.qualified_name in generate_testdata.DATA_PRODUCTS:
        assert name in {source.file_name for source in generate_testdata.generate()}


def test_the_data_products_of_the_synthetic_data_set_exist(data_context: DataContext) -> None:
    names = {data_product.qualified_name for data_product in data_context.data_products}

    assert set(generate_testdata.DATA_PRODUCTS) <= names


def test_the_synthetic_files_have_the_columns_of_the_published_files() -> None:
    # The published files: the transformations read their columns.
    assert {name: ",".join(columns) for name, columns in generate_testdata.HEADERS.items()} == {
        name: SOURCE_HEADERS[name] for name in generate_testdata.HEADERS
    }


def checks_by_name(table: Table, context: ServiceContext) -> dict[str, dict[str, Any]]:
    return {check["name"]: check for check in build_checks(table, context).checks}


@pytest.mark.parametrize("table_name", TABLES)
def test_the_row_count_threshold_depends_on_the_data_set(data_context: DataContext, table_name: str) -> None:
    table = data_context.get_table(table_name)
    register = checks_by_name(table, service_context(data_context, synthetic_data=False))
    synthetic = checks_by_name(table, service_context(data_context, synthetic_data=True))

    # Same checks, whatever the data set; only the thresholds of the row counts differ.
    assert register.keys() == synthetic.keys()
    for name, check in register.items():
        if check["check"]["function"] != "is_aggr_not_less_than":
            continue
        limits = (check["check"]["arguments"]["limit"], synthetic[name]["check"]["arguments"]["limit"])
        assert limits[1] <= generate_testdata.ROWS < limits[0], f"{name}: {limits}"


@pytest.mark.parametrize("table_name", TABLES)
def test_contracts_follow_the_odcs_schema(data_context: DataContext, table_name: str) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads((files("open_data_contract_standard") / "schema.json").read_text(encoding="utf-8"))
    document = data_context.get_table(table_name).contract.document
    assert document is not None

    errors = sorted(jsonschema.Draft201909Validator(schema).iter_errors(document.raw), key=str)

    assert not errors, [f"{list(error.absolute_path)}: {error.message}" for error in errors]


def test_the_data_products_follow_the_odps_schema(data_context: DataContext) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(ODPS_SCHEMA.read_text(encoding="utf-8"))

    for data_product in data_context.data_products:
        document = data_product.document
        assert document is not None
        errors = sorted(jsonschema.Draft201909Validator(schema).iter_errors(document.raw), key=str)
        assert not errors, [f"{list(error.absolute_path)}: {error.message}" for error in errors]
        assert document.id == data_product.qualified_name


def inherited(defaults: Mapping[str, Any], own: Mapping[str, Any], merged: Mapping[str, Any]) -> list[str]:
    """The defaults a merged document lacks: those its own document does not set, recursively in mappings."""
    missing = []
    for key, value in defaults.items():
        if key not in own:
            if merged.get(key) != value:
                missing.append(key)
        elif isinstance(value, Mapping) and isinstance(own[key], Mapping):
            missing.extend(f"{key}.{name}" for name in inherited(value, own[key], merged.get(key, {})))
    return missing


def test_the_contracts_inherit_the_defaults_they_do_not_set(data_context: DataContext) -> None:
    """The defaults of the group, the ODPS document and the partial contract of the data product."""
    for contract in data_context.contracts:
        document = contract.document
        assert document is not None
        own = DataContract.from_file(contract.document_path).raw

        assert inherited(contract.data_product.contract_defaults, own, document.raw) == [], contract.name
        assert document.data_product == contract.data_product.qualified_name


def test_the_history_tables_do_not_replace_tables(data_context: DataContext) -> None:
    names = {table.name for table in data_context.tables}
    histories = [history_table_name(table) for table in data_context.tables if table.primary_key]

    assert len(set(histories)) == len(histories)
    assert not names & set(histories)


def test_the_codes_are_checked_against_their_value_domain(data_context: DataContext) -> None:
    dqx = pytest.importorskip("databricks.labs.dqx.engine")
    context = service_context(data_context)

    for table in data_context.tables:
        checks = checks_by_name(table, context)
        assert not dqx.DQEngine.validate_checks(list(checks.values())).has_errors, table.name
        # The checks inferred from the contracts are errors, unless the settings say otherwise.
        criticality = (resolved(table.settings(QualitySettings).criticality) or {}).get("allowed_values", "error")
        for prop in table.columns:
            if ALLOWED_VALUES not in prop.custom_properties:
                continue
            check = checks[f"{prop.column}_is_not_allowed"]
            assert check["criticality"] == criticality, prop.column
            # The values of a column of another table, read at run time. DQX reads bare
            # strings as SQL: the codes read are passed as string literals, sorted.
            assert check["check"]["arguments"]["allowed"] == [f"'{code}'" for code in sorted(CODES)], prop.column


def test_cli_validates_the_products(data_context: DataContext, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", str(PRODUCTS), "--environment", "nprod"]) == 0
    output = capsys.readouterr().out
    for table in data_context.tables:
        assert f"+ {table.name}: {len(table.columns)} column(s)" in output
