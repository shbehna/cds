"""Command line interface: validates projects without Spark.

Usage::

    cds validate <root> [<root> ...]
    cds checks <root> [<root> ...] --data-product <group>.<data_product> [--environment <id>]
    cds genie <root> [<root> ...] --data-product <group>.<data_product> [--environment <id>]
    cds dashboard <root> [<root> ...] --data-product <group>.<data_product> [--environment <id>]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict
from typing import Any

from cds.config import DashboardSettings, GenieSettings
from cds.context import ServiceContext
from cds.dashboard import build_dashboard
from cds.data_context import DataContext, DataProduct, Table
from cds.errors import CdsError
from cds.genie import build_space, run_time_examples
from cds.services.checks import CheckSet, build_checks, checked_tables, configured_checks
from cds.services.summary import summary_table
from cds.services.tags import discover_domains, domain_environments, tag_problems
from cds.sla import sla_problems


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cds", description="CDS (Contract Driven Services) tools.")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser(
        "validate", help="Load the data products of root folders and validate their contracts and checks."
    )
    validate.add_argument("roots", nargs="+", help="Root folders of the data products.")
    validate.add_argument("--environment", default="validation", help="Environment id used to resolve names.")

    checks = commands.add_parser("checks", help="Print the DQX checks of the tables of a data product.")
    checks.add_argument("roots", nargs="+", help="Root folders of the data products.")
    checks.add_argument("--data-product", required=True, help="<group>.<data_product>")
    checks.add_argument("--environment", default="validation", help="Environment id used to resolve names.")

    genie = commands.add_parser("genie", help="Print the Genie space of a data product.")
    genie.add_argument("roots", nargs="+", help="Root folders of the data products.")
    genie.add_argument("--data-product", required=True, help="<group>.<data_product>")
    genie.add_argument("--environment", default="validation", help="Environment id used to resolve names.")

    dashboard = commands.add_parser("dashboard", help="Print the quality dashboard of a data product.")
    dashboard.add_argument("roots", nargs="+", help="Root folders of the data products.")
    dashboard.add_argument("--data-product", required=True, help="<group>.<data_product>")
    dashboard.add_argument("--environment", default="validation", help="Environment id used to resolve names.")

    args = parser.parse_args(argv)
    try:
        context = ServiceContext(
            spark=None, environment=args.environment, data_context=DataContext.discover(args.roots)
        )
        if args.command == "validate":
            return _validate(context)
        if args.command == "genie":
            return _print_genie(context, args.data_product)
        if args.command == "dashboard":
            return _print_dashboard(context, args.data_product)
        return _print_checks(context, args.data_product)
    except CdsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def offline_checks(table: Table, context: ServiceContext) -> tuple[CheckSet, bool]:
    """Returns the checks of a table that can be built without Spark.

    Returns:
        The checks, and whether checks are computed at run time: by the ``quality_checks``
        function of the configuration, or from the ``allowedValues`` of the contract
        (described in ``CheckSet.run_time``).
    """
    check_set = build_checks(table, context, include_configured=False, read_values=False)
    configured = callable(getattr(table.contract.config, "quality_checks", None))
    if not configured:
        check_set.checks.extend(configured_checks(table, context))
    return check_set, configured or bool(check_set.run_time)


def _validate(context: ServiceContext) -> int:
    validator = _dqx_validator()
    failed = False
    for data_product in context.data_context.data_products:
        print(f"{data_product.qualified_name}: {len(data_product.contracts)} contract(s)")
        for table in data_product.tables:
            try:
                check_set, run_time = offline_checks(table, context)
                problems = validator(check_set.checks) if validator else []
                summary_table(table, context)
            except CdsError as exc:
                failed = True
                print(f"  x {table.name}: {exc}")
                continue
            details = f"{len(table.columns)} column(s), key ({', '.join(table.primary_key) or 'none'}), "
            details += f"{len(check_set.checks)} check(s)"
            if run_time:
                details += " + checks computed at run time"
            for note in check_set.run_time:
                details += f"\n      computed at run time: {note}"
            for note in check_set.unsupported:
                details += f"\n      not checked: {note}"
            tags = tag_problems(table)
            if problems:
                failed = True
                print(f"  x {table.name}: invalid checks: {'; '.join(problems)}")
            if tags:
                failed = True
                print(f"  x {table.name}: invalid tags: {'; '.join(tags)}")
            if not problems and not tags:
                print(f"  + {table.name}: {details}")
        for contract in data_product.contracts:
            sla = sla_problems(contract.document) if contract.document else []
            if sla:
                failed = True
                print(f"  x contract {contract.name}: invalid SLA: {'; '.join(sla)}")
        if not _validate_genie(data_product, context):
            failed = True
        if not _validate_dashboard(data_product, context):
            failed = True
    if validator is None:
        print("DQX is not installed: the checks were not validated (install cds-framework[dqx]).")
    return 1 if failed else 0


def _validate_genie(data_product: DataProduct, context: ServiceContext) -> bool:
    """Builds the Genie space of a data product that has one; returns whether it is valid."""
    if not data_product.settings(GenieSettings).enabled:
        return True
    try:
        space = build_space(data_product, context)["serialized_space"]
        run_time = run_time_examples(data_product, context)
        domains = discover_domains(data_product.tables)
    except CdsError as exc:
        print(f"  x Genie space: {exc}")
        return False
    instructions = space["instructions"]
    details = (
        f"{len(space['data_sources']['tables'])} table(s), {len(instructions['join_specs'])} join(s), "
        f"{len(instructions['example_question_sqls'])} example(s)"
    )
    if run_time:
        details += f" + {len(run_time)} example(s) computed at run time"
    print(f"  + Genie space: {details}{_in_domains(data_product.tables, domains)}")
    return True


def _validate_dashboard(data_product: DataProduct, context: ServiceContext) -> bool:
    """Builds the quality dashboard of a data product that has one; returns whether it is valid."""
    if not data_product.settings(DashboardSettings).enabled:
        return True
    try:
        tables = checked_tables(data_product, context)
        dashboard = build_dashboard(data_product, context, tables)["serialized_dashboard"]
        domains = discover_domains(tables)
    except CdsError as exc:
        print(f"  x Dashboard: {exc}")
        return False
    pages = len(dashboard["pages"])
    print(f"  + Dashboard: {len(tables)} table(s), {pages} page(s){_in_domains(tables, domains)}")
    return True


def _in_domains(tables: Sequence[Table], domains: Sequence[str]) -> str:
    """The Discover domains a workspace object of tables joins, and in which environments."""
    if not domains:
        return ""
    environments = domain_environments(tables)
    where = f" in {', '.join(environments)}" if environments is not None else ""
    return f", in the Discover domain(s) {', '.join(domains)}{where}"


def _print_dashboard(context: ServiceContext, reference: str) -> int:
    data_product = context.get_data_product(reference)
    if not data_product.settings(DashboardSettings).enabled:
        print(
            f"error: {data_product.qualified_name} has no dashboard: enable it with DashboardSettings(enabled=True).",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(build_dashboard(data_product, context), indent=2))
    return 0


def _print_genie(context: ServiceContext, reference: str) -> int:
    data_product = context.get_data_product(reference)
    if not data_product.settings(GenieSettings).enabled:
        print(
            f"error: {data_product.qualified_name} has no Genie space: enable it with GenieSettings(enabled=True).",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(build_space(data_product, context), indent=2))
    for question in run_time_examples(data_product, context):
        print(f"not included, computed at run time: example {question!r}", file=sys.stderr)
    return 0


def _print_checks(context: ServiceContext, reference: str) -> int:
    data_product = context.get_data_product(reference)
    result: dict[str, Any] = {}
    for table in data_product.tables:
        check_set, run_time = offline_checks(table, context)
        result[table.name] = {
            "checks": check_set.checks,
            "thresholds": {
                name: [asdict(threshold) for threshold in thresholds]
                for name, thresholds in check_set.thresholds.items()
            },
            "not_checked": check_set.unsupported,
            "run_time_checks": run_time,
            "computed_at_run_time": check_set.run_time,
        }
    print(json.dumps(result, indent=2, default=str))
    return 0


def _dqx_validator() -> Callable[[list[dict[str, Any]]], list[str]] | None:
    try:
        from databricks.labs.dqx.engine import DQEngine
    except ImportError:
        return None
    return lambda checks: list(DQEngine.validate_checks(checks).errors)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
