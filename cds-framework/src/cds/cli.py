"""Command line interface: validates projects without Spark.

Usage::

    cds validate <root> [<root> ...]
    cds checks <root> [<root> ...] --data-product <group>.<data_product> [--environment <id>]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from typing import Any

from cds.context import ServiceContext
from cds.data_context import DataContext, Table
from cds.errors import CdsError
from cds.services.checks import CheckSet, build_checks, configured_checks


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

    args = parser.parse_args(argv)
    try:
        context = ServiceContext(
            spark=None, environment=args.environment, data_context=DataContext.discover(args.roots)
        )
        if args.command == "validate":
            return _validate(context)
        return _print_checks(context, args.data_product)
    except CdsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def offline_checks(table: Table, context: ServiceContext) -> tuple[CheckSet, bool]:
    """Returns the checks of a table that can be built without Spark.

    Returns:
        The checks, and whether the configuration also defines checks computed at run time.
    """
    check_set = build_checks(table, context, include_configured=False)
    dynamic = callable(getattr(table.contract.config, "quality_checks", None))
    if not dynamic:
        check_set.checks.extend(configured_checks(table, context))
    return check_set, dynamic


def _validate(context: ServiceContext) -> int:
    validator = _dqx_validator()
    failed = False
    for data_product in context.data_context.data_products:
        print(f"{data_product.qualified_name}: {len(data_product.contracts)} contract(s)")
        for table in data_product.tables:
            try:
                check_set, dynamic = offline_checks(table, context)
                problems = validator(check_set.checks) if validator else []
            except CdsError as exc:
                failed = True
                print(f"  x {table.name}: {exc}")
                continue
            details = f"{len(table.columns)} column(s), key ({', '.join(table.primary_key) or 'none'}), "
            details += f"{len(check_set.checks)} check(s)"
            if dynamic:
                details += " + checks computed at run time"
            for note in check_set.unsupported:
                details += f"\n      not checked: {note}"
            if problems:
                failed = True
                print(f"  x {table.name}: invalid checks: {'; '.join(problems)}")
            else:
                print(f"  + {table.name}: {details}")
    if validator is None:
        print("DQX is not installed: the checks were not validated (install cds-framework[dqx]).")
    return 1 if failed else 0


def _print_checks(context: ServiceContext, reference: str) -> int:
    data_product = context.get_data_product(reference)
    result: dict[str, Any] = {}
    for table in data_product.tables:
        check_set, dynamic = offline_checks(table, context)
        result[table.name] = {
            "checks": check_set.checks,
            "not_checked": check_set.unsupported,
            "run_time_checks": dynamic,
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
