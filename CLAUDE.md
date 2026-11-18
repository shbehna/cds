# Description

The goal of this project is to provide a data engineering framework, based on the [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/latest/).

## Language

Code and technical documentation (comments, docstrings, READMEs) are in English. The business domain of the REQ demo is in French: the names of its tables and columns, and the documentation of its data contracts and data product document (descriptions, labels, usage).

## Typing

All Python code is type-annotated, tests included. Strict mypy and the `ANN` rules of ruff check it.

## Repository

- `cds-framework/`: the framework (package `cds` in `src/cds`), its unit tests and its service notebooks. Unit tests use mocks: no Spark.
- `cds-demo/`: the demo data product (`products/`) and its tests. `cds-demo/deploy/` is the Pulumi deployment, a separate uv project excluded from the workspace.

## Commands

- `uv sync --all-packages`: installs the workspace.
- `uv run pytest`: tests of the framework and the demo.
- `uv run ruff check .` and `uv run ruff format --check .`: lint.
- `uv run mypy`: type checks of the framework and the demo (strict).
- `uv run cds validate cds-demo/products`: validates the contracts and configurations of the demo.
- `uv run mypy` and `uv run pytest` in `cds-demo/deploy`: type checks and tests of the Pulumi program.

The `[tool.uv] constraint-dependencies` block of the root `pyproject.toml` is managed by `databricks environments setup-local`: do not edit it.
