"""Resources of the demo in a Databricks workspace.

The deployment uploads the framework wheel, the service notebooks and the data products
of the demo to a workspace folder, then creates the job running the services in order:

    schema --> metadata
           --> transformation --> quality --> scd

The job has no schedule: it runs on demand.
"""

from __future__ import annotations

import hashlib
import posixpath
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import pulumi
import pulumi_databricks as databricks
import tomllib

REPOSITORY = Path(__file__).resolve().parents[2]
FRAMEWORK = REPOSITORY / "cds-framework"
PRODUCTS = REPOSITORY / "cds-demo" / "products"

# Services run by the job, with the services they wait for.
TASKS: dict[str, list[str]] = {
    "schema": [],
    "metadata": ["schema"],
    "transformation": ["schema"],
    "quality": ["transformation"],
    "scd": ["quality"],
}
ENVIRONMENT_KEY = "cds"


@dataclass(frozen=True)
class Settings:
    """Settings of a deployment, read from the configuration of the Pulumi stack."""

    environment: str
    source_path: str
    workspace_root: str
    service_principal: str | None = None
    data_product: str = "donnees_qc.req"
    # Version 5 is the serverless environment targeted by the local environment of the
    # repository (pyproject.toml, [tool.databricks.environment]).
    environment_version: str = "5"
    dqx_version: str = "0.17.0"
    max_workers: int = 4
    wheel_directory: Path = field(default=REPOSITORY / "dist")

    @classmethod
    def from_stack(cls) -> Settings:
        config = pulumi.Config()
        environment = config.require("environment")
        return cls(
            environment=environment,
            source_path=config.require("sourcePath"),
            workspace_root=config.get("workspaceRoot") or f"/Shared/cds-demo/{environment}",
            service_principal=pulumi.Config("databricks").get("clientId"),
            environment_version=config.get("environmentVersion") or cls.environment_version,
            dqx_version=config.get("dqxVersion") or cls.dqx_version,
            max_workers=config.get_int("maxWorkers") or cls.max_workers,
        )


def framework_wheel(directory: Path) -> Path:
    """Returns the wheel of the current version of the framework.

    Raises:
        FileNotFoundError: If the wheel was not built.
    """
    project = tomllib.loads((FRAMEWORK / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = directory / f"cds_framework-{project['project']['version']}-py3-none-any.whl"
    if not wheel.is_file():
        raise FileNotFoundError(
            f"{wheel} does not exist. Build it first, from the root of the repository: "
            "uv build --package cds-framework --wheel --out-dir dist"
        )
    return wheel


def product_files() -> list[Path]:
    """Returns the files of the data products: contracts and configuration modules."""
    return [
        path
        for path in sorted(PRODUCTS.rglob("*"))
        if path.is_file() and path.suffix in (".py", ".yaml", ".yml") and "__pycache__" not in path.parts
    ]


def folders(root: str, paths: Iterable[str]) -> list[str]:
    """Returns the root and the folders under it holding the paths, parents first."""
    found = {root}
    for path in paths:
        folder = posixpath.dirname(path)
        while folder.startswith(f"{root}/"):
            found.add(folder)
            folder = posixpath.dirname(folder)
    return sorted(found, key=lambda folder: (folder.count("/"), folder))


def deploy(settings: Settings) -> databricks.Job:
    """Declares the resources of the demo and returns the job."""
    root = settings.workspace_root.rstrip("/")

    wheel = framework_wheel(settings.wheel_directory)
    # A folder named after the content of the wheel: serverless environments are cached by
    # their dependencies, so a new build must get a new path to be installed.
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()[:16]
    wheel_path = f"{root}/wheels/{digest}/{wheel.name}"
    notebook_paths = {service: f"{root}/notebooks/{service}" for service in TASKS}
    product_paths = {path: f"{root}/products/{path.relative_to(PRODUCTS).as_posix()}" for path in product_files()}

    # The folders are created first, each after its parent: the provider creates a missing
    # parent folder when it uploads a file, but not reliably when files are uploaded in
    # parallel.
    directories: dict[str, databricks.Directory] = {}
    for folder in folders(root, [wheel_path, *notebook_paths.values(), *product_paths.values()]):
        parent = directories.get(posixpath.dirname(folder))
        directories[folder] = databricks.Directory(
            f"folder:{posixpath.relpath(folder, root)}",
            path=folder,
            opts=pulumi.ResourceOptions(depends_on=[parent] if parent else None),
        )

    def in_folder(path: str) -> pulumi.ResourceOptions:
        return pulumi.ResourceOptions(depends_on=[directories[posixpath.dirname(path)]])

    wheel_file = databricks.WorkspaceFile(
        "framework-wheel", path=wheel_path, source=str(wheel), opts=in_folder(wheel_path)
    )

    notebooks = {
        service: databricks.Notebook(
            f"notebook-{service}",
            path=path,
            source=str(FRAMEWORK / "notebooks" / f"{service}.py"),
            language="PYTHON",
            opts=in_folder(path),
        )
        for service, path in notebook_paths.items()
    }

    products = [
        databricks.WorkspaceFile(
            f"products/{source.relative_to(PRODUCTS).as_posix()}",
            path=path,
            source=str(source),
            opts=in_folder(path),
        )
        for source, path in product_paths.items()
    ]

    parameters = {
        "environment": settings.environment,
        "roots": f"/Workspace{root}/products",
        "data_product": settings.data_product,
        "source_path": settings.source_path,
        "max_workers": str(settings.max_workers),
    }
    job = databricks.Job(
        "pipeline",
        name=f"cds-demo-req-{settings.environment}",
        description=(
            "CDS demo: builds the donnees_qc.req data product (Registraire des entreprises du Québec) "
            "from its data contracts."
        ),
        parameters=[databricks.JobParameterArgs(name=name, default=value) for name, value in parameters.items()],
        environments=[
            databricks.JobEnvironmentArgs(
                environment_key=ENVIRONMENT_KEY,
                spec=databricks.JobEnvironmentSpecArgs(
                    environment_version=settings.environment_version,
                    dependencies=[
                        wheel_file.path.apply(lambda path: f"/Workspace{path}"),
                        f"databricks-labs-dqx=={settings.dqx_version}",
                    ],
                ),
            )
        ],
        tasks=[
            databricks.JobTaskArgs(
                task_key=service,
                description=f"CDS {service} service",
                depends_ons=[databricks.JobTaskDependsOnArgs(task_key=upstream) for upstream in upstreams],
                environment_key=ENVIRONMENT_KEY,
                notebook_task=databricks.JobTaskNotebookTaskArgs(
                    notebook_path=notebooks[service].path, source="WORKSPACE"
                ),
                max_retries=0,
                timeout_seconds=2 * 3600,
            )
            for service, upstreams in TASKS.items()
        ],
        max_concurrent_runs=1,
        run_as=(
            databricks.JobRunAsArgs(service_principal_name=settings.service_principal)
            if settings.service_principal
            else None
        ),
        tags={"project": "cds-demo", "environment": settings.environment, "data_product": settings.data_product},
        opts=pulumi.ResourceOptions(depends_on=products),
    )
    pulumi.export("job_id", job.id)
    pulumi.export("job_url", job.url)
    pulumi.export("workspace_root", root)
    return job
