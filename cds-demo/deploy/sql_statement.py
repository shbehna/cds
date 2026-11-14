"""A Pulumi resource running SQL statements on a SQL warehouse.

The Databricks provider has no resource for some Unity Catalog objects, such as the SQL
functions. A ``SqlStatement`` creates such an object with a statement (``CREATE OR
REPLACE ...``), runs it again when it changes, and drops the object with another
statement when the resource is deleted. When the name of the object changes, the
resource is replaced: the new object is created, then the previous one dropped.

A statement still running after 10 minutes, for instance on a SQL warehouse that does
not start, is canceled and fails the deployment.

The statements run with the Databricks SDK, authenticated as the provider: the host and
the client id of the service principal are inputs of the resource, and its OAuth secret
is read from the ``DATABRICKS_CLIENT_SECRET`` environment variable.
"""

from __future__ import annotations

from typing import Any

import pulumi
import pulumi.dynamic


def statement_provider() -> pulumi.dynamic.ResourceProvider:
    """Returns the provider of the ``SqlStatement`` resources.

    Pulumi serializes the provider with dill, to run it in another process. Its class is
    defined in this function so that dill serializes it by value: dill serializes a class
    of a module by reference, and the module would have to be importable in that process.
    For the same reason, the methods import what they use.
    """

    class SqlStatementProvider(pulumi.dynamic.ResourceProvider):
        # How long a statement may run before it is canceled.
        timeout_seconds = 600

        def _run(self, props: dict[str, Any], statement: str) -> None:
            import time

            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.sql import StatementState

            client = WorkspaceClient(host=props.get("host") or None, client_id=props.get("client_id") or None)
            deadline = time.monotonic() + self.timeout_seconds
            response = client.statement_execution.execute_statement(
                statement=statement, warehouse_id=props["warehouse_id"], wait_timeout="30s"
            )
            while (
                response.status
                and response.status.state in (StatementState.PENDING, StatementState.RUNNING)
                and response.statement_id
            ):
                if time.monotonic() > deadline:
                    client.statement_execution.cancel_execution(response.statement_id)
                    raise RuntimeError(
                        f"The statement did not finish within {self.timeout_seconds} seconds, it was canceled. "
                        f"Is the SQL warehouse {props['warehouse_id']} running?\n{statement}"
                    )
                time.sleep(2)
                response = client.statement_execution.get_statement(response.statement_id)
            state = response.status.state if response.status else None
            if state != StatementState.SUCCEEDED:
                error = response.status.error if response.status else None
                details = error.message if error and error.message else "no details"
                raise RuntimeError(f"The statement failed ({state}): {details}\n{statement}")

        def create(self, props: dict[str, Any]) -> pulumi.dynamic.CreateResult:
            self._run(props, props["create"])
            return pulumi.dynamic.CreateResult(id_=props["object_name"], outs=props)

        def diff(self, _id: str, _olds: dict[str, Any], _news: dict[str, Any]) -> pulumi.dynamic.DiffResult:
            inputs = ("object_name", "create", "delete", "warehouse_id", "host", "client_id")
            changed = [key for key in inputs if _olds.get(key) != _news.get(key)]
            # Another object is created first, then the previous one dropped.
            replaces = ["object_name"] if "object_name" in changed else []
            return pulumi.dynamic.DiffResult(changes=bool(changed), replaces=replaces, delete_before_replace=False)

        def update(self, _id: str, _olds: dict[str, Any], _news: dict[str, Any]) -> pulumi.dynamic.UpdateResult:
            # The same object: it is created again if its statement changed. The other
            # inputs, such as the statement dropping it, are only recorded.
            if _olds.get("create") != _news["create"]:
                self._run(_news, _news["create"])
            return pulumi.dynamic.UpdateResult(outs=_news)

        def delete(self, _id: str, _props: dict[str, Any]) -> None:
            self._run(_props, _props["delete"])

    return SqlStatementProvider()


class SqlStatement(pulumi.dynamic.Resource):
    """An object created and dropped by SQL statements, run on a SQL warehouse."""

    def __init__(
        self,
        name: str,
        *,
        warehouse_id: pulumi.Input[str],
        object_name: str,
        create: pulumi.Input[str],
        delete: pulumi.Input[str],
        host: str | None,
        client_id: str | None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        """Declares the object.

        Args:
            name: Name of the resource.
            warehouse_id: Id of the SQL warehouse running the statements.
            object_name: Full name of the object, also the id of the resource.
            create: Statement creating or replacing the object, run again when it changes.
            delete: Statement dropping the object.
            host: URL of the workspace; the default authentication of the SDK if ``None``.
            client_id: Client id of the service principal running the statements.
            opts: Options of the resource.
        """
        props = {
            "name": name,
            "warehouse_id": warehouse_id,
            "object_name": object_name,
            "create": create,
            "delete": delete,
            "host": host,
            "client_id": client_id,
        }
        super().__init__(statement_provider(), name, props, opts)
