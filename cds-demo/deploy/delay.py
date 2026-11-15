"""A Pulumi resource waiting for a while when it is created.

Some changes take time to propagate. Unity Catalog, for instance, does not know a new
governed tag right away, and refuses the policies using it until then ("Unknown tag
policy key"). A ``Delay`` declared after such a change, and before the resources
depending on it, gives the change time to propagate. It waits when it is created, and
again when it is replaced, which happens when its trigger changes.
"""

from __future__ import annotations

from typing import Any

import pulumi
import pulumi.dynamic


def delay_provider() -> pulumi.dynamic.ResourceProvider:
    """Returns the provider of the ``Delay`` resources.

    Pulumi serializes the provider with dill, to run it in another process. Its class is
    defined in this function so that dill serializes it by value, as for the provider of
    ``sql_statement``: the module would otherwise have to be importable in that process.
    """

    class DelayProvider(pulumi.dynamic.ResourceProvider):
        def create(self, props: dict[str, Any]) -> pulumi.dynamic.CreateResult:
            import time

            time.sleep(float(props["seconds"]))
            return pulumi.dynamic.CreateResult(id_=props["name"], outs=props)

        def diff(self, _id: str, _olds: dict[str, Any], _news: dict[str, Any]) -> pulumi.dynamic.DiffResult:
            # A new trigger waits again; another duration only applies to the next wait.
            replaces = ["trigger"] if _olds.get("trigger") != _news.get("trigger") else []
            changes = bool(replaces) or _olds.get("seconds") != _news.get("seconds")
            return pulumi.dynamic.DiffResult(changes=changes, replaces=replaces, delete_before_replace=False)

        def update(self, _id: str, _olds: dict[str, Any], _news: dict[str, Any]) -> pulumi.dynamic.UpdateResult:
            return pulumi.dynamic.UpdateResult(outs=_news)

        def delete(self, _id: str, _props: dict[str, Any]) -> None:
            return None

    return DelayProvider()


class Delay(pulumi.dynamic.Resource):
    """A wait, when the resource is created or its trigger changes."""

    def __init__(
        self,
        name: str,
        *,
        seconds: int,
        trigger: pulumi.Input[str],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        """Declares the wait.

        Args:
            name: Name of the resource, also its id.
            seconds: Duration of the wait.
            trigger: A value whose change waits again, such as the id of the resource to wait for.
            opts: Options of the resource, such as ``depends_on`` the resource to wait for.
        """
        super().__init__(delay_provider(), name, {"name": name, "seconds": seconds, "trigger": trigger}, opts)
