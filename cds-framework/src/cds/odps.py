"""Object model of Open Data Product Standard (ODPS) documents.

The ODPS document of a data product, ``<group>/<data_product>/<data_product>.odps.yaml``,
describes the product itself: its identity, status, business domain, description and
support channels. The fields its contracts also have are written once in it, and copied
into each contract of the data product (``contract_fields``). Each document keeps its
source mapping in ``raw``, so any other part of it, such as its ports, remains readable.

Reference: https://bitol-io.github.io/open-data-product-standard/latest/
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from cds.errors import ContractError
from cds.odcs import SupportChannel, _Parser

KIND = "DataProduct"
API_VERSIONS = ("v1.0.0", "v1.1.0")
# Fields of the description, as text.
DESCRIPTION_FIELDS = ("purpose", "usage", "limitations")
# Fields of the description copied into the contracts: the purpose of each contract is its own.
CONTRACT_DESCRIPTION_FIELDS = ("usage", "limitations")
# Fields of a support channel in ODCS: those of ODPS only, such as its tags, are not copied.
CONTRACT_SUPPORT_FIELDS = ("id", "channel", "url", "description", "tool", "scope", "invitationUrl", "customProperties")


@dataclass(frozen=True)
class DataProductDocument:
    """An ODPS data product document.

    Attributes:
        id: Identifier of the data product: ``<group>.<data_product>`` in a project.
        description: Its ``purpose``, ``usage`` and ``limitations``, among others.
        custom_properties: Values of its ``customProperties``, by property name.
    """

    id: str
    api_version: str
    name: str | None = None
    version: str | None = None
    status: str | None = None
    type: str | None = None
    domain: str | None = None
    tenant: str | None = None
    description: Mapping[str, Any] = field(default_factory=dict)
    tags: tuple[str, ...] = ()
    custom_properties: Mapping[str, Any] = field(default_factory=dict)
    support: tuple[SupportChannel, ...] = ()
    source: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @property
    def purpose(self) -> str | None:
        """The purpose of the data product, from its description."""
        purpose = self.description.get("purpose")
        return purpose if isinstance(purpose, str) else None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any], source: str | Path | None = None) -> DataProductDocument:
        """Builds a document from its mapping representation.

        Raises:
            ContractError: If the mapping is not a valid ODPS document.
        """
        return _ProductParser(str(source) if source is not None else "<data product>").product(data)

    @classmethod
    def from_file(cls, path: str | Path) -> DataProductDocument:
        """Reads a document from a YAML file.

        Raises:
            ContractError: If the file cannot be read or is not a valid ODPS document.
        """
        path = Path(path)
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise ContractError(f"Cannot read the data product document {path}: {exc}") from exc
        return cls.from_dict(document, source=path)


def contract_fields(document: DataProductDocument) -> dict[str, Any]:
    """Returns the fields of a data product document that its contracts also have, as a partial ODCS document.

    They are merged under every contract of the data product:

    - ``id``, as the ``dataProduct`` of the contracts;
    - ``domain``, ``tenant`` and ``status``;
    - ``customProperties``, the list as written, and ``support``, with the fields of its
      channels that ODCS also has (``CONTRACT_SUPPORT_FIELDS``);
    - the ``usage`` and ``limitations`` of the ``description``.

    The ``purpose`` is not copied: each contract has its own. Neither are the ``tags``: a
    list replaces that of the previous levels, so they would replace the tags of the group.
    Empty lists and missing values are not copied either.
    """
    fields: dict[str, Any] = {"dataProduct": document.id}
    for key, value in (("domain", document.domain), ("tenant", document.tenant), ("status", document.status)):
        if value is not None:
            fields[key] = value
    if document.custom_properties:
        fields["customProperties"] = copy.deepcopy(document.raw["customProperties"])
    if document.support:
        fields["support"] = [
            {key: copy.deepcopy(channel.raw[key]) for key in CONTRACT_SUPPORT_FIELDS if key in channel.raw}
            for channel in document.support
        ]
    description = {key: document.description[key] for key in CONTRACT_DESCRIPTION_FIELDS if key in document.description}
    if description:
        fields["description"] = description
    return fields


class _ProductParser(_Parser):
    """Converts the mapping of a data product document into the object model, with precise errors."""

    def product(self, data: object) -> DataProductDocument:
        if not isinstance(data, Mapping):
            raise self._error("", "a data product document must be a mapping")
        kind = data.get("kind")
        if kind != KIND:
            raise self._error("kind", f"expected {KIND!r}, got {kind!r}")
        api_version = self._required_text(data, "apiVersion", "")
        if api_version not in API_VERSIONS:
            raise self._error(
                "apiVersion", f"unsupported version {api_version!r}, CDS reads ODPS {', '.join(API_VERSIONS)}"
            )
        description = dict(self._mapping(data.get("description") or {}, "description"))
        for key in DESCRIPTION_FIELDS:
            text = self._text(description, key, "description")
            if text is None:
                description.pop(key, None)
            else:
                description[key] = text
        status = self._text(data, "status", "")
        if status is None and api_version == "v1.0.0":
            raise self._error("status", "is required by ODPS v1.0.0")
        return DataProductDocument(
            id=self._required_text(data, "id", ""),
            api_version=api_version,
            name=self._text(data, "name", ""),
            version=self._text(data, "version", ""),
            status=status,
            type=self._text(data, "type", ""),
            domain=self._text(data, "domain", ""),
            tenant=self._text(data, "tenant", ""),
            description=description,
            tags=self._tags(data, ""),
            custom_properties=self._custom_properties(data, ""),
            support=tuple(
                self.support_channel(item, f"support[{index}]")
                for index, item in enumerate(self._list(data, "support", ""))
            ),
            source=self.source,
            raw=data,
        )
