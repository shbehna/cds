"""Loading of the Python configuration files of a project layout."""

from __future__ import annotations

import hashlib
import importlib
import importlib.machinery
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from cds.config import _LevelConfig
from cds.errors import ConfigurationError

CONFIG_SUFFIX = "_config.py"
CONFIG_ATTRIBUTE = "config"


class ModuleLoader:
    """Imports the Python files of a root folder as modules of a private package.

    The root folder becomes a package named ``_cds_<hash>``, and its sub-folders
    namespace packages. Configuration files can therefore share code with relative
    imports (``from ..helpers import read_source``) without anything being added to
    ``sys.path``.
    """

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        digest = hashlib.sha1(str(self.root).encode("utf-8")).hexdigest()[:12]
        self.package = f"_cds_{digest}"
        self._register_package()

    def _register_package(self) -> None:
        # Start from a fresh package, so that files edited since a previous discovery in
        # the same interpreter (e.g. an interactive notebook) are imported again.
        prefix = f"{self.package}."
        for name in [name for name in sys.modules if name == self.package or name.startswith(prefix)]:
            del sys.modules[name]
        spec = importlib.machinery.ModuleSpec(self.package, None, is_package=True)
        spec.submodule_search_locations = [str(self.root)]
        sys.modules[self.package] = importlib.util.module_from_spec(spec)
        importlib.invalidate_caches()

    def module_name(self, path: Path) -> str:
        """Returns the name of the module of a file of the root folder."""
        relative = path.resolve().relative_to(self.root).with_suffix("")
        return ".".join((self.package, *relative.parts))

    def load(self, path: Path) -> ModuleType:
        """Imports a Python file of the root folder.

        Raises:
            ConfigurationError: If the file cannot be imported.
        """
        previous = sys.dont_write_bytecode
        # Workspace folders should not get __pycache__ entries.
        sys.dont_write_bytecode = True
        try:
            return importlib.import_module(self.module_name(path))
        except ConfigurationError:
            raise
        except Exception as exc:
            raise ConfigurationError(f"Cannot import {path}: {type(exc).__name__}: {exc}") from exc
        finally:
            sys.dont_write_bytecode = previous


def load_config(loader: ModuleLoader, path: Path, name: str, expected_class: type[_LevelConfig]) -> object:
    """Loads the ``config`` object of a configuration file.

    The object may be an instance of the configuration class of its level, of a
    subclass, or any object with the same attributes. A class is instantiated without
    arguments.

    Args:
        loader: Loader of the root folder of the file.
        path: The configuration file.
        name: Name of the element, which ``config.name`` must match.
        expected_class: Configuration class of the level, e.g. ``GroupConfig``.

    Raises:
        ConfigurationError: If the file does not define a valid ``config``.
    """
    module = loader.load(path)
    config = getattr(module, CONFIG_ATTRIBUTE, None)
    if config is None:
        raise ConfigurationError(
            f"{path} must define a module-level {CONFIG_ATTRIBUTE!r} variable, typically a {expected_class.__name__}."
        )
    if isinstance(config, type):
        try:
            config = config()
        except TypeError as exc:
            raise ConfigurationError(f"{path}: cannot instantiate {config.__name__}: {exc}") from exc
    if isinstance(config, _LevelConfig) and not isinstance(config, expected_class):
        raise ConfigurationError(f"{path} defines a {type(config).__name__}, expected a {expected_class.__name__}.")
    declared = getattr(config, "name", None)
    if declared != name:
        raise ConfigurationError(
            f"{path}: config.name is {declared!r}, but it must be {name!r}, the name of its folder."
        )
    return config
