"""
Dynamic plugin loader.
Scans the plugins/ directory for Python modules and loads them.
Plugins can register new agent roles, add scheduler jobs, or extend services.

Convention:
- Each plugin is a .py file in the plugins/ directory
- Each plugin must have a `register(app_context)` function
- app_context provides access to agent_factory, scheduler, etc.
"""

import importlib
import logging
from pathlib import Path

from config import PLUGINS_DIR

logger = logging.getLogger(__name__)


class PluginContext:
    """Context object passed to plugins during registration."""

    def __init__(self):
        self.registered_roles: list[str] = []
        self.registered_hooks: list[str] = []

    def register_agent_role(self, role_name: str, agent_cls):
        """Register a new agent role that can be used in YAML configs."""
        from agents.agent_factory import register_role
        register_role(role_name, agent_cls)
        self.registered_roles.append(role_name)
        logger.info(f"Plugin registered agent role: {role_name}")


def load_plugins() -> list[str]:
    """
    Load all plugins from the plugins/ directory.
    Returns list of successfully loaded plugin names.
    """
    loaded = []
    context = PluginContext()

    if not PLUGINS_DIR.exists():
        return loaded

    for plugin_file in sorted(PLUGINS_DIR.glob("*.py")):
        if plugin_file.name.startswith("_") or plugin_file.name == "plugin_loader.py":
            continue

        module_name = plugin_file.stem

        try:
            spec = importlib.util.spec_from_file_location(
                f"plugins.{module_name}", str(plugin_file)
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            if hasattr(module, "register"):
                module.register(context)
                loaded.append(module_name)
                logger.info(f"Plugin loaded: {module_name}")
            else:
                logger.warning(f"Plugin {module_name} has no register() function, skipped.")

        except Exception as e:
            logger.error(f"Failed to load plugin {module_name}: {e}")

    return loaded
