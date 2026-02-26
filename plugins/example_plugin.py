"""
Example Plugin Template.
Copy this file and modify to create your own plugin.
OpenClaw can create new plugins by writing .py files to this directory.

To create a new agent type via plugin:
1. Define a class that inherits from BaseAgent
2. Implement run_cycle()
3. Register it in the register() function
4. Create a YAML config in agent_configs/ with the new role
"""

from agents.base_agent import BaseAgent


class ExampleCustomAgent(BaseAgent):
    """Example custom agent created via plugin."""

    def run_cycle(self) -> dict:
        self.log("info", "Example plugin agent running")
        return {"ok": True, "summary": "Example plugin cycle complete"}


def register(context):
    """
    Called by the plugin loader on startup.
    Use context to register agent roles, hooks, etc.
    """
    # Uncomment to register this example agent role:
    # context.register_agent_role("example_custom", ExampleCustomAgent)
    pass
