"""Tests for the agent system."""

import json
from config import AgentConfigYAML, load_agent_configs
from agents.base_agent import BaseAgent
from agents.agent_factory import create_agent, GenericAgent
from agents.chief import ChiefAgent
from agents.observer import ObserverAgent
from agents.agent_registry import AgentRegistry


def test_create_chief_agent():
    """Test that chief config creates a ChiefAgent."""
    cfg = AgentConfigYAML(id="test_chief", name="Test Chief", role="chief", persona="Test")
    agent = create_agent(cfg)
    assert isinstance(agent, ChiefAgent)
    assert agent.id == "test_chief"


def test_create_observer_agent():
    """Test that observer config creates an ObserverAgent."""
    cfg = AgentConfigYAML(id="test_obs", name="Test Observer", role="observer")
    agent = create_agent(cfg)
    assert isinstance(agent, ObserverAgent)


def test_create_generic_agent():
    """Test that unknown role creates a GenericAgent."""
    cfg = AgentConfigYAML(id="test_custom", name="Custom", role="unknown_role")
    agent = create_agent(cfg)
    assert isinstance(agent, GenericAgent)


def test_agent_logging(reset_db):
    """Test that agent logging writes to DB."""
    from db import engine

    cfg = AgentConfigYAML(id="log_test", name="Logger", role="custom")
    agent = create_agent(cfg)
    agent.log("info", "Test message", {"key": "value"})

    logs = engine.query("SELECT * FROM agent_logs WHERE agent_id = 'log_test'")
    assert len(logs) == 1
    assert logs[0]["message"] == "Test message"
    assert logs[0]["level"] == "info"


def test_agent_create_suggestion(reset_db):
    """Test that agents can create suggestions."""
    from db import engine

    cfg = AgentConfigYAML(id="sugg_test", name="Suggester", role="custom")
    agent = create_agent(cfg)
    agent.create_suggestion("trade", "Buy BTC", description="Good opportunity", payload={"market": "btc"})

    suggs = engine.query("SELECT * FROM suggestions WHERE agent_id = 'sugg_test'")
    assert len(suggs) == 1
    assert suggs[0]["title"] == "Buy BTC"
    assert suggs[0]["type"] == "trade"
    assert suggs[0]["status"] == "pending"


def test_agent_memory(reset_db, tmp_path):
    """Test agent memory save/load."""
    import config
    original = config.DATA_DIR
    config.DATA_DIR = tmp_path

    cfg = AgentConfigYAML(id="mem_test", name="Memory", role="custom")
    agent = create_agent(cfg)

    agent.save_memory({"learned": "something", "count": 42})
    loaded = agent.load_memory()

    assert loaded["learned"] == "something"
    assert loaded["count"] == 42

    config.DATA_DIR = original


def test_agent_registry():
    """Test agent registry operations."""
    registry = AgentRegistry()
    assert registry.count == 0

    cfg = AgentConfigYAML(id="reg_test", name="Registered", role="custom", enabled=True)
    from agents.agent_factory import create_agent
    agent = create_agent(cfg)
    registry._agents["reg_test"] = agent

    assert registry.count == 1
    assert registry.get("reg_test") is not None
    assert registry.get("nonexistent") is None

    registry.remove("reg_test")
    assert registry.count == 0


def test_load_agent_configs():
    """Test that YAML agent configs can be loaded."""
    configs = load_agent_configs()
    assert isinstance(configs, list)
    # Should have at least chief, observer, analyst, risk_manager, trader
    assert len(configs) >= 5
    roles = [c.role for c in configs]
    assert "chief" in roles
    assert "observer" in roles
