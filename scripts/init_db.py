#!/usr/bin/env python3
"""
One-shot database initialization script.
Run: python scripts/init_db.py
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.migrations import initialize_database, get_schema_version


def main():
    print("Initializing database...")
    initialize_database()
    version = get_schema_version()
    print(f"Database initialized successfully (schema version: {version})")
    print("Tables created: agents, markets, trades, agent_logs, api_costs,")
    print("  suggestions, ml_models, circuit_breaker, response_cache")


if __name__ == "__main__":
    main()
