# Polymarket Agent Dashboard

Self-erweiterbare Streamlit-Plattform zur Steuerung und Überwachung eines Polymarket-Agent-Teams.

## Features

- **Dashboard Home** — KPI-Übersicht (Positionen, PnL, AI-Kosten, Agents)
- **Security & Setup** — VPS-Sicherheitsstatus + Härtungs-Skripte
- **Agent Manager** — Agents aus YAML erstellen/verwalten
- **Live Monitoring** — Polymarket-Märkte, Preise, Volumen (Plotly Charts)
- **Backtesting** — Monte-Carlo, Walk-Forward, Drawdown (Phase 3)
- **ML Improvement** — XGBoost/LightGBM Auto-Training (Phase 4)
- **Cost Tracker** — API-Kosten pro Agent, Budget-Limits, Warnungen
- **Vorschläge** — Chief Agent Empfehlungen mit Ja/Nein/Testen
- **Execution** — EXECUTE-Kommando, Circuit Breaker
- **System Config** — YAML-Editor im Browser

## Quick Start

### Lokal

```bash
# 1. Klonen
git clone <repo-url>
cd polymarket-dashboard

# 2. Virtuelle Umgebung
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Dependencies
pip install -r requirements.txt

# 4. Konfiguration
cp .env.example .env
# .env bearbeiten mit deinen API Keys

# 5. Datenbank initialisieren
python scripts/init_db.py

# 6. Starten
streamlit run app.py
```

### Docker (VPS)

```bash
# 1. .env konfigurieren
cp .env.example .env
nano .env

# 2. Starten
docker-compose up -d

# 3. Nginx + SSL (optional)
sudo bash scripts/setup_nginx.sh deine-subdomain.example.com
```

## Architektur

```
Browser → Streamlit UI ↔ SQLite DB ↔ Response Cache
                ↕                          ↕
         Agent Registry ← YAML Configs    Cost Tracker → Budget Enforcement
                ↕                                              ↕
         Chief Agent → Sub-Agents                    Telegram Alerts → DU
                ↕                ↕
     Telegram Bridge      Polymarket API
           ↕
     OpenClaw (VPS)
```

## Config-Driven

Alles wird über YAML gesteuert:

- `platform_config.yaml` — Budgets, Alerts, Scheduler, Cache
- `agent_configs/*.yaml` — Agent-Definitionen (Persona, Skills, Schedule, Budget)
- `plugins/*.py` — Dynamische Erweiterungen

OpenClaw kann YAML-Dateien und Plugins erstellen um das System selbst zu erweitern.

## Sicherheit

- Alle Trades nur mit explizitem `EXECUTE` Kommando
- Circuit Breaker: 3 Verluste → 24h Pause
- Budget-Limits pro Agent/Tag/Monat
- Passwort-geschütztes Dashboard
- VPS-Härtung via Script

## Projektstruktur

```
├── app.py              # Streamlit Entrypoint
├── config.py           # Config Loader
├── platform_config.yaml
├── agent_configs/      # Agent YAML Definitionen
├── db/                 # SQLite Database Layer
├── services/           # Polymarket, Telegram, Costs, Scheduler
├── agents/             # Agent System (Base, Chief, Observer, etc.)
├── plugins/            # Dynamische Erweiterungen
├── pages/              # Streamlit Pages (10 Tabs)
├── components/         # Wiederverwendbare UI-Komponenten
├── ml/                 # Machine Learning (Phase 4)
├── backtesting/        # Backtesting Engine (Phase 3)
├── scripts/            # Setup & Deployment Scripts
└── data/               # SQLite DB, ML Models, Agent Memories
```
