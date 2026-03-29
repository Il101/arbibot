# ArbiBot

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![WebSocket](https://img.shields.io/badge/WebSocket-Real--time-green.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

Statistical arbitrage bot for crypto futures markets (Bybit/BingX) with spread validation, Z-score strategy, and risk controls.

## Features

- Z-score mean reversion strategy
- ADF stationarity validation
- Real-time market data via WebSocket
- Market scanner for pair discovery
- Risk controls (position limits, stop-loss, time stop)
- Desktop GUI (PyQt)

## Quick Start

```bash
git clone https://github.com/Il101/arbibot.git
cd arbibot
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp config/exchanges.yaml.example config/exchanges.yaml
# fill API keys in config/exchanges.yaml
python -m services.market_scanner
```

## Project Structure

- `core/` — exchange/websocket clients
- `services/` — scanner, validation, execution logic
- `gui/` — desktop interface
- `utils/` — configs, logging, helpers

## Disclaimer

Trading is risky. Use on testnet first and at your own risk.

## License

MIT
