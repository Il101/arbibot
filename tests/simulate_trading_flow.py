import asyncio
import sys
import json
import os
from pathlib import Path
from typing import Dict, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from core.event_bus import EventBus
from services.execution import ExecutionEngine
from utils.logger import setup_logger

class MockWSManager:
    def __init__(self):
        self.prices = {
            'bingx': {'BTC/USDT': {'bid': 90000.0, 'ask': 90005.0, 'last': 90002.5}},
            'bybit': {'BTC/USDT': {'bid': 89950.0, 'ask': 89955.0, 'last': 89952.5}}
        }
    
    def get_latest_price(self, exchange: str, symbol: str) -> Dict[str, float]:
        return self.prices.get(exchange, {}).get(symbol)

async def simulate_flow():
    setup_logger('simulation', level='INFO')
    bus = EventBus.instance()
    mock_ws = MockWSManager()
    
    # Path to paper state
    state_file = Path('data/paper_state.json')
    if state_file.exists():
        os.remove(state_file)
        print("🗑️ Reset existing paper state.")

    print("🚀 Initializing Multi-Exchange ExecutionEngine with Mock WS...")
    engine = ExecutionEngine(
        position_size_usdt=100.0,
        ws_manager=mock_ws
    )
    
    # --- PHASE 1: ENTRY ---
    print("\n--- PHASE 1: EMITTING ENTRY SIGNAL ---")
    bus.emit_signal_triggered('BTC/USDT', 'ENTRY', 3.5, 'bingx', 'bybit')
    
    # Give it time to execute, CCXT fetching can take ~2s
    await asyncio.sleep(4)
    
    if 'BTC/USDT' in engine.active_trades:
        print("✅ Trade successfully opened and tracked in ExecutionEngine.")
    else:
        print("❌ Trade NOT opened. Check logs.")
        return

    # --- PHASE 2: EXIT ---
    print("\n--- PHASE 2: EMITTING EXIT SIGNAL ---")
    # Change prices to show some profit
    mock_ws.prices['bingx']['BTC/USDT']['bid'] = 89900.0  # Buy leg was here? Actually Leg A side depends on Z-Score.
    # In execute_arb_entry:
    # if z_score > 0: side_a = 'sell', side_b = 'buy'
    # Here z_score = 3.5, so A = SELL (Short), B = BUY (Long)
    # To profit on Short A: Price A must drop. (90000 -> 89900) ✅
    # To profit on Long B: Price B must rise. (89950 -> 90050)
    mock_ws.prices['bybit']['BTC/USDT']['ask'] = 90050.0  
    
    bus.emit_signal_triggered('BTC/USDT', 'EXIT', 0.5, 'bingx', 'bybit')
    
    # Give it time to execute
    await asyncio.sleep(4)
    
    if 'BTC/USDT' not in engine.active_trades:
        print("✅ Trade successfully closed.")
        print(f"📈 Total Trades: {engine.total_trades}")
        print(f"💰 Cumulative P&L: ${engine.cumulative_pnl:.2f}")
    else:
        print("❌ Trade NOT closed. Check logs.")

    # --- PHASE 3: STATE VERIFICATION ---
    if state_file.exists():
        with open(state_file, 'r') as f:
            state = json.load(f)
            print(f"\n📄 Persisted State Check:")
            print(f"Balance BingX: ${state.get('bingx', {}).get('balance'):.2f}")
            print(f"Balance Bybit: ${state.get('bybit', {}).get('balance'):.2f}")
            
    # Cleanup
    await engine.shutdown()

if __name__ == "__main__":
    asyncio.run(simulate_flow())
