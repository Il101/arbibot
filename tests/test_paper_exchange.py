"""
Test Paper Exchange

Verifies PaperExchange functionality:
- Order execution with bid/ask pricing
- Fee deduction
- Position tracking
- P&L calculation
- State persistence
"""

import asyncio
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock

from core.exchanges.paper import PaperExchange


class MockWebSocketManager:
    """Mock WebSocket Manager for testing."""
    
    def __init__(self):
        self.prices = {}
    
    def get_latest_price(self, exchange, symbol):
        return self.prices.get(symbol)
    
    def set_price(self, symbol, bid, ask, last):
        self.prices[symbol] = {
            'bid': bid,
            'ask': ask,
            'last': last,
            'timestamp': 1234567890
        }


async def test_paper_exchange():
    print("🧪 Starting PaperExchange Test...")
    
    # Setup
    data_dir = Path('data')
    state_file = data_dir / 'test_paper_state.json'
    
    # Clean up previous test
    if state_file.exists():
        state_file.unlink()
    
    # Mock dependencies
    ws_manager = MockWebSocketManager()
    ws_manager.set_price('BTC/USDT', 50000.0, 50010.0, 50005.0)
    
    # Initialize Exchange
    exchange = PaperExchange(
        exchange_name='bingx',
        initial_balance=10000.0,
        ws_manager=ws_manager,
        fee_rate=0.001,  # 0.1% fee for easy math
        state_file=str(state_file)
    )
    
    print(f"✅ Initial Balance: ${exchange.balance:.2f}")
    
    # Test 1: Buy Order (Long)
    print("\n📝 Test 1: Buy Order (Long)")
    # Buy 0.1 BTC at Ask (50010.0)
    # Cost = 5001.0
    # Fee = 5.001
    # Total = 5006.001
    order = await exchange.create_order('BTC/USDT', 'buy', 0.1)
    
    print(f"Order Executed: {order['side']} {order['filled']} @ ${order['average']}")
    print(f"Fee: ${order['fee']['cost']:.3f}")
    
    assert order['average'] == 50010.0, "Should buy at ASK price"
    assert abs(order['fee']['cost'] - 5.001) < 0.001, "Fee calculation incorrect"
    assert abs(exchange.balance - (10000.0 - 5006.001)) < 0.001, "Balance deduction incorrect"
    print("✅ Buy Order Verified")
    
    # Test 2: Position Tracking & P&L
    print("\n📝 Test 2: Position Tracking & P&L")
    # Price moves up: Bid=51000, Ask=51010
    ws_manager.set_price('BTC/USDT', 51000.0, 51010.0, 51005.0)
    
    positions = await exchange.fetch_positions()
    pos = positions[0]
    
    print(f"Position: {pos['side']} {pos['contracts']} BTC")
    print(f"Entry: ${pos['entryPrice']:.2f}, Mark: ${pos['markPrice']:.2f}")
    print(f"Unrealized P&L: ${pos['unrealizedPnl']:.2f}")
    
    # Entry: 50010, Mark: 51000 (Bid, for closing long)
    # Gross P&L = (51000 - 50010) * 0.1 = 99.0
    # Closing Fee = (51000 * 0.1) * 0.001 = 5.1
    # Net Unrealized P&L = 99.0 - 5.1 = 93.9
    assert abs(pos['unrealizedPnl'] - 93.9) < 0.01, "P&L calculation incorrect"
    print("✅ P&L Verified")
    
    # Test 3: State Persistence
    print("\n📝 Test 3: State Persistence")
    # Re-initialize exchange to simulate restart
    exchange_new = PaperExchange(
        exchange_name='bingx',
        initial_balance=10000.0,  # Should be ignored
        ws_manager=ws_manager,
        fee_rate=0.001,
        state_file=str(state_file)
    )
    
    print(f"Restored Balance: ${exchange_new.balance:.2f}")
    positions_new = await exchange_new.fetch_positions()
    
    assert abs(exchange_new.balance - exchange.balance) < 0.001, "Balance not restored"
    assert len(positions_new) == 1, "Positions not restored"
    assert positions_new[0]['contracts'] == 0.1, "Position size incorrect"
    print("✅ Persistence Verified")
    
    # Test 4: Close Position
    print("\n📝 Test 4: Close Position")
    # Close at Bid (51000.0)
    # Proceeds = 5100.0
    # Fee = 5.1
    # Net Proceeds = 5094.9
    close_order = await exchange_new.close_position('BTC/USDT')
    
    print(f"Closed @ ${close_order['average']}")
    print(f"Realized P&L: ${close_order['pnl']:.2f}")
    
    # Entry: 50010, Exit: 51000
    # Gross P&L: (51000 - 50010) * 0.1 = 99.0
    # Realized P&L = Gross P&L - Fee on the Closing Trade
    # Fee = (51000 * 0.1) * 0.001 = 5.1
    # Net Realized P&L = 99.0 - 5.1 = 93.9
    assert abs(close_order['pnl'] - 93.9) < 0.01, "Realized P&L incorrect"
    assert len(exchange_new.positions) == 0, "Position not removed"
    print("✅ Close Position Verified")
    
    # Cleanup
    if state_file.exists():
        state_file.unlink()
    
    print("\n🎉 All Tests Passed!")

if __name__ == '__main__':
    asyncio.run(test_paper_exchange())
