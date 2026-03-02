#!/usr/bin/env python3
"""
Pre-LIVE Exchange Connectivity Test

Run this BEFORE switching to LIVE mode to verify:
1. API keys are valid and have correct permissions
2. Balance fetching works
3. Ticker/price data works
4. Order book fetching works
5. Position fetching works
6. Symbol resolution works
7. Market info (min order size) is available

DOES NOT PLACE ANY ORDERS.

Usage:
    python tests/test_exchange_connectivity.py --exchange bingx --config config/config.yaml
    python tests/test_exchange_connectivity.py --exchange bybit --config config/config.yaml
    python tests/test_exchange_connectivity.py --all --config config/config.yaml
"""

import asyncio
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import ccxt.async_support as ccxt
from utils.config import get_config
from utils.logger import get_logger

logger = get_logger('connectivity_test')


class ExchangeConnectivityTest:
    """Test all exchange API calls without placing orders."""
    
    def __init__(self, exchange_name: str, config: dict):
        self.exchange_name = exchange_name
        self.config = config
        self.client = None
        self.results = {}
        self.test_symbol = config.get('trading', {}).get('default_symbol', 'BTC/USDT')
    
    async def setup(self):
        """Create authenticated CCXT client."""
        ex_config = self.config.get('exchanges', {}).get(self.exchange_name, {})
        api_key = ex_config.get('api_key', '')
        api_secret = ex_config.get('api_secret', '')
        passphrase = ex_config.get('passphrase', '')
        testnet = ex_config.get('testnet', False)
        
        if not api_key or not api_secret:
            print(f"  ⚠️  No API keys configured for {self.exchange_name}")
            print(f"     Set them in config.yaml under exchanges.{self.exchange_name}")
            self.results['setup'] = 'SKIP (no keys)'
            return False
        
        exchange_map = {
            'bingx': ccxt.bingx,
            'bybit': ccxt.bybit,
            'bitget': ccxt.bitget,
            'gateio': ccxt.gateio,
            'htx': ccxt.htx,
            'phemex': ccxt.phemex,
            'mexc': ccxt.mexc,
        }
        
        if self.exchange_name not in exchange_map:
            print(f"  ❌ Unknown exchange: {self.exchange_name}")
            self.results['setup'] = 'FAIL'
            return False
        
        ex_class = exchange_map[self.exchange_name]
        opts = {'defaultType': ex_config.get('default_type', 'swap')}
        if testnet:
            opts['testnet'] = True
        
        self.client = ex_class({
            'apiKey': api_key,
            'secret': api_secret,
            'password': passphrase,
            'enableRateLimit': True,
            'options': opts,
        })
        
        self.results['setup'] = 'OK'
        return True
    
    async def test_load_markets(self):
        """Test 1: Load markets."""
        try:
            await self.client.load_markets()
            n_markets = len(self.client.symbols)
            print(f"  ✅ Load Markets: {n_markets} symbols available")
            
            # Check if test symbol exists
            if self.test_symbol in self.client.symbols:
                print(f"     ✅ {self.test_symbol} found")
            else:
                # Try with :USDT suffix
                alt = f"{self.test_symbol}:USDT"
                if alt in self.client.symbols:
                    print(f"     ✅ {alt} found (futures format)")
                    self.test_symbol = alt
                else:
                    print(f"     ⚠️  {self.test_symbol} not found, some tests may fail")
            
            self.results['load_markets'] = 'OK'
        except Exception as e:
            print(f"  ❌ Load Markets: {e}")
            self.results['load_markets'] = f'FAIL: {e}'
    
    async def test_fetch_balance(self):
        """Test 2: Fetch balance (requires auth)."""
        try:
            balance = await self.client.fetch_balance()
            usdt_free = balance.get('USDT', {}).get('free', 0)
            usdt_total = balance.get('USDT', {}).get('total', 0)
            print(f"  ✅ Balance: USDT free=${usdt_free:.2f}, total=${usdt_total:.2f}")
            
            if usdt_free < 10:
                print(f"     ⚠️  Low balance! Need at least $10 for live trading")
            
            self.results['fetch_balance'] = 'OK'
        except ccxt.AuthenticationError as e:
            print(f"  ❌ Balance (AUTH ERROR): {e}")
            print(f"     → Check your API key and secret for {self.exchange_name}")
            self.results['fetch_balance'] = f'FAIL: Auth'
        except Exception as e:
            print(f"  ❌ Balance: {e}")
            self.results['fetch_balance'] = f'FAIL: {e}'
    
    async def test_fetch_ticker(self):
        """Test 3: Fetch ticker data."""
        try:
            ticker = await self.client.fetch_ticker(self.test_symbol)
            print(
                f"  ✅ Ticker {self.test_symbol}: "
                f"bid=${ticker.get('bid', 0):.2f}, "
                f"ask=${ticker.get('ask', 0):.2f}, "
                f"last=${ticker.get('last', 0):.2f}"
            )
            self.results['fetch_ticker'] = 'OK'
        except Exception as e:
            print(f"  ❌ Ticker: {e}")
            self.results['fetch_ticker'] = f'FAIL: {e}'
    
    async def test_fetch_order_book(self):
        """Test 4: Fetch order book (depth)."""
        try:
            book = await self.client.fetch_order_book(self.test_symbol, limit=10)
            n_bids = len(book.get('bids', []))
            n_asks = len(book.get('asks', []))
            
            if n_bids > 0 and n_asks > 0:
                best_bid = book['bids'][0][0]
                best_ask = book['asks'][0][0]
                spread = best_ask - best_bid
                print(
                    f"  ✅ Order Book: {n_bids} bids, {n_asks} asks. "
                    f"BBO: ${best_bid:.2f} / ${best_ask:.2f} (spread=${spread:.2f})"
                )
            else:
                print(f"  ⚠️  Order Book: empty ({n_bids} bids, {n_asks} asks)")
            
            self.results['fetch_order_book'] = 'OK'
        except Exception as e:
            print(f"  ❌ Order Book: {e}")
            self.results['fetch_order_book'] = f'FAIL: {e}'
    
    async def test_fetch_positions(self):
        """Test 5: Fetch open positions (requires auth)."""
        try:
            positions = await self.client.fetch_positions()
            open_pos = [p for p in positions if float(p.get('contracts', 0)) > 0]
            print(f"  ✅ Positions: {len(open_pos)} open positions")
            
            for p in open_pos:
                print(
                    f"     📊 {p['symbol']}: {p['side']} {p['contracts']} contracts, "
                    f"entry=${p.get('entryPrice', '?')}, "
                    f"uPnL=${p.get('unrealizedPnl', '?')}"
                )
            
            self.results['fetch_positions'] = 'OK'
        except Exception as e:
            print(f"  ❌ Positions: {e}")
            self.results['fetch_positions'] = f'FAIL: {e}'
    
    async def test_market_info(self):
        """Test 6: Check market limits (min order size, step size)."""
        try:
            market = self.client.market(self.test_symbol)
            limits = market.get('limits', {})
            amount_limits = limits.get('amount', {})
            cost_limits = limits.get('cost', {})
            precision = market.get('precision', {})
            
            min_amount = amount_limits.get('min', 'N/A')
            min_cost = cost_limits.get('min', 'N/A')
            amount_precision = precision.get('amount', 'N/A')
            
            print(
                f"  ✅ Market Info {self.test_symbol}: "
                f"min_amount={min_amount}, min_cost=${min_cost}, "
                f"precision={amount_precision}"
            )
            
            # Check if our position size is above minimum
            pos_size = self.config.get('trading', {}).get('position_size_usdt', 100)
            if min_cost and min_cost != 'N/A' and pos_size < float(min_cost):
                print(f"     ⚠️  Position size ${pos_size} is below exchange minimum ${min_cost}!")
            
            self.results['market_info'] = 'OK'
        except Exception as e:
            print(f"  ❌ Market Info: {e}")
            self.results['market_info'] = f'FAIL: {e}'
    
    async def test_permissions(self):
        """Test 7: Verify API key has trading permissions."""
        try:
            # Try fetching open orders (common endpoint that requires trade permission)
            orders = await self.client.fetch_open_orders(self.test_symbol)
            print(f"  ✅ Trading Permission: OK ({len(orders)} open orders)")
            self.results['permissions'] = 'OK'
        except ccxt.AuthenticationError as e:
            if 'permission' in str(e).lower() or 'access' in str(e).lower():
                print(f"  ❌ Trading Permission: API key lacks trading permissions!")
                print(f"     → Enable 'Trade' permission in your API key settings")
                self.results['permissions'] = 'FAIL: No trade permission'
            else:
                print(f"  ❌ Trading Permission check: {e}")
                self.results['permissions'] = f'FAIL: {e}'
        except Exception as e:
            # Some exchanges don't support fetch_open_orders for all market types
            print(f"  ⚠️  Trading Permission: Could not verify ({e})")
            self.results['permissions'] = 'WARN'
    
    async def run_all(self):
        """Run all connectivity tests."""
        print(f"\n{'='*60}")
        print(f"🔍 Exchange Connectivity Test: {self.exchange_name.upper()}")
        print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        success = await self.setup()
        if not success:
            print(f"\n{'='*60}")
            print(f"⏩ Skipped {self.exchange_name} (no API keys)")
            print(f"{'='*60}\n")
            return self.results
        
        await self.test_load_markets()
        await self.test_fetch_balance()
        await self.test_fetch_ticker()
        await self.test_fetch_order_book()
        await self.test_fetch_positions()
        await self.test_market_info()
        await self.test_permissions()
        
        # Cleanup
        await self.client.close()
        
        # Summary
        print(f"\n{'─'*60}")
        passed = sum(1 for v in self.results.values() if v == 'OK')
        total = len(self.results)
        failed = sum(1 for v in self.results.values() if isinstance(v, str) and v.startswith('FAIL'))
        
        if failed == 0:
            print(f"✅ ALL TESTS PASSED ({passed}/{total})")
        else:
            print(f"⚠️  {passed}/{total} passed, {failed} FAILED")
        print(f"{'='*60}\n")
        
        return self.results


async def main():
    parser = argparse.ArgumentParser(description='Pre-LIVE Exchange Connectivity Test')
    parser.add_argument('--exchange', type=str, help='Exchange to test (e.g., bingx, bybit)')
    parser.add_argument('--all', action='store_true', help='Test all configured exchanges')
    parser.add_argument('--config', default='config/config.yaml', help='Config path')
    parser.add_argument('--symbol', type=str, help='Override test symbol (e.g., ETH/USDT)')
    
    args = parser.parse_args()
    config = get_config(args.config)
    
    if args.symbol:
        config.setdefault('trading', {})['default_symbol'] = args.symbol
    
    exchanges_to_test = []
    
    if args.all:
        # Test all exchanges that have API keys configured
        for ex_name, ex_cfg in config.get('exchanges', {}).items():
            if ex_cfg.get('api_key'):
                exchanges_to_test.append(ex_name)
        
        if not exchanges_to_test:
            print("❌ No exchanges have API keys configured in config.yaml")
            sys.exit(1)
    elif args.exchange:
        exchanges_to_test = [args.exchange]
    else:
        print("Usage: python tests/test_exchange_connectivity.py --exchange bingx")
        print("       python tests/test_exchange_connectivity.py --all")
        sys.exit(1)
    
    all_results = {}
    for ex_name in exchanges_to_test:
        tester = ExchangeConnectivityTest(ex_name, config)
        results = await tester.run_all()
        all_results[ex_name] = results
    
    # Final summary
    if len(all_results) > 1:
        print(f"\n{'='*60}")
        print("📋 FINAL SUMMARY")
        print(f"{'='*60}")
        for ex_name, results in all_results.items():
            failed = sum(1 for v in results.values() if isinstance(v, str) and v.startswith('FAIL'))
            if failed == 0:
                print(f"  ✅ {ex_name}: ALL PASSED")
            else:
                print(f"  ❌ {ex_name}: {failed} FAILED")
        print(f"{'='*60}\n")


if __name__ == '__main__':
    asyncio.run(main())
