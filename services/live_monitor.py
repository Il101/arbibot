"""
Live Monitor Service

Consumes real-time WebSocket price feeds and calculates live Z-Scores for arbitrage monitoring.
Detects entry/exit signals and emits events via EventBus.

HYBRID APPROACH:
- Pre-loads 60 minutes of historical 1-minute candles to establish baseline spread statistics
- Calculates real-time Z-Scores by comparing live tick spreads against historical baseline
- Updates historical baseline once per minute to keep it moving forward slowly
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Optional
from collections import deque
from datetime import datetime
import signal
import time

import pandas as pd
import ccxt.async_support as ccxt

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from core.ws_manager import WebSocketManager
from core.event_bus import EventBus
from utils.metrics import calculate_z_score, calculate_net_spread
from utils.logger import get_logger
from utils.symbol_resolver import SymbolResolver


class LiveMonitor:
    """
    Real-time arbitrage monitoring service with hybrid Z-Score calculation.
    
    Consumes WebSocket price feeds, calculates spreads and Z-Scores against
    historical baseline, and emits trading signals when thresholds are crossed.
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialize Live Monitor.
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = get_logger(__name__)
        
        # Components
        self.ws_manager = WebSocketManager(config_path)
        self.event_bus = EventBus.instance()
        self.config = self.ws_manager.config
        self.resolver = SymbolResolver(self.config)
        
        # Initialize CCXT exchanges for all supported platforms
        self.supported_exchanges = {'bingx', 'bybit', 'bitget', 'gateio', 'htx', 'phemex', 'mexc'}
        self.exchange_clients: Dict[str, ccxt.Exchange] = {}
        
        # Mapping of exchange IDs to their CCXT classes and options
        exchange_configs = {
            'bingx': (ccxt.bingx, {'options': {'defaultType': 'swap'}}),
            'bybit': (ccxt.bybit, {'options': {'defaultType': 'linear'}}),
            'bitget': (ccxt.bitget, {'options': {'defaultType': 'swap'}}),
            'gateio': (ccxt.gateio, {'options': {'defaultType': 'swap'}}),
            'htx': (ccxt.htx, {'options': {'defaultType': 'swap'}}),
            'phemex': (ccxt.phemex, {'options': {'defaultType': 'swap'}}),
            'mexc': (ccxt.mexc, {'options': {'defaultType': 'swap'}})
        }
        
        for ex_id, (ex_class, options) in exchange_configs.items():
            self.exchange_clients[ex_id] = ex_class({
                'enableRateLimit': True,
                **options
            })
        
        # Fees storage: exchange_id -> {'taker': float, 'maker': float}
        self.fees: Dict[str, Dict[str, float]] = {
            ex_id: self.config['fees'].get(ex_id, {'taker': 0.0006, 'maker': 0.0002})
            for ex_id in self.supported_exchanges
        }
        
        # Spread history storage (symbol -> deque of historical GROSS spreads)
        # CRITICAL: This stores GROSS SPREAD (market data), not net spread
        # Z-Score measures market anomaly, not profitability
        self.spread_history: Dict[str, deque] = {}
        
        # Load Z-Score parameters from config
        monitor_config = self.config.get('monitoring', {})
        self.history_timeframe = monitor_config.get('timeframe', '5m')
        self.history_length = monitor_config.get('history_length', 100)
        
        # Calculate update interval based on timeframe
        timeframe_minutes = {
            '1m': 1, '3m': 3, '5m': 5, '15m': 15, '30m': 30, '1h': 60, '4h': 240
        }
        self.timeframe_mins = timeframe_minutes.get(self.history_timeframe, 5)
        self.history_update_interval = self.timeframe_mins * 60  # Update interval in seconds
        
        self.logger.info(f"Z-Score Config: Timeframe={self.history_timeframe} ({self.timeframe_mins}m), Window={self.history_length} candles")
        
        # Track last history update time for each symbol
        self.last_history_update: Dict[str, float] = {}
        
        # Track active exchange pairs for each symbol
        self.active_pairs: Dict[str, tuple] = {}  # symbol -> (ex_a, ex_b)
        
        # Track position state for each symbol
        self.in_position: Dict[str, bool] = {}
        
        # Price cache for all exchanges
        self.price_cache: Dict[str, Dict[str, dict]] = {
            ex: {} for ex in self.supported_exchanges
        }
        
        # Signal persistence counters: symbol -> {'entry': int, 'exit': int}
        self.signal_counters: Dict[str, Dict[str, int]] = {}
        
        # Control
        self.running = False
        self.monitor_task: Optional[asyncio.Task] = None
        
        self.logger.info("LiveMonitor initialized with hybrid Z-Score approach")
    
    async def _preload_history(self, symbol: str, ex_a: str = 'bingx', ex_b: str = 'bybit') -> None:
        """
        Pre-load historical 1-minute candles for baseline spread calculation.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            ex_a: First exchange ID
            ex_b: Second exchange ID
        """
        try:
            self.logger.info(f"Pre-loading 60 minutes of history for {symbol} on {ex_a} and {ex_b}...")
            
            client_a = self.exchange_clients.get(ex_a)
            client_b = self.exchange_clients.get(ex_b)
            
            if not client_a or not client_b:
                self.logger.error(f"Invalid exchanges for pre-load: {ex_a}, {ex_b}")
                return

            # Resolve exchange-specific symbols
            symbol_a = await self.resolver.resolve(client_a, symbol)
            symbol_b = await self.resolver.resolve(client_b, symbol)
            
            if not symbol_a or not symbol_b:
                self.logger.warning(f"Could not resolve symbols for pre-loading {symbol}. {ex_a}: {symbol_a}, {ex_b}: {symbol_b}")
                # Fallback: start with empty deque
                self.spread_history[symbol] = deque(maxlen=self.history_length)
                self.last_history_update[symbol] = time.time()
                return

            # Fetch candles from both exchanges
            candles_a = await client_a.fetch_ohlcv(
                symbol=symbol_a,
                timeframe=self.history_timeframe,
                limit=self.history_length
            )
            
            candles_b = await client_b.fetch_ohlcv(
                symbol=symbol_b,
                timeframe=self.history_timeframe,
                limit=self.history_length
            )
            
            # Ensure we have data from both exchanges
            if not candles_a or not candles_b:
                self.logger.warning(
                    f"Failed to fetch candles for {symbol}. "
                    f"{ex_a}: {len(candles_a) if candles_a else 0}, "
                    f"{ex_b}: {len(candles_b) if candles_b else 0}"
                )
                # Fallback: start with empty deque
                self.spread_history[symbol] = deque(maxlen=self.history_length)
                self.last_history_update[symbol] = time.time()
                return
            
            # Convert to DataFrames for easier processing
            df_a = pd.DataFrame(
                candles_a,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            df_b = pd.DataFrame(
                candles_b,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            
            # Align by timestamp (use inner join to ensure matching timestamps)
            df_a['time'] = pd.to_datetime(df_a['timestamp'], unit='ms')
            df_b['time'] = pd.to_datetime(df_b['timestamp'], unit='ms')
            
            df_merged = pd.merge(
                df_a[['time', 'close']],
                df_b[['time', 'close']],
                on='time',
                suffixes=('_a', '_b')
            )
            
            # Calculate historical GROSS spreads: Close_A - Close_B
            df_merged['gross_spread'] = df_merged['close_a'] - df_merged['close_b']
            
            # Populate spread_history with GROSS spreads (market data)
            historical_gross_spreads = df_merged['gross_spread'].tolist()
            self.spread_history[symbol] = deque(
                historical_gross_spreads,
                maxlen=self.history_length
            )
            
            # Set initial update time
            self.last_history_update[symbol] = time.time()
            
            self.logger.info(
                f"✅ Pre-loaded history for {symbol} ({self.history_timeframe}). "
                f"Got {len(self.spread_history[symbol])} spread values. "
                f"Initial Z-Score parameters set."
            )
            
        except Exception as e:
            self.logger.error(f"Error pre-loading history for {symbol}: {e}")
            # Fallback: start with empty deque and build gradually
            self.spread_history[symbol] = deque(maxlen=self.history_length)
            self.last_history_update[symbol] = time.time()
            self.logger.warning(
                f"Starting with empty history for {symbol}. "
                f"Will build baseline slowly."
            )
    
    async def _process_price_updates(self) -> None:
        """
        Continuously process price updates from WebSocket queue.
        """
        queue = self.ws_manager.get_queue()
        
        while self.running:
            try:
                # Get price update with timeout
                data = await asyncio.wait_for(queue.get(), timeout=1.0)
                
                # Update price cache
                exchange = data['exchange']
                symbol = data['symbol']
                
                # Only process supported exchanges (BingX vs Bybit arbitrage)
                if exchange not in self.supported_exchanges:
                    continue
                
                self.price_cache[exchange][symbol] = data
                
                # Emit price update event
                self.event_bus.emit_price_update(data)
                
                # Check if we have prices from both exchanges
                await self._check_arbitrage_opportunity(symbol)
            
            except asyncio.TimeoutError:
                # No message received, continue
                continue
            
            except Exception as e:
                self.logger.error(f"Error processing price update: {e}")
                self.event_bus.emit_error('LiveMonitor', str(e))
    
    async def _check_arbitrage_opportunity(self, symbol: str) -> None:
        """
        Check for arbitrage opportunity when we have prices from both exchanges.
        
        CORRECTED CALCULATION LOGIC:
        - Step A: Calculate gross_spread from live ticks (market data)
        - Step B: Calculate Z-Score using GROSS spread against historical GROSS baseline
        - Step C: Calculate net_spread (gross_spread - fees) for profitability check
        - Step D: Signal ONLY if Z-Score high AND net_spread > 0
        - Step E: Update the historical baseline with GROSS spread periodically
        
        Args:
            symbol: Trading pair symbol
        """
        # Get active pair for this symbol
        pair = self.active_pairs.get(symbol)
        if not pair:
            return
            
        ex_a, ex_b = pair
        
        # Check if we have prices from both exchanges
        price_a = self.price_cache[ex_a].get(symbol)
        price_b = self.price_cache[ex_b].get(symbol)
        
        if not price_a or not price_b:
            return
        
        # === STEP A: CALCULATE GROSS SPREAD ===
        
        # Calculate current executable spread (buy on one, sell on other)
        # Spread = ask_A - bid_B (cost to execute arbitrage)
        spread_a_to_b = price_a['ask'] - price_b['bid']
        spread_b_to_a = price_b['ask'] - price_a['bid']
        
        # Use the more favorable spread (gross spread)
        gross_spread = min(abs(spread_a_to_b), abs(spread_b_to_a))
        if spread_a_to_b < 0:
            gross_spread = -gross_spread  # Negative spread means Exchange A cheaper
        
        # === STEP B: CALCULATE NET SPREAD (CRITICAL) ===
        
        # Calculate mid-price for fee calculation
        mid_price = (price_a['last'] + price_b['last']) / 2.0
        
        # Get fees for both exchanges
        fee_a = self.fees[ex_a]['taker']
        fee_b = self.fees[ex_b]['taker']
        
        # Calculate net spread after fees
        net_spread_val, net_spread_pct, fee_cost = calculate_net_spread(
            gross_spread=abs(gross_spread),
            price=mid_price,
            taker_fee_a=fee_a,
            taker_fee_b=fee_b
        )
        
        # Preserve sign of spread
        if gross_spread < 0:
            net_spread_val = -net_spread_val
            net_spread_pct = -net_spread_pct
        
        # Calculate gross spread percentage for display
        gross_spread_pct = (abs(gross_spread) / mid_price) * 100 if mid_price > 0 else 0.0
        
        
        # === STEP B: CALCULATE Z-SCORE ON GROSS SPREAD ===
        
        # Check if we have historical baseline
        if symbol not in self.spread_history or len(self.spread_history[symbol]) < 10:
            # Not enough history yet, skip Z-Score calculation
            return
        
        # Calculate Z-Score from historical baseline using GROSS SPREAD
        try:
            # Get mean and std from historical baseline (which contains GROSS spreads)
            gross_spreads = list(self.spread_history[symbol])
            mean = sum(gross_spreads) / len(gross_spreads)
            
            # Calculate standard deviation
            variance = sum((x - mean) ** 2 for x in gross_spreads) / len(gross_spreads)
            std_dev = variance ** 0.5
            
            # Handle zero standard deviation
            if std_dev == 0:
                self.logger.debug(f"{symbol}: Zero std deviation, setting Z-Score to 0")
                z_score = 0.0
            else:
                # CORRECTED: Calculate Z-Score using GROSS SPREAD (market anomaly)
                # Z-Score = (current_gross_spread - baseline_mean) / baseline_std
                z_score = (gross_spread - mean) / std_dev
                self.logger.debug(
                    f"{symbol}: Z-Score={z_score:.2f}, "
                    f"gross_spread={gross_spread:.4f}, mean={mean:.4f}, std={std_dev:.4f}"
                )
            
            # Emit comprehensive spread update with all values
            self.event_bus.spread_updated.emit({
                'symbol': symbol,
                'gross_spread': gross_spread,
                'gross_spread_pct': gross_spread_pct,
                'fee_cost': fee_cost,
                'fee_pct': (fee_a + fee_b) * 100,
                'net_spread': net_spread_val,
                'net_spread_pct': net_spread_pct,
                'z_score': z_score,
                'mid_price': mid_price,
                'exchanges': pair
            })
            
            # Check for entry/exit signals (requires BOTH high Z-Score AND positive net spread)
            await self._check_signals(symbol, z_score, net_spread_val, net_spread_pct)
            
        except ZeroDivisionError:
            self.logger.warning(f"{symbol}: ZeroDivisionError in Z-Score calculation")
            z_score = 0.0
        except Exception as e:
            self.logger.error(f"{symbol}: Error calculating Z-Score: {e}")
            return
        
        # === STEP E: HISTORY MAINTENANCE ===
        
        # Update historical baseline based on timeframe interval
        current_time = time.time()
        time_since_update = current_time - self.last_history_update.get(symbol, 0)
        
        if time_since_update >= self.history_update_interval:
            # CORRECTED: Add current GROSS spread to history (market data)
            # Z-Score measures market anomaly, not profitability
            self.spread_history[symbol].append(gross_spread)
            self.last_history_update[symbol] = current_time
            
            self.logger.debug(
                f"{symbol}: Updated historical baseline with GROSS spread "
                f"(size={len(self.spread_history[symbol])})"
            )
    
    async def _check_signals(self, symbol: str, z_score: float, net_spread_val: float, net_spread_pct: float) -> None:
        """
        Check if entry or exit signal conditions are met using SIGNAL PERSISTENCE.
        
        Args:
            symbol: Trading pair symbol
            z_score: Current Z-Score
            net_spread_val: Net spread value
            net_spread_pct: Net spread percentage
        """
        z_entry = self.config['trading']['z_score_entry']
        z_exit = self.config['trading']['z_score_exit']
        
        # Get persistence config (defaults: 3 ticks for entry, 5 for exit to be safe)
        min_entry_ticks = self.config.get('trading', {}).get('min_entry_ticks', 3)
        min_exit_ticks = self.config.get('trading', {}).get('min_exit_ticks', 5)
        
        # Initialize counters for symbol if needed
        if symbol not in self.signal_counters:
            self.signal_counters[symbol] = {'entry': 0, 'exit': 0}
            
        if symbol not in self.in_position:
            self.in_position[symbol] = False
        
        # === SIGNAL LOGIC ===
        
        # 1. ENTRY CONDITION
        min_spread = self.config.get('trading', {}).get('min_spread_pct', 0.003) * 100 # Convert to %
        
        is_entry_condition = (not self.in_position[symbol] and 
                            abs(z_score) > z_entry and 
                            net_spread_pct > min_spread)
        
        # 2. EXIT CONDITION
        is_exit_condition = (self.in_position[symbol] and 
                           abs(z_score) < z_exit)

        # Update Counters
        if is_entry_condition:
            self.signal_counters[symbol]['entry'] += 1
            self.signal_counters[symbol]['exit'] = 0  # Reset exit counter
        elif is_exit_condition:
            self.signal_counters[symbol]['exit'] += 1
            self.signal_counters[symbol]['entry'] = 0 # Reset entry counter
        else:
            # Noise / unstable state - reset both
            if self.signal_counters[symbol]['entry'] > 0 or self.signal_counters[symbol]['exit'] > 0:
                self.logger.debug(f"{symbol}: Signal lost stability. Resetting counters.")
            self.signal_counters[symbol]['entry'] = 0
            self.signal_counters[symbol]['exit'] = 0

        # === TRIGGER ACTION ===
        
        # Check Entry Trigger
        if self.signal_counters[symbol]['entry'] >= min_entry_ticks and not self.in_position[symbol]:
            self.in_position[symbol] = True
            ex_a, ex_b = self.active_pairs[symbol]
            self.event_bus.emit_signal_triggered(symbol, 'ENTRY', z_score, ex_a, ex_b)
            
            self.logger.info(
                f"🔔 ENTRY SIGNAL: {symbol} | Z-Score={z_score:.2f} | "
                f"Net Spread={net_spread_pct:.3f}% | "
                f"Confirmed for {self.signal_counters[symbol]['entry']} ticks"
            )
            # Reset counter after action to prevent double firing? 
            # Actually, keep it high or reset? 
            # Resetting is safer to prevent immediate re-trigger if logic loops.
            self.signal_counters[symbol]['entry'] = 0

        # Check Exit Trigger
        elif self.signal_counters[symbol]['exit'] >= min_exit_ticks and self.in_position[symbol]:
            self.in_position[symbol] = False
            ex_a, ex_b = self.active_pairs[symbol]
            self.event_bus.emit_signal_triggered(symbol, 'EXIT', z_score, ex_a, ex_b)
            
            self.logger.info(
                f"🔔 EXIT SIGNAL: {symbol} | Z-Score={z_score:.2f} | "
                f"Confirmed for {self.signal_counters[symbol]['exit']} ticks. "
                f"[Audit: NetSpread={net_spread_pct:.3f}%]"
            )
            self.signal_counters[symbol]['exit'] = 0

    
    async def start(self, symbols: List[str], pair: tuple = ('bingx', 'bybit')) -> None:
        """
        Start live monitoring for given symbols with specified exchange pair.
        Supports dynamic addition of symbols if already running.
        
        Args:
            symbols: List of trading pair symbols to monitor
            pair: Tuple of exchange IDs (ex_a, ex_b)
        """
        # Store active pair for these symbols
        for symbol in symbols:
            self.active_pairs[symbol] = pair

        # If already running, just add new symbols dynamically
        if self.running:
            self.logger.info(f"LiveMonitor already running. Dynamically adding {len(symbols)} symbols: {symbols} on {pair}")
            
            # Pre-load history for new symbols
            for symbol in symbols:
                await self._preload_history(symbol, ex_a=pair[0], ex_b=pair[1])
            
            # Subscribe dynamically on specific exchanges
            await self.ws_manager.subscribe(symbols, exchanges=list(pair))
            return

        self.running = True
        self.logger.info(f"Starting LiveMonitor for {len(symbols)} symbols on {pair}")
        
        # Pre-load historical data for all symbols
        for symbol in symbols:
            await self._preload_history(symbol, ex_a=pair[0], ex_b=pair[1])
        
        # Start WebSocket manager with specific exchanges
        await self.ws_manager.start(symbols, exchanges=list(pair))
        
        # Start price processing task
        self.monitor_task = asyncio.create_task(self._process_price_updates())
        
        self.logger.info("LiveMonitor started with hybrid Z-Score calculation")
    
    async def stop(self) -> None:
        """
        Stop live monitoring and cleanup resources.
        """
        self.logger.info("Stopping LiveMonitor...")
        self.running = False
        
        # Stop WebSocket manager
        await self.ws_manager.stop()
        
        # Cancel monitor task
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        # Close all CCXT exchanges
        for client in self.exchange_clients.values():
            await client.close()
        
        # Clear buffers
        self.spread_history.clear()
        self.price_cache = {ex: {} for ex in self.supported_exchanges}
        self.last_history_update.clear()
        self.active_pairs.clear()
        self.in_position.clear()
        self.signal_counters.clear()
        
        self.logger.info("LiveMonitor stopped")
    
    def get_current_stats(self, symbol: str) -> Optional[Dict]:
        """
        Get current statistics for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dictionary with current stats or None
        """
        if symbol not in self.spread_history:
            return None
        
        if len(self.spread_history[symbol]) < 10:
            return None
        
        # Get active pair
        pair = self.active_pairs.get(symbol)
        if not pair:
            return None
        ex_a, ex_b = pair

        # Get current prices
        price_a = self.price_cache[ex_a].get(symbol)
        price_b = self.price_cache[ex_b].get(symbol)
        
        if not price_a or not price_b:
            return None
        
        # Calculate current spread
        spread_a_to_b = price_a['ask'] - price_b['bid']
        spread_b_to_a = price_b['ask'] - price_a['bid']
        gross_spread = min(abs(spread_a_to_b), abs(spread_b_to_a))
        if spread_a_to_b < 0:
            gross_spread = -gross_spread
        
        # Calculate mid-price and net spread
        mid_price = (price_a['last'] + price_b['last']) / 2.0
        net_spread_val, net_spread_pct, fee_cost = calculate_net_spread(
            gross_spread=abs(gross_spread),
            price=mid_price,
            taker_fee_a=self.fees[ex_a]['taker'],
            taker_fee_b=self.fees[ex_b]['taker']
        )
        if gross_spread < 0:
            net_spread_val = -net_spread_val
            net_spread_pct = -net_spread_pct
        
        # Calculate Z-Score from historical baseline (GROSS spreads)
        spreads = list(self.spread_history[symbol])
        mean = sum(spreads) / len(spreads)
        variance = sum((x - mean) ** 2 for x in spreads) / len(spreads)
        std_dev = variance ** 0.5
        
        if std_dev == 0:
            z_score = 0.0
        else:
            # CORRECTED: Use gross spread for Z-Score
            z_score = (gross_spread - mean) / std_dev
        
        return {
            'symbol': symbol,
            'gross_spread': gross_spread,
            'net_spread': net_spread_val,
            'net_spread_pct': net_spread_pct,
            'fee_cost': fee_cost,
            'z_score': z_score,
            'in_position': self.in_position.get(symbol, False),
            'history_length': len(self.spread_history[symbol]),
            'baseline_mean': mean,
            'baseline_std': std_dev,
            'mid_price': mid_price,
            'spread': gross_spread  # Alias for stats loop
        }


async def main():
    """CLI interface for testing LiveMonitor."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Live Arbitrage Monitor (Hybrid Z-Score)')
    parser.add_argument(
        'symbols',
        nargs='*',
        default=['BTC/USDT'],
        help='Symbols to monitor (default: BTC/USDT)'
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to config file'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Stats display interval in seconds (default: 5)'
    )
    
    args = parser.parse_args()
    
    # Create monitor
    monitor = LiveMonitor(config_path=args.config)
    
    # Setup graceful shutdown
    shutdown_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        print("\n\nReceived shutdown signal...")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start monitoring (includes pre-loading history)
    await monitor.start(args.symbols)
    
    print(f"\n{'='*70}")
    print(f"Live Arbitrage Monitor (Hybrid Z-Score) - {', '.join(args.symbols)}")
    print(f"{'='*70}\n")
    print("Pre-loaded historical baseline. Monitoring live ticks...\n")
    
    # Stats display loop
    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(args.interval)
            
            print(f"\n{datetime.now().strftime('%H:%M:%S')} - Current Stats:")
            print(f"{'-'*70}")
            
            for symbol in args.symbols:
                stats = monitor.get_current_stats(symbol)
                
                if stats:
                    position_indicator = "📈 IN POSITION" if stats['in_position'] else "⏸️  WAITING"
                    
                    print(
                        f"{symbol:12} | "
                        f"Gross Spread: {stats['gross_spread']:8.2f} | "
                        f"Z-Score: {stats['z_score']:6.2f} | "
                        f"Baseline μ={stats['baseline_mean']:.2f} σ={stats['baseline_std']:.2f} | "
                        f"{position_indicator}"
                    )
                else:
                    history_len = len(monitor.spread_history.get(symbol, deque()))
                    print(f"{symbol:12} | Building baseline... ({history_len}/60 samples)")
            
            print(f"{'-'*70}")
    
    except Exception as e:
        print(f"\nError: {e}")
    
    finally:
        print("\nShutting down...")
        await monitor.stop()
        print("Stopped.")


if __name__ == '__main__':
    asyncio.run(main())
