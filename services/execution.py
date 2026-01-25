"""
Execution Engine

Manages arbitrage trade execution with rollback protection.
Works with both Paper and Real exchange clients via dependency injection.
"""

import asyncio
from typing import Dict, Optional, Any
from datetime import datetime

from core.interfaces.exchange import BaseExchange
from core.event_bus import EventBus
from core.exchange_factory import create_exchange_client
from utils.logger import get_logger
from utils.config import get_config


class ExecutionEngine:
    """
    Core trading logic for arbitrage execution.
    
    Features:
    - Exchange-agnostic via BaseExchange interface
    - Atomic-like execution with rollback on failure
    - Event-driven signal handling
    - Position tracking and P&L calculation
    """
    
    def __init__(
        self,
        config_path: str = 'config/config.yaml',
        ws_manager: Optional[Any] = None,
        position_size_usdt: float = 100.0,
        max_positions: int = 5
    ):
        """
        Initialize Multi-Exchange Execution Engine.
        
        Args:
            config_path: Path to configuration
            ws_manager: WebSocketManager (required for Paper mode)
            position_size_usdt: Fixed position size in USDT
            max_positions: Maximum concurrent positions
        """
        self.config_path = config_path
        self.config = get_config(config_path)
        self.ws_manager = ws_manager
        self.position_size_usdt = position_size_usdt
        self.max_positions = max_positions
        
        self.logger = get_logger(__name__)
        self.event_bus = EventBus.instance()
        
        # State tracking
        self.is_busy = False
        self.active_trades: Dict[str, Dict] = {}  # symbol -> trade metadata
        self.clients: Dict[str, BaseExchange] = {} # exchange_name -> client
        self.total_trades = 0
        self.cumulative_pnl = 0.0
        
        # Determine mode
        self.mode = self.config.get('trading', {}).get('mode', 'PAPER')
        
        # Subscribe to trading signals
        self.event_bus.signal_triggered.connect(self._handle_signal)
        
        self.logger.info(
            f"🚀 Multi-Exchange ExecutionEngine initialized ({self.mode} mode). "
            f"Position Size: ${position_size_usdt}"
        )
    
    def _get_client(self, exchange_name: str) -> BaseExchange:
        """Get or create exchange client."""
        if exchange_name not in self.clients:
            self.logger.info(f"Creating {exchange_name} client for {self.mode} mode...")
            self.clients[exchange_name] = create_exchange_client(
                exchange_name, self.config, self.mode, self.ws_manager
            )
        return self.clients[exchange_name]

    def _handle_signal(self, symbol: str, signal_type: str, z_score: float, ex_a: str = '', ex_b: str = '') -> None:
        """
        Handle trading signal from EventBus.
        """
        # Run async handler in event loop
        asyncio.create_task(self._process_signal(symbol, signal_type, z_score, ex_a, ex_b))
    
    async def _process_signal(
        self,
        symbol: str,
        signal_type: str,
        z_score: float,
        ex_a: str = '',
        ex_b: str = ''
    ) -> None:
        """
        Process trading signal asynchronously.
        """
        try:
            if not ex_a or not ex_b:
                # Fallback to defaults if not provided (old signal format)
                ex_a, ex_b = 'bingx', 'bybit'

            if signal_type == 'ENTRY':
                await self.execute_arb_entry(symbol, z_score, ex_a, ex_b)
            elif signal_type == 'EXIT':
                await self.execute_arb_exit(symbol)
        except Exception as e:
            self.logger.error(f"Error processing {signal_type} signal for {symbol}: {e}")
            self.event_bus.emit_error('ExecutionEngine', str(e))
    
    async def execute_arb_entry(self, symbol: str, z_score: float, ex_a: str, ex_b: str) -> bool:
        """
        Execute arbitrage entry with rollback protection.
        
        Strategy:
        - If Z-Score > 0: Buy on A (cheaper), Sell on B (expensive)
        - If Z-Score < 0: Sell on A (expensive), Buy on B (cheaper)
        
        Args:
            symbol: Trading pair
            z_score: Z-Score value (determines direction)
        
        Returns:
            True if successful, False otherwise
        """
        # Check if already busy
        if self.is_busy:
            self.logger.warning(f"⏸️  Execution busy, skipping {symbol}")
            return False
        
        # Check if already have position
        if symbol in self.active_trades:
            self.logger.warning(f"⏸️  Already have position in {symbol}")
            return False
        
        # Check position limit
        if len(self.active_trades) >= self.max_positions:
            self.logger.warning(f"⏸️  Max positions reached ({self.max_positions})")
            return False
        
        self.is_busy = True
        
        try:
            # Get clients
            client_a = self._get_client(ex_a)
            client_b = self._get_client(ex_b)

            # Check balances
            balance_a = await client_a.get_balance()
            balance_b = await client_b.get_balance()
            
            free_a = balance_a.get('USDT', {}).get('free', 0)
            free_b = balance_b.get('USDT', {}).get('free', 0)
            
            if free_a < self.position_size_usdt or free_b < self.position_size_usdt:
                self.logger.error(
                    f"❌ Insufficient balance: {ex_a}=${free_a:.2f}, {ex_b}=${free_b:.2f}"
                )
                return False
            
            # Get current prices
            ticker_a = await client_a.fetch_ticker(symbol)
            ticker_b = await client_b.fetch_ticker(symbol)
            
            # Calculate position size in base currency
            # Use mid price for sizing
            avg_price = (ticker_a['last'] + ticker_b['last']) / 2
            amount = self.position_size_usdt / avg_price
            
            # Determine trade direction based on Z-Score
            # Z-Score is calculated in LiveMonitor as: (gross_spread - mean) / std
            # Where gross_spread is negative if Exchange A is cheaper.
            # So: Z-Score < 0 means Exchange A is undervalued (Cheaper) -> BUY A, SELL B
            
            if z_score < 0:
                # A is cheaper, B is expensive
                side_a = 'buy'
                side_b = 'sell'
                entry_spread = ticker_b['bid'] - ticker_a['ask']
            else:
                # A is expensive, B is cheaper
                side_a = 'sell'
                side_b = 'buy'
                entry_spread = ticker_a['bid'] - ticker_b['ask']
            
            self.logger.info(
                f"🚀 Opening arbitrage: {symbol}, Z-Score={z_score:.2f}, "
                f"Spread=${entry_spread:.2f}, Size={amount:.6f}"
            )
            
            # ATOMIC EXECUTION WITH ROLLBACK
            order_a = None
            order_b = None
            
            try:
                # First leg
                order_a = await client_a.create_order(
                    symbol=symbol,
                    side=side_a,
                    amount=amount
                )
                
                self.logger.info(
                    f"✅ Leg A complete: {side_a.upper()} on {ex_a}"
                )
                
                # Second leg
                try:
                    order_b = await client_b.create_order(
                        symbol=symbol,
                        side=side_b,
                        amount=amount
                    )
                    
                    self.logger.info(
                        f"✅ Leg B complete: {side_b.upper()} on {ex_b}"
                    )
                
                except Exception as e:
                    # CRITICAL: Second leg failed, rollback first leg
                    self.logger.error(
                        f"🚨 Second leg failed: {e}. Rolling back first leg..."
                    )
                    
                    try:
                        await client_a.close_position(symbol)
                        self.logger.warning("⚠️  Rollback successful")
                    except Exception as rollback_error:
                        self.logger.error(
                            f"💀 ROLLBACK FAILED: {rollback_error}. "
                            f"MANUAL INTERVENTION REQUIRED!"
                        )
                    
                    raise Exception(f"Arbitrage entry failed - rolled back: {e}")
            
            except Exception as e:
                self.logger.error(f"❌ Entry failed: {e}")
                return False
            
            # Store trade metadata
            # Store trade metadata
            trade_data = {
                'symbol': symbol,
                'entry_time': datetime.now().isoformat(),
                'entry_z_score': z_score,
                'entry_spread': entry_spread,
                'order_a_id': order_a['id'],
                'order_b_id': order_b['id'],
                'side_a': side_a,
                'side_b': side_b,
                'amount': amount,
                'ex_a': ex_a,
                'ex_b': ex_b,
                'entry_price_a': order_a.get('average', 0),
                'entry_price_b': order_b.get('average', 0)
            }
            self.active_trades[symbol] = trade_data
            
            # Emit signal for GUI
            self.event_bus.emit_trade_opened(trade_data)
            
            self.logger.info(
                f"✅ Arbitrage opened: {symbol} "
                f"({ex_a} {side_a.upper()} / "
                f"{ex_b} {side_b.upper()})"
            )
            
            self.total_trades += 1
            self.logger.info(f"📊 Total trades opened: {self.total_trades}")
            
            return True
        
        except Exception as e:
            self.logger.error(f"❌ Arbitrage entry error: {e}")
            return False
        
        finally:
            self.is_busy = False
    
    async def execute_arb_exit(self, symbol: str) -> bool:
        """
        Execute arbitrage exit.
        
        Args:
            symbol: Trading pair
        
        Returns:
            True if successful, False otherwise
        """
        if symbol not in self.active_trades:
            self.logger.warning(f"⏸️  No active trade for {symbol}")
            return False
        
        self.is_busy = True
        
        try:
            trade = self.active_trades[symbol]
            
            self.logger.info(f"🔄 Closing arbitrage: {symbol}")
            
            client_a = self._get_client(trade['ex_a'])
            client_b = self._get_client(trade['ex_b'])

            # Close both positions
            close_a = await client_a.close_position(symbol)
            close_b = await client_b.close_position(symbol)
            
            # Calculate total P&L
            pnl_a = close_a.get('pnl', 0)
            pnl_b = close_b.get('pnl', 0)
            total_pnl = pnl_a + pnl_b
            
            # Calculate holding time
            entry_time = datetime.fromisoformat(trade['entry_time'])
            holding_time = (datetime.now() - entry_time).total_seconds()
            
            # Update cumulative stats
            self.cumulative_pnl += total_pnl
            
            self.logger.info(
                f"✅ Arbitrage closed: {symbol}, "
                f"P&L=${total_pnl:.2f}, "
                f"Holding Time={holding_time:.0f}s"
            )
            self.logger.info(f"💰 Cumulative P&L: ${self.cumulative_pnl:.2f}")
            
            # Emit signal for GUI
            self.event_bus.emit_trade_closed({
                'symbol': symbol,
                'pnl': total_pnl,
                'holding_time': holding_time,
                'exit_time': datetime.now().isoformat()
            })
            
            # Remove from active trades
            del self.active_trades[symbol]
            
            return True
        
        except Exception as e:
            self.logger.error(f"❌ Arbitrage exit error: {e}")
            return False
        
        finally:
            self.is_busy = False
    
    async def get_active_positions(self) -> Dict[str, Dict]:
        """
        Get all active arbitrage positions.
        
        Returns:
            Dictionary of symbol -> trade metadata
        """
        return self.active_trades.copy()
    
    async def emergency_close_all(self) -> None:
        """
        Emergency close all positions.
        
        Use this for risk management or shutdown.
        """
        self.logger.warning("🚨 EMERGENCY CLOSE ALL POSITIONS")
        
        for symbol in list(self.active_trades.keys()):
            try:
                await self.execute_arb_exit(symbol)
            except Exception as e:
                self.logger.error(f"Failed to close {symbol}: {e}")
