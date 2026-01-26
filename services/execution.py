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
            f"Base Position Size: ${position_size_usdt}"
        )

    async def _calculate_adaptive_size(self, symbol: str, client_a: BaseExchange, client_b: BaseExchange, side_a: str, side_b: str) -> float:
        """
        Calculate safe amount based on order book depth to minimize slippage.
        """
        try:
            # 1. Get order books from both exchanges
            book_a = await client_a.fetch_order_book(symbol, limit=20)
            book_b = await client_b.fetch_order_book(symbol, limit=20)
            
            # The execution config is under trading.execution in config.yaml
            exec_cfg = self.config.get('trading', {}).get('execution', {})
            max_slippage = exec_cfg.get('max_slippage_pct', 0.002)
            depth_factor = exec_cfg.get('liquidity_depth_factor', 0.1)
            min_depth_required = exec_cfg.get('min_depth_usdt', 200.0)
            
            def get_safe_volume(targets, side, slippage_limit):
                if not targets: return 0.0
                best_price = targets[0][0]
                # If buying, we can go up to best_price * (1 + limit)
                # If selling, we can go down to best_price * (1 - limit)
                is_buy = (side == 'buy')
                limit_price = best_price * (1 + slippage_limit) if is_buy else best_price * (1 - slippage_limit)
                
                safe_vol_usdt = 0
                for price, amount in targets:
                    if (is_buy and price <= limit_price) or (not is_buy and price >= limit_price):
                        safe_vol_usdt += price * amount
                    else:
                        break
                return safe_vol_usdt
            
            # 2. Determine which side of the book we care about
            # For BUY A, we look at ASKS on A. For SELL B, we look at BIDS on B.
            side_a_targets = book_a['asks'] if side_a == 'buy' else book_a['bids']
            side_b_targets = book_b['asks'] if side_b == 'buy' else book_b['bids']
            
            # 3. Calculate depth available within slippage limit on each exchange
            depth_a = get_safe_volume(side_a_targets, side_a, max_slippage)
            depth_b = get_safe_volume(side_b_targets, side_b, max_slippage)
            
            self.logger.debug(f"🔍 Depth check for {symbol}: A (${side_a})=${depth_a:.0f}, B (${side_b})=${depth_b:.0f}")
            
            # 4. Minimum depth check
            if depth_a < min_depth_required or depth_b < min_depth_required:
                self.logger.warning(f"⚠️ Insufficient depth for {symbol}: A=${depth_a:.0f}, B=${depth_b:.0f} (Min=${min_depth_required})")
                return 0.0
            
            # 5. Calculate adaptive amount
            # Use only a fraction of available depth for safety
            safe_amount_usdt = min(depth_a, depth_b) * depth_factor
            
            # Final amount is min(safe_amount, default_position_size)
            final_amount_usdt = min(safe_amount_usdt, self.position_size_usdt)
            
            if final_amount_usdt < self.position_size_usdt:
                self.logger.info(f"⚖️ Adaptive sizing: Reduced {symbol} from ${self.position_size_usdt} to ${final_amount_usdt:.2f} due to depth")
            
            return final_amount_usdt
            
        except Exception as e:
            self.logger.error(f"Error calculating adaptive size: {e}")
            return 0.0

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
        Execute arbitrage entry with adaptive sizing and depth protection.
        """
        if self.is_busy:
            self.logger.warning(f"⏸️  Execution busy, skipping {symbol}")
            return False
            
        if symbol in self.active_trades:
            self.logger.warning(f"⏸️  Already have position in {symbol}")
            return False
            
        if len(self.active_trades) >= self.max_positions:
            self.logger.warning(f"⏸️  Max positions reached ({self.max_positions})")
            return False
            
        self.is_busy = True
        
        try:
            # 1. Setup clients and directions
            client_a = self._get_client(ex_a)
            client_b = self._get_client(ex_b)
            
            if z_score < 0:
                side_a, side_b = 'buy', 'sell'
            else:
                side_a, side_b = 'sell', 'buy'
                
            # 2. Calculate adaptive size based on depth
            trade_amount_usdt = await self._calculate_adaptive_size(
                symbol, client_a, client_b, side_a, side_b
            )
            
            if trade_amount_usdt <= 0:
                self.logger.warning(f"⚠️ Skipping {symbol} due to insufficient liquidity/size")
                return False

            # 3. Check balances against the actual trade amount
            balance_a = await client_a.get_balance()
            balance_b = await client_b.get_balance()
            
            free_a = balance_a.get('USDT', {}).get('free', 0)
            free_b = balance_b.get('USDT', {}).get('free', 0)
            
            if free_a < trade_amount_usdt or free_b < trade_amount_usdt:
                self.logger.error(
                    f"❌ Insufficient balance for reduced size (${trade_amount_usdt:.2f}): "
                    f"{ex_a}=${free_a:.2f}, {ex_b}=${free_b:.2f}"
                )
                return False
            
            # 4. Get current prices for amount calculation
            ticker_a = await client_a.fetch_ticker(symbol)
            ticker_b = await client_b.fetch_ticker(symbol)
            
            avg_price = (ticker_a['last'] + ticker_b['last']) / 2
            amount = trade_amount_usdt / avg_price
            
            if side_a == 'buy':
                entry_spread = ticker_b['bid'] - ticker_a['ask']
            else:
                entry_spread = ticker_a['bid'] - ticker_b['ask']
            
            self.logger.info(
                f"🚀 Opening adaptive arbitrage: {symbol}, Z-Score={z_score:.2f}, "
                f"Spread=${entry_spread:.2f}, Amount=${trade_amount_usdt:.2f} ({amount:.6f} size)"
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
