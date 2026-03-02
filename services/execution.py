"""
Execution Engine

Manages arbitrage trade execution with rollback protection,
stop-loss, max-loss circuit breaker, and adaptive sizing.
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
    - Stop-loss and max daily loss protection
    - asyncio.Lock for concurrency safety
    """
    
    def __init__(
        self,
        config_path: str = 'config/config.yaml',
        ws_manager: Optional[Any] = None,
        position_size_usdt: float = 100.0,
        max_positions: int = 5
    ):
        self.config_path = config_path
        self.config = get_config(config_path)
        self.ws_manager = ws_manager
        self.max_positions = max_positions
        
        self.logger = get_logger(__name__)
        self.event_bus = EventBus.instance()
        
        # [M4 FIX] Read position_size from config, fallback to parameter
        self.position_size_usdt = self.config.get('trading', {}).get(
            'position_size_usdt', position_size_usdt
        )
        
        # [H1 FIX] Use asyncio.Lock instead of bare boolean
        self._execution_lock = asyncio.Lock()
        
        # State tracking
        self.active_trades: Dict[str, Dict] = {}  # symbol -> trade metadata
        self.clients: Dict[str, BaseExchange] = {} # exchange_name -> client
        self.total_trades = 0
        self.cumulative_pnl = 0.0
        self.daily_pnl = 0.0
        self._daily_reset_date = datetime.now().date()
        
        # Determine mode
        self.mode = self.config.get('trading', {}).get('mode', 'PAPER')
        
        # [C4] Risk management parameters
        risk_cfg = self.config.get('trading', {}).get('risk', {})
        self.max_holding_time = risk_cfg.get('max_holding_time_seconds', 3600)
        self.max_loss_per_trade = risk_cfg.get('max_loss_per_trade_usdt', 5.0)
        self.max_daily_loss = risk_cfg.get('max_daily_loss_usdt', 20.0)
        
        # Risk monitor task
        self._risk_monitor_task: Optional[asyncio.Task] = None
        
        # Subscribe to trading signals
        self.event_bus.signal_triggered.connect(self._handle_signal)
        
        self.logger.info(
            f"🚀 ExecutionEngine initialized ({self.mode} mode). "
            f"Position: ${self.position_size_usdt}, MaxPos: {self.max_positions}, "
            f"StopLoss: ${self.max_loss_per_trade}, MaxDailyLoss: ${self.max_daily_loss}"
        )

    async def start_risk_monitor(self):
        """Start background risk monitoring task."""
        if self._risk_monitor_task is None or self._risk_monitor_task.done():
            self._risk_monitor_task = asyncio.create_task(self._risk_monitor_loop())
            self.logger.info("🛡️ Risk monitor started")

    async def _risk_monitor_loop(self):
        """
        [C4] Background loop that checks open positions for:
        - Max holding time exceeded
        - Per-trade loss limit exceeded
        - Daily loss limit exceeded (circuit breaker)
        """
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Reset daily P&L at midnight
                today = datetime.now().date()
                if today != self._daily_reset_date:
                    self.logger.info(f"📅 Daily P&L reset: ${self.daily_pnl:.2f} -> $0.00")
                    self.daily_pnl = 0.0
                    self._daily_reset_date = today
                
                # Circuit breaker: stop trading if daily loss exceeded
                if self.daily_pnl < -self.max_daily_loss:
                    self.logger.error(
                        f"🚨 CIRCUIT BREAKER: Daily loss ${self.daily_pnl:.2f} "
                        f"exceeds limit -${self.max_daily_loss}"
                    )
                    # Don't close positions — just stop opening new ones
                    # (max_daily_loss check is in execute_arb_entry)
                    continue
                
                # Check each open position
                for symbol in list(self.active_trades.keys()):
                    trade = self.active_trades.get(symbol)
                    if not trade:
                        continue
                    
                    # 1. Check holding time
                    entry_time = datetime.fromisoformat(trade['entry_time'])
                    holding_secs = (datetime.now() - entry_time).total_seconds()
                    
                    if holding_secs > self.max_holding_time:
                        self.logger.warning(
                            f"⏰ Max holding time exceeded for {symbol} "
                            f"({holding_secs:.0f}s > {self.max_holding_time}s). Force closing."
                        )
                        await self.execute_arb_exit(symbol)
                        continue
                    
                    # 2. Check unrealized P&L (approximate)
                    try:
                        client_a = self._get_client(trade['ex_a'])
                        client_b = self._get_client(trade['ex_b'])
                        
                        ticker_a = await client_a.fetch_ticker(symbol)
                        ticker_b = await client_b.fetch_ticker(symbol)
                        
                        # Calculate unrealized P&L based on entry prices
                        # Leg A PnL
                        if trade['side_a'] == 'buy':
                            close_price_a = ticker_a['bid']
                            pnl_a = (close_price_a - trade['entry_price_a']) * trade['amount']
                        else:
                            close_price_a = ticker_a['ask']
                            pnl_a = (trade['entry_price_a'] - close_price_a) * trade['amount']
                            
                        # Leg B PnL
                        if trade['side_b'] == 'buy':
                            close_price_b = ticker_b['bid']
                            pnl_b = (close_price_b - trade['entry_price_b']) * trade['amount']
                        else:
                            close_price_b = ticker_b['ask']
                            pnl_b = (trade['entry_price_b'] - close_price_b) * trade['amount']
                            
                        # Approximate Fee costs
                        fee_a = (close_price_a * trade['amount']) * getattr(client_a, 'fee_rate', 0.0006)
                        fee_b = (close_price_b * trade['amount']) * getattr(client_b, 'fee_rate', 0.0006)
                        
                        unrealized_pnl = pnl_a + pnl_b - fee_a - fee_b
                        
                        if unrealized_pnl < -self.max_loss_per_trade:
                            self.logger.warning(
                                f"🛑 Stop-loss triggered for {symbol}: "
                                f"Unrealized P&L=${unrealized_pnl:.2f} < -${self.max_loss_per_trade}"
                            )
                            await self.execute_arb_exit(symbol)
                    except Exception as e:
                        self.logger.debug(f"Risk check error for {symbol}: {e}")
                        
            except asyncio.CancelledError:
                self.logger.info("🛡️ Risk monitor stopped")
                return
            except Exception as e:
                self.logger.error(f"Risk monitor error: {e}")

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
            side_a_targets = book_a['asks'] if side_a == 'buy' else book_a['bids']
            side_b_targets = book_b['asks'] if side_b == 'buy' else book_b['bids']
            
            # 3. Calculate depth available within slippage limit on each exchange
            depth_a = get_safe_volume(side_a_targets, side_a, max_slippage)
            depth_b = get_safe_volume(side_b_targets, side_b, max_slippage)
            
            self.logger.debug(f"🔍 Depth check for {symbol}: A ({side_a})=${depth_a:.0f}, B ({side_b})=${depth_b:.0f}")
            
            # 4. Minimum depth check
            if depth_a < min_depth_required or depth_b < min_depth_required:
                self.logger.warning(f"⚠️ Insufficient depth for {symbol}: A=${depth_a:.0f}, B=${depth_b:.0f} (Min=${min_depth_required})")
                return 0.0
            
            # 5. Calculate adaptive amount
            safe_amount_usdt = min(depth_a, depth_b) * depth_factor
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
        """Handle trading signal from EventBus."""
        asyncio.create_task(self._process_signal(symbol, signal_type, z_score, ex_a, ex_b))
    
    async def _process_signal(
        self,
        symbol: str,
        signal_type: str,
        z_score: float,
        ex_a: str = '',
        ex_b: str = ''
    ) -> None:
        """Process trading signal asynchronously."""
        try:
            if not ex_a or not ex_b:
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
        Uses asyncio.Lock for concurrency safety.
        """
        # [H1 FIX] Use lock instead of bare boolean
        if self._execution_lock.locked():
            self.logger.warning(f"⏸️  Execution busy, skipping {symbol}")
            return False
            
        if symbol in self.active_trades:
            self.logger.warning(f"⏸️  Already have position in {symbol}")
            return False
            
        if len(self.active_trades) >= self.max_positions:
            self.logger.warning(f"⏸️  Max positions reached ({self.max_positions})")
            return False
        
        # [C4] Circuit breaker check
        if self.daily_pnl < -self.max_daily_loss:
            self.logger.error(
                f"🚨 Circuit breaker active: daily loss ${self.daily_pnl:.2f}. "
                f"Rejecting {symbol}."
            )
            return False
        
        async with self._execution_lock:
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
                        f"❌ Insufficient balance (${trade_amount_usdt:.2f}): "
                        f"{ex_a}=${free_a:.2f}, {ex_b}=${free_b:.2f}"
                    )
                    return False
                
                # 4. Get current prices for amount calculation
                ticker_a = await client_a.fetch_ticker(symbol)
                ticker_b = await client_b.fetch_ticker(symbol)
                
                avg_price = (ticker_a['last'] + ticker_b['last']) / 2
                if avg_price <= 0:
                    self.logger.error(f"❌ Invalid price for {symbol}: {avg_price}")
                    return False
                
                amount = trade_amount_usdt / avg_price
                
                # [H2] Minimum order size check
                # TODO: Validate against exchange.markets[symbol]['limits'] when available
                if amount <= 0:
                    self.logger.error(f"❌ Calculated amount is zero for {symbol}")
                    return False
                
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
                        # [C5 FIX] CRITICAL: Second leg failed, rollback first leg
                        # Use direct counter-order instead of close_position
                        self.logger.error(
                            f"🚨 Second leg failed: {e}. Rolling back first leg..."
                        )
                        
                        opposite_side_a = 'sell' if side_a == 'buy' else 'buy'
                        try:
                            await client_a.create_order(
                                symbol=symbol,
                                side=opposite_side_a,
                                amount=amount
                            )
                            self.logger.warning("⚠️  Rollback successful (counter-order placed)")
                        except Exception as rollback_error:
                            self.logger.error(
                                f"💀 ROLLBACK FAILED: {rollback_error}. "
                                f"MANUAL INTERVENTION REQUIRED! "
                                f"Open position: {side_a} {amount} {symbol} on {ex_a}"
                            )
                        
                        raise Exception(f"Arbitrage entry failed - rolled back: {e}")
                
                except Exception as e:
                    self.logger.error(f"❌ Entry failed: {e}")
                    return False
                
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
                    'trade_amount_usdt': trade_amount_usdt,
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
                
                # Start risk monitor if not running
                await self.start_risk_monitor()
                
                return True
            
            except Exception as e:
                self.logger.error(f"❌ Arbitrage entry error: {e}")
                return False
    
    async def execute_arb_exit(self, symbol: str) -> bool:
        """Execute arbitrage exit."""
        if symbol not in self.active_trades:
            self.logger.warning(f"⏸️  No active trade for {symbol}")
            return False
        
        async with self._execution_lock:
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
                self.daily_pnl += total_pnl
                
                self.logger.info(
                    f"✅ Arbitrage closed: {symbol}, "
                    f"P&L=${total_pnl:.2f}, "
                    f"Holding Time={holding_time:.0f}s"
                )
                self.logger.info(f"💰 Cumulative P&L: ${self.cumulative_pnl:.2f} (Daily: ${self.daily_pnl:.2f})")
                
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
    
    async def get_active_positions(self) -> Dict[str, Dict]:
        """Get all active arbitrage positions."""
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
    
    async def shutdown(self) -> None:
        """
        [H4] Graceful shutdown: close all positions and CCXT clients.
        """
        self.logger.warning("🔻 ExecutionEngine shutting down...")
        
        # Cancel risk monitor
        if self._risk_monitor_task and not self._risk_monitor_task.done():
            self._risk_monitor_task.cancel()
            try:
                await self._risk_monitor_task
            except asyncio.CancelledError:
                pass
        
        # Close all open positions
        await self.emergency_close_all()
        
        # Close all CCXT clients
        for name, client in self.clients.items():
            try:
                if hasattr(client, 'close'):
                    await client.close()
                    self.logger.info(f"Closed {name} client")
            except Exception as e:
                self.logger.error(f"Error closing {name} client: {e}")
        
        self.logger.info(
            f"✅ Shutdown complete. Final P&L: ${self.cumulative_pnl:.2f}"
        )
