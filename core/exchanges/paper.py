"""
Paper Trading Exchange Implementation

Simulates trading without real money using WebSocket price feeds.
Implements realistic bid/ask spread execution and fee simulation.
"""

import json
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import asyncio
import ccxt.async_support as ccxt

from core.interfaces.exchange import BaseExchange
from utils.logger import get_logger


class PaperExchange(BaseExchange):
    """
    Paper trading exchange client for risk-free strategy testing.
    
    Features:
    - Realistic bid/ask spread execution
    - Fee simulation from config
    - State persistence to JSON
    - P&L calculation with current market prices
    - CCXT-compatible response format
    """
    
    def __init__(
        self,
        exchange_name: str,
        initial_balance: float,
        ws_manager,
        fee_rate: float = 0.0006,
        state_file: str = 'data/paper_state.json'
    ):
        """
        Initialize Paper Exchange.
        
        Args:
            exchange_name: Exchange identifier ('bingx' or 'bybit')
            initial_balance: Starting balance in USDT
            ws_manager: WebSocketManager instance for price data
            fee_rate: Trading fee rate (default: 0.06%)
            state_file: Path to state persistence file
        """
        self.exchange_name = exchange_name
        self.ws_manager = ws_manager
        self.fee_rate = fee_rate
        self.state_file = Path(state_file)
        self.logger = get_logger(__name__)
        
        # CCXT Client for Real Orderbooks
        self.ccxt_client = None
        if hasattr(ccxt, exchange_name):
            try:
                self.ccxt_client = getattr(ccxt, exchange_name)({'enableRateLimit': True})
            except Exception as e:
                self.logger.warning(f"Failed to init CCXT for {exchange_name}: {e}")
        else:
            self.logger.warning(f"CCXT exchange {exchange_name} not found, falling back to synthetic orderbooks")
            
        self._orderbook_cache = {}  # {symbol: {'data': ob, 'timestamp': time}}
        self._ob_cache_ttl = 1.0  # 1 second cache
        
        # State variables
        self.balance = initial_balance
        self.positions: Dict[str, Dict] = {}  # symbol -> position info
        
        # Load persisted state if exists
        self._load_state()
        
        self.logger.info(
            f"📄 PaperExchange initialized for {exchange_name}: "
            f"Balance=${self.balance:.2f}, Fee={fee_rate*100:.2f}%"
        )
    
    def _load_state(self) -> None:
        """Load state from JSON file if it exists."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                
                # Load state for this exchange
                exchange_state = state.get(self.exchange_name, {})
                if exchange_state:
                    self.balance = exchange_state.get('balance', self.balance)
                    self.positions = exchange_state.get('positions', {})
                    
                    self.logger.info(
                        f"📄 Loaded state for {self.exchange_name}: "
                        f"Balance=${self.balance:.2f}, Positions={len(self.positions)}"
                    )
            except Exception as e:
                self.logger.error(f"Failed to load state: {e}")
    
    def _save_state(self) -> None:
        """Save current state to JSON file."""
        try:
            # Create data directory if needed
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Load existing state
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
            else:
                state = {}
            
            # Update state for this exchange
            state[self.exchange_name] = {
                'balance': self.balance,
                'positions': self.positions,
                'last_updated': datetime.now().isoformat()
            }
            
            # Save to file
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            self.logger.debug(f"📄 Saved state for {self.exchange_name}")
        
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
    
    async def get_balance(self) -> Dict[str, Dict[str, float]]:
        """
        Get account balance.
        
        Returns:
            Balance in CCXT format
        """
        # Calculate used balance (locked in positions)
        used = sum(
            pos['size'] * pos['entry_price']
            for pos in self.positions.values()
        )
        
        return {
            'USDT': {
                'free': self.balance,
                'used': used,
                'total': self.balance + used
            }
        }
    
    async def fetch_ticker(self, symbol: str) -> Dict[str, float]:
        """
        Get current ticker data from WebSocket.
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
        
        Returns:
            Ticker data with bid/ask/last
        """
        price_data = self.ws_manager.get_latest_price(self.exchange_name, symbol)
        
        if not price_data:
            raise ValueError(f"No price data available for {symbol} on {self.exchange_name}")
        
        return {
            'bid': price_data['bid'],
            'ask': price_data['ask'],
            'last': price_data['last'],
            'timestamp': int(price_data.get('timestamp', time.time() * 1000))
        }
    
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Dict:
        """
        Get simulated order book.
        
        Args:
            symbol: Trading pair
            limit: Number of levels
            
        Returns:
            Order book
        """
        now = time.time()
        
        # 1. Check Cache
        cached = self._orderbook_cache.get(symbol)
        if cached and (now - cached['timestamp']) < self._ob_cache_ttl:
            return cached['data']
            
        # 2. Try fetching real order book via CCXT
        if self.ccxt_client:
            # CCXT expects symbols like 'BTC/USDT:USDT' for futures usually
            ccxt_symbol = symbol
            if self.exchange_name in ['bybit', 'bingx', 'bitget'] and ':' not in symbol:
                ccxt_symbol = f"{symbol}:USDT"
                
            try:
                ob = await asyncio.wait_for(
                    self.ccxt_client.fetch_order_book(ccxt_symbol, limit),
                    timeout=2.0
                )
                self._orderbook_cache[symbol] = {'data': ob, 'timestamp': now}
                return ob
            except Exception as e:
                self.logger.debug(f"Failed to fetch real orderbook for {symbol}, falling back to synthetic: {e}")
                
        # 3. Fallback to Synthetic (from websockets)
        ticker = await self.fetch_ticker(symbol)
        
        # Simulate depth with 0.01% steps
        bids = []
        asks = []
        for i in range(limit):
            spread_step = 0.0001 * (i + 1)
            # Simulate some volume (e.g., $1000 per level)
            vol = 1000.0 / ticker['last']
            
            bids.append([ticker['bid'] * (1 - spread_step), vol])
            asks.append([ticker['ask'] * (1 + spread_step), vol])
            
        ob = {
            'bids': bids,
            'asks': asks,
            'timestamp': ticker['timestamp']
        }
        self._orderbook_cache[symbol] = {'data': ob, 'timestamp': now}
        return ob

    async def create_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: Optional[float] = None
    ) -> Dict:
        """
        Create and execute a simulated order.
        
        Uses realistic bid/ask pricing:
        - BUY orders execute at ASK price (you pay seller's price)
        - SELL orders execute at BID price (you receive buyer's price)
        
        Args:
            symbol: Trading pair
            side: 'buy' or 'sell'
            amount: Order size in base currency
            price: Ignored for paper trading (uses market price)
        
        Returns:
            Order info in CCXT format
        """
        # Get current market price
        ticker = await self.fetch_ticker(symbol)
        
        # Realistic execution price
        if side == 'buy':
            execution_price = ticker['ask']  # Pay the ask
        else:
            execution_price = ticker['bid']  # Receive the bid
        
        # Calculate cost and fees
        cost = amount * execution_price
        fee_cost = cost * self.fee_rate
        total_cost = cost + fee_cost
        
        if symbol in self.positions:
            existing = self.positions[symbol]
            if existing['side'] != side:
                # We are closing or reducing a position
                # Verify if we are taking more than we have
                reduce_amount = min(amount, existing['size'])
                remaining_amount = amount - reduce_amount
                
                # Calculate P&L for the reduced portion
                if existing['side'] == 'buy':
                    pnl = (execution_price - existing['entry_price']) * reduce_amount
                else:
                    pnl = (existing['entry_price'] - execution_price) * reduce_amount
                
                # Return allocated margin and add P&L, minus the fee cost of the closing trade
                margin_freed = reduce_amount * existing['entry_price']
                self.balance += margin_freed + pnl - fee_cost
                
                if abs(existing['size'] - reduce_amount) < 1e-8:
                    del self.positions[symbol]
                else:
                    self.positions[symbol]['size'] -= reduce_amount
                
                if remaining_amount > 1e-8:
                    # We closed the position and reversed the direction! Open new opposite position.
                    # Verify we can afford to open the rest
                    cost_rem = remaining_amount * execution_price
                    fee_cost_rem = cost_rem * self.fee_rate
                    total_cost_rem = cost_rem + fee_cost_rem
                    
                    if total_cost_rem > self.balance:
                        raise ValueError(
                            f"Insufficient balance for reverse trade: need ${total_cost_rem:.2f}, have ${self.balance:.2f}"
                        )
                    
                    self.balance -= total_cost_rem
                    self.positions[symbol] = {
                        'side': side,
                        'size': remaining_amount,
                        'entry_price': execution_price,
                        'entry_time': datetime.now().isoformat()
                    }
                    fee_cost += fee_cost_rem
                    cost += cost_rem
                    
            else:
                # Average down/up existing position
                if total_cost > self.balance:
                    raise ValueError(
                        f"Insufficient balance: need ${total_cost:.2f}, have ${self.balance:.2f}"
                    )
                self.balance -= total_cost
                total_size = existing['size'] + amount
                avg_price = (
                    (existing['entry_price'] * existing['size']) +
                    (execution_price * amount)
                ) / total_size
                
                self.positions[symbol] = {
                    'side': side,
                    'size': total_size,
                    'entry_price': avg_price,
                    'entry_time': existing['entry_time']
                }
                
        else:
            # New position
            if total_cost > self.balance:
                raise ValueError(
                    f"Insufficient balance: need ${total_cost:.2f}, have ${self.balance:.2f}"
                )
            self.balance -= total_cost
            self.positions[symbol] = {
                'side': side,
                'size': amount,
                'entry_price': execution_price,
                'entry_time': datetime.now().isoformat()
            }
        
        # Save state
        self._save_state()
        
        # Generate order response
        order_id = f"paper_{uuid.uuid4().hex[:8]}"
        
        self.logger.info(
            f"📄 [PAPER] {side.upper()} {amount} {symbol} @ ${execution_price:.2f} "
            f"(Fee: ${fee_cost:.2f})"
        )
        
        return {
            'id': order_id,
            'status': 'closed',
            'symbol': symbol,
            'side': side,
            'type': 'market',
            'filled': amount,
            'average': execution_price,
            'cost': cost,
            'fee': {
                'cost': fee_cost,
                'currency': 'USDT'
            },
            'timestamp': int(time.time() * 1000)
        }
    
    async def fetch_positions(self) -> List[Dict]:
        """
        Get all open positions with unrealized P&L.
        
        Returns:
            List of positions in CCXT format
        """
        positions = []
        
        for symbol, pos in self.positions.items():
            try:
                # Get current market price
                ticker = await self.fetch_ticker(symbol)
                
                # Calculate unrealized P&L using execution prices
                if pos['side'] == 'buy':
                    # To close a long, we would sell at bid
                    close_price = ticker['bid']
                    unrealized_pnl = (close_price - pos['entry_price']) * pos['size']
                else:
                    # To close a short, we would buy at ask
                    close_price = ticker['ask']
                    unrealized_pnl = (pos['entry_price'] - close_price) * pos['size']
                
                # Subtract simulated closing fee from unrealized P&L
                closing_fee = (close_price * pos['size']) * self.fee_rate
                unrealized_pnl -= closing_fee
                
                # Calculate percentage
                percentage = (unrealized_pnl / (pos['entry_price'] * pos['size'])) * 100
                
                positions.append({
                    'symbol': symbol,
                    'side': 'long' if pos['side'] == 'buy' else 'short',
                    'contracts': pos['size'],
                    'contractSize': 1.0,
                    'entryPrice': pos['entry_price'],
                    'markPrice': close_price,
                    'unrealizedPnl': unrealized_pnl,
                    'percentage': percentage,
                    'timestamp': int(time.time() * 1000)
                })
            
            except Exception as e:
                self.logger.error(f"Error calculating P&L for {symbol}: {e}")
        
        return positions
    
    async def close_position(self, symbol: str) -> Dict:
        """
        Close a position by executing opposite side order.
        
        Args:
            symbol: Trading pair
        
        Returns:
            Order info for closing trade
        """
        if symbol not in self.positions:
            raise ValueError(f"No open position for {symbol}")
            
        pos = self.positions[symbol]
        side_to_close = 'sell' if pos['side'] == 'buy' else 'buy'
        size_to_close = pos['size']
        
        # We can just delegate to create_order!
        # It handles P&L calculation and balance updates properly now.
        order_resp = await self.create_order(symbol, side_to_close, size_to_close)
        
        # Calculate Pnl merely for reporting
        avg_price = order_resp['average']
        if pos['side'] == 'buy':
            pnl = (avg_price - pos['entry_price']) * size_to_close
        else:
            pnl = (pos['entry_price'] - avg_price) * size_to_close
            
        pnl -= order_resp['fee']['cost']
        
        order_resp['pnl'] = pnl
        
        self.logger.info(
            f"📄 [PAPER] CLOSED {symbol}: P&L=${pnl:.2f} "
            f"(Entry: ${pos['entry_price']:.2f}, Exit: ${avg_price:.2f})"
        )
        
        return order_resp
    
    def get_exchange_name(self) -> str:
        """Get exchange name."""
        return self.exchange_name
    
    async def close(self) -> None:
        """Close the CCXT client instance to prevent session warnings."""
        if self.ccxt_client:
            await self.ccxt_client.close()
