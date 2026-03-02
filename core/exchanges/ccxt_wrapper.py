"""
Real Exchange Implementation (CCXT Wrapper)

Wraps CCXT library for real trading with retry logic and timeout protection.
"""

import asyncio
import ccxt.async_support as ccxt
from typing import Dict, List, Optional

from core.interfaces.exchange import BaseExchange
from utils.logger import get_logger


# Default timeout for exchange API calls (seconds)
API_TIMEOUT = 15
# Number of retry attempts
MAX_RETRIES = 3
# Base delay between retries (seconds), doubles each attempt
RETRY_BASE_DELAY = 1.0


class RealExchange(BaseExchange):
    """
    Real trading exchange client using CCXT.
    
    Features:
    - Retry with exponential backoff on all API calls
    - Timeout protection (default 15s)
    - P&L calculation on position close
    """
    
    def __init__(
        self,
        exchange_name: str,
        api_key: str,
        api_secret: str,
        testnet: bool = False,
        passphrase: str = ''
    ):
        self.exchange_name = exchange_name
        self.logger = get_logger(__name__)
        
        # Build CCXT config
        ccxt_config = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
        }
        
        # Exchange-specific setup
        exchange_map = {
            'bingx': (ccxt.bingx, {'defaultType': 'swap', 'testnet': testnet}),
            'bybit': (ccxt.bybit, {'defaultType': 'linear', 'testnet': testnet}),
            'bitget': (ccxt.bitget, {'defaultType': 'swap', 'testnet': testnet}),
            'gateio': (ccxt.gateio, {'defaultType': 'swap', 'testnet': testnet}),
            'htx': (ccxt.htx, {'defaultType': 'swap', 'testnet': testnet}),
            'phemex': (ccxt.phemex, {'defaultType': 'swap', 'testnet': testnet}),
            'mexc': (ccxt.mexc, {'defaultType': 'swap', 'testnet': testnet}),
        }
        
        if exchange_name not in exchange_map:
            raise ValueError(f"Unsupported exchange: {exchange_name}")
        
        ex_class, options = exchange_map[exchange_name]
        ccxt_config['options'] = options
        
        if passphrase:
            ccxt_config['password'] = passphrase
        
        self.client = ex_class(ccxt_config)
        
        self.logger.info(
            f"💰 RealExchange initialized for {exchange_name} "
            f"(Testnet: {testnet})"
        )
    
    async def _retry_call(self, coro_func, *args, **kwargs):
        """
        Execute an async exchange call with retry and timeout.
        
        Retries on network/exchange errors with exponential backoff.
        Does NOT retry on authentication or invalid order errors.
        """
        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return await asyncio.wait_for(
                    coro_func(*args, **kwargs),
                    timeout=API_TIMEOUT
                )
            except asyncio.TimeoutError:
                last_error = TimeoutError(
                    f"{self.exchange_name}: API call timed out after {API_TIMEOUT}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                self.logger.warning(str(last_error))
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, 
                    ccxt.RequestTimeout, ccxt.DDoSProtection) as e:
                last_error = e
                self.logger.warning(
                    f"{self.exchange_name}: Retryable error (attempt {attempt}/{MAX_RETRIES}): {e}"
                )
            except (ccxt.AuthenticationError, ccxt.InvalidOrder, 
                    ccxt.InsufficientFunds, ccxt.BadSymbol) as e:
                # Non-retryable errors — fail immediately
                self.logger.error(f"{self.exchange_name}: Non-retryable error: {e}")
                raise
            except Exception as e:
                last_error = e
                self.logger.warning(
                    f"{self.exchange_name}: Unexpected error (attempt {attempt}/{MAX_RETRIES}): {e}"
                )
            
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                self.logger.info(f"Retrying in {delay}s...")
                await asyncio.sleep(delay)
        
        raise last_error
    
    async def get_balance(self) -> Dict[str, Dict[str, float]]:
        """Get account balance with retry."""
        balance = await self._retry_call(self.client.fetch_balance)
        return balance
    
    async def fetch_ticker(self, symbol: str) -> Dict[str, float]:
        """Get current ticker data with retry."""
        ticker = await self._retry_call(self.client.fetch_ticker, symbol)
        return {
            'bid': ticker.get('bid', 0),
            'ask': ticker.get('ask', 0),
            'last': ticker.get('last', 0),
            'timestamp': ticker.get('timestamp', 0)
        }
    
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Dict:
        """Fetch real order book with retry."""
        return await self._retry_call(self.client.fetch_order_book, symbol, limit)
    
    async def create_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: Optional[float] = None
    ) -> Dict:
        """Create and execute order with retry and timeout."""
        order_type = 'limit' if price else 'market'
        
        order = await self._retry_call(
            self.client.create_order,
            symbol=symbol,
            type=order_type,
            side=side,
            amount=amount,
            price=price
        )
        
        self.logger.info(
            f"💰 [LIVE] {side.upper()} {amount} {symbol} @ "
            f"${order.get('average', price or 0):.6f}"
        )
        
        return order
    
    async def fetch_positions(self) -> List[Dict]:
        """Get all open positions with retry."""
        positions = await self._retry_call(self.client.fetch_positions)
        
        # Filter out closed positions
        open_positions = [
            pos for pos in positions
            if float(pos.get('contracts', 0)) > 0
        ]
        
        return open_positions
    
    async def close_position(self, symbol: str) -> Dict:
        """
        Close a position and return P&L.
        
        Returns dict with 'pnl' key for ExecutionEngine compatibility.
        """
        # Get current position with entry price
        positions = await self.fetch_positions()
        position = next(
            (p for p in positions if p['symbol'] == symbol),
            None
        )
        
        if not position:
            raise ValueError(f"No open position for {symbol}")
        
        # Extract position details for P&L calculation
        side = position.get('side', 'long')
        opposite_side = 'sell' if side == 'long' else 'buy'
        amount = abs(float(position.get('contracts', 0)))
        entry_price = float(position.get('entryPrice', 0))
        
        # Grab unrealized P&L from exchange (most reliable source)
        exchange_pnl = position.get('unrealizedPnl')
        
        # Execute closing order
        order = await self.create_order(
            symbol=symbol,
            side=opposite_side,
            amount=amount
        )
        
        # Calculate P&L
        close_price = float(order.get('average', 0))
        
        if exchange_pnl is not None and exchange_pnl != 0:
            # Use exchange-reported P&L (includes funding, etc.)
            pnl = float(exchange_pnl)
        elif entry_price > 0 and close_price > 0:
            # Calculate from prices
            if side == 'long':
                pnl = (close_price - entry_price) * amount
            else:
                pnl = (entry_price - close_price) * amount
            # Subtract fees
            fee = order.get('fee', {}).get('cost', 0)
            pnl -= float(fee) if fee else 0
        else:
            pnl = 0.0
            self.logger.warning(f"⚠️ Could not calculate P&L for {symbol}")
        
        self.logger.info(
            f"💰 [LIVE] CLOSED {symbol}: P&L=${pnl:.2f} "
            f"(Entry: ${entry_price:.6f}, Exit: ${close_price:.6f})"
        )
        
        # Return with pnl key for ExecutionEngine compatibility
        result = order.copy()
        result['pnl'] = pnl
        return result
    
    def get_exchange_name(self) -> str:
        """Get exchange name."""
        return self.exchange_name
    
    async def close(self) -> None:
        """Close CCXT client connection."""
        await self.client.close()
