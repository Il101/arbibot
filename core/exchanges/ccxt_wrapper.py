"""
Real Exchange Implementation (CCXT Wrapper)

Wraps CCXT library for real trading on BingX and Bybit.
"""

import ccxt.async_support as ccxt
from typing import Dict, List, Optional

from core.interfaces.exchange import BaseExchange
from utils.logger import get_logger


class RealExchange(BaseExchange):
    """
    Real trading exchange client using CCXT.
    
    This is a skeleton implementation. Full implementation with
    error handling, retry logic, and rate limiting will be added later.
    """
    
    def __init__(
        self,
        exchange_name: str,
        api_key: str,
        api_secret: str,
        testnet: bool = False
    ):
        """
        Initialize Real Exchange client.
        
        Args:
            exchange_name: Exchange identifier ('bingx' or 'bybit')
            api_key: API key
            api_secret: API secret
            testnet: Use testnet if True
        """
        self.exchange_name = exchange_name
        self.logger = get_logger(__name__)
        
        # Initialize CCXT client
        if exchange_name == 'bingx':
            self.client = ccxt.bingx({
                'apiKey': api_key,
                'secret': api_secret,
                'options': {
                    'defaultType': 'swap',  # Perpetual futures
                    'testnet': testnet
                }
            })
        elif exchange_name == 'bybit':
            self.client = ccxt.bybit({
                'apiKey': api_key,
                'secret': api_secret,
                'options': {
                    'defaultType': 'linear',  # Linear perpetuals
                    'testnet': testnet
                }
            })
        elif exchange_name == 'bitget':
            self.client = ccxt.bitget({
                'apiKey': api_key,
                'secret': api_secret,
                'password': '',  # Passphrase - loaded from config if provided
                'options': {
                    'defaultType': 'swap',  # Perpetual futures
                    'testnet': testnet
                }
            })
        elif exchange_name == 'gateio':
            self.client = ccxt.gateio({
                'apiKey': api_key,
                'secret': api_secret,
                'options': {
                    'defaultType': 'swap',  # USDT-settled futures
                    'testnet': testnet
                }
            })
        elif exchange_name == 'htx':
            self.client = ccxt.htx({
                'apiKey': api_key,
                'secret': api_secret,
                'options': {
                    'defaultType': 'swap',  # Linear swap
                    'testnet': testnet
                }
            })
        elif exchange_name == 'phemex':
            self.client = ccxt.phemex({
                'apiKey': api_key,
                'secret': api_secret,
                'options': {
                    'defaultType': 'swap',  # Perpetual contracts
                    'testnet': testnet
                }
            })
        elif exchange_name == 'mexc':
            self.client = ccxt.mexc({
                'apiKey': api_key,
                'secret': api_secret,
                'options': {
                    'defaultType': 'swap',  # USDT-M Futures
                    'testnet': testnet
                }
            })
        else:
            raise ValueError(f"Unsupported exchange: {exchange_name}")
        
        self.logger.info(
            f"💰 RealExchange initialized for {exchange_name} "
            f"(Testnet: {testnet})"
        )
    
    async def get_balance(self) -> Dict[str, Dict[str, float]]:
        """
        Get account balance from exchange.
        
        Returns:
            Balance in CCXT format
        """
        balance = await self.client.fetch_balance()
        
        # CCXT already returns in correct format
        return balance
    
    async def fetch_ticker(self, symbol: str) -> Dict[str, float]:
        """
        Get current ticker data.
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
        
        Returns:
            Ticker data
        """
        ticker = await self.client.fetch_ticker(symbol)
        
        return {
            'bid': ticker.get('bid', 0),
            'ask': ticker.get('ask', 0),
            'last': ticker.get('last', 0),
            'timestamp': ticker.get('timestamp', 0)
        }
    
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Dict:
        """
        Fetch real order book from exchange.
        """
        return await self.client.fetch_order_book(symbol, limit)
    
    async def create_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: Optional[float] = None
    ) -> Dict:
        """
        Create and execute an order on the exchange.
        
        Args:
            symbol: Trading pair
            side: 'buy' or 'sell'
            amount: Order size
            price: Limit price (None for market order)
        
        Returns:
            Order info in CCXT format
        """
        order_type = 'limit' if price else 'market'
        
        order = await self.client.create_order(
            symbol=symbol,
            type=order_type,
            side=side,
            amount=amount,
            price=price
        )
        
        self.logger.info(
            f"💰 [LIVE] {side.upper()} {amount} {symbol} @ "
            f"${order.get('average', price):.2f}"
        )
        
        return order
    
    async def fetch_positions(self) -> List[Dict]:
        """
        Get all open positions.
        
        Returns:
            List of positions in CCXT format
        """
        positions = await self.client.fetch_positions()
        
        # Filter out closed positions
        open_positions = [
            pos for pos in positions
            if float(pos.get('contracts', 0)) > 0
        ]
        
        return open_positions
    
    async def close_position(self, symbol: str) -> Dict:
        """
        Close a position by executing opposite side order.
        
        Args:
            symbol: Trading pair
        
        Returns:
            Order info for closing trade
        """
        # Get current position
        positions = await self.fetch_positions()
        position = next(
            (p for p in positions if p['symbol'] == symbol),
            None
        )
        
        if not position:
            raise ValueError(f"No open position for {symbol}")
        
        # Determine opposite side
        side = position.get('side', 'long')
        opposite_side = 'sell' if side == 'long' else 'buy'
        amount = abs(float(position.get('contracts', 0)))
        
        # Execute closing order
        order = await self.create_order(
            symbol=symbol,
            side=opposite_side,
            amount=amount
        )
        
        self.logger.info(f"💰 [LIVE] CLOSED {symbol}")
        
        return order
    
    def get_exchange_name(self) -> str:
        """Get exchange name."""
        return self.exchange_name
    
    async def close(self) -> None:
        """Close CCXT client connection."""
        await self.client.close()
