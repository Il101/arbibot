"""
BaseExchange Interface

Defines the contract that all exchange implementations must follow.
This enables seamless switching between Paper Trading and Real Trading.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class BaseExchange(ABC):
    """
    Abstract base class for exchange implementations.
    
    All exchange clients (Paper and Real) must implement these methods
    to ensure compatibility with the ExecutionEngine.
    
    Return values must match CCXT format for consistency.
    """
    
    @abstractmethod
    async def get_balance(self) -> Dict[str, Dict[str, float]]:
        """
        Get account balance.
        
        Returns:
            Balance in CCXT format:
            {
                'USDT': {
                    'free': 1000.0,
                    'used': 0.0,
                    'total': 1000.0
                }
            }
        """
        pass
    
    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, float]:
        """
        Get current ticker data for a symbol.
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
        
        Returns:
            Ticker data in CCXT format:
            {
                'bid': 45000.0,
                'ask': 45010.0,
                'last': 45005.0,
                'timestamp': 1234567890
            }
        """
        pass
    
    @abstractmethod
    async def create_order(
        self,
        symbol: str,
        side: str,
        amount: float,
        price: Optional[float] = None
    ) -> Dict:
        """
        Create and execute an order.
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
            side: 'buy' or 'sell'
            amount: Order size in base currency
            price: Optional limit price (None for market order)
        
        Returns:
            Order info in CCXT format:
            {
                'id': 'order_12345',
                'status': 'closed',
                'symbol': 'BTC/USDT',
                'side': 'buy',
                'type': 'market',
                'filled': 0.01,
                'average': 45005.0,
                'cost': 450.05,
                'fee': {
                    'cost': 0.27,
                    'currency': 'USDT'
                },
                'timestamp': 1234567890
            }
        """
        pass
    
    @abstractmethod
    async def fetch_positions(self) -> List[Dict]:
        """
        Get all open positions with P&L.
        
        Returns:
            List of positions in CCXT format:
            [
                {
                    'symbol': 'BTC/USDT',
                    'side': 'long',
                    'contracts': 0.01,
                    'contractSize': 1.0,
                    'entryPrice': 45000.0,
                    'markPrice': 45100.0,
                    'unrealizedPnl': 1.0,
                    'percentage': 0.22,
                    'timestamp': 1234567890
                }
            ]
        """
        pass
    
    @abstractmethod
    async def close_position(self, symbol: str) -> Dict:
        """
        Close a specific position.
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
        
        Returns:
            Order info for the closing trade (same format as create_order)
        """
        pass
    
    @abstractmethod
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Dict:
        """
        Get order book for a symbol.
        
        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
            limit: Number of levels to fetch
            
        Returns:
            Order book in CCXT format:
            {
                'bids': [[price, amount], ...],
                'asks': [[price, amount], ...],
                'timestamp': 1234567890
            }
        """
        pass
    
    @abstractmethod
    def get_exchange_name(self) -> str:
        """
        Get the exchange name.
        
        Returns:
            Exchange identifier (e.g., 'bingx', 'bybit')
        """
        pass
