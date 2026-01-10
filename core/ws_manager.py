"""
WebSocket Manager for Real-Time Price Feeds

Manages WebSocket connections to BingX and Bybit exchanges for live price streaming.
Handles connection lifecycle, message normalization, and auto-reconnection.
"""

import asyncio
import json
import gzip
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from collections import defaultdict
import sys

import aiohttp
from aiohttp import WSMsgType
from utils.logger import get_logger
from core.event_bus import EventBus
from utils.config import get_config

class WebSocketManager:
    """
    Manages WebSocket connections to cryptocurrency exchanges.
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialize WebSocket Manager.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.logger = get_logger(__name__)
        self.config = get_config(config_path)
        
        # Connection state
        self.connections: Dict[str, aiohttp.ClientWebSocketResponse] = {}
        self.connection_status: Dict[str, bool] = {
            'bingx': False,
            'bybit': False,
            'bitget': False,
            'gateio': False,
            'htx': False,
            'phemex': False,
            'mexc': False
        }
        
        # Data distribution
        self.message_queue: asyncio.Queue = asyncio.Queue(
            maxsize=self.config['websocket'].get('message_queue_size', 1000)
        )
        
        # Latest prices cache
        self.latest_prices: Dict[str, Dict[str, Dict]] = defaultdict(dict)
        
        # Task management
        self.tasks: List[asyncio.Task] = []
        self.running = False
        
        # Subscription tracking (exchange-specific format)
        self.subscribed_symbols: Dict[str, List[str]] = {
            'bingx': [],
            'bybit': [],
            'bitget': [],
            'gateio': [],
            'htx': [],
            'phemex': [],
            'mexc': []
        }
        
        # Active symbols tracking (normalized format: BTC/USDT)
        # This set tracks which symbols should be subscribed across all exchanges
        self.active_symbols: set = set()
        
        self.logger.info("WebSocketManager initialized")
        
        # Get EventBus instance for connection status updates
        self.event_bus = EventBus.instance()
    
    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        """Safely convert value to float, handling empty strings and None."""
        if value is None or value == '':
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _load_config(self, config_path: str) -> dict:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to config file
            
        Returns:
            Configuration dictionary
        """
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                self.logger.warning(f"Config file not found: {config_path}, using defaults")
                return self._get_default_config()
            
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            self.logger.info(f"Configuration loaded from {config_path}")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading config: {e}, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> dict:
        """Return default configuration."""
        return {
            'websocket': {
                'bingx': {
                    'url': 'wss://open-api-swap.bingx.com/swap-market'
                },
                'bybit': {
                    'url': 'wss://stream.bybit.com/v5/public/linear'
                },
                'reconnect_delay': 5,
                'max_reconnect_attempts': 10,
                'ping_interval': 30,
                'pong_timeout': 60,
                'message_queue_size': 1000
            }
        }
    
    async def connect_exchange(
        self,
        exchange_name: str,
        symbols: List[str] = None
    ) -> None:
        """
        Establish WebSocket connection to an exchange with auto-reconnect.
        
        Args:
            exchange_name: Exchange identifier ('bingx' or 'bybit')
            symbols: Optional list of symbols (deprecated, use active_symbols)
        """
        ws_config = self.config['websocket'][exchange_name]
        url = ws_config['url']
        
        reconnect_delay = self.config['websocket']['reconnect_delay']
        max_attempts = self.config['websocket']['max_reconnect_attempts']
        
        attempt = 0
        
        while self.running and attempt < max_attempts:
            try:
                self.logger.info(f"Connecting to {exchange_name} WebSocket (attempt {attempt + 1}/{max_attempts})")
                
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        self.connections[exchange_name] = ws
                        self.connection_status[exchange_name] = True
                        self.logger.info(f"Connected to {exchange_name}")
                        
                        # Emit connection status
                        self.event_bus.emit_connection_status(exchange_name, True)
                        
                        # STATE RECOVERY: Resubscribe to all active symbols after reconnection
                        if self.active_symbols:
                            symbols_list = list(self.active_symbols)
                            self.logger.info(
                                f"State recovery: Resubscribing to {len(symbols_list)} symbols on {exchange_name}"
                            )
                            await self._subscribe_symbols(exchange_name, ws, symbols_list)
                        
                        # Start heartbeat task
                        heartbeat_task = asyncio.create_task(
                            self._heartbeat(ws, exchange_name)
                        )
                        self.tasks.append(heartbeat_task)
                        
                        # Message processing loop
                        async for msg in ws:
                            # Handle TEXT messages (Bybit JSON, BingX Pong/Ping)
                            if msg.type == WSMsgType.TEXT:
                                # Check if it's a server ping from BingX - respond with Pong
                                if msg.data.strip().lower() == "ping":
                                    await ws.send_str("Pong")
                                    self.logger.debug(f"Received Ping from {exchange_name}, sent Pong")
                                    continue
                                
                                # Check if it's our own Pong echo
                                if msg.data.strip().lower() == "pong":
                                    self.logger.debug(f"Received Pong echo from {exchange_name}")
                                    continue
                                
                                # Otherwise parse as JSON
                                try:
                                    data = json.loads(msg.data)
                                    await self._handle_message(exchange_name, data)
                                except json.JSONDecodeError as e:
                                    self.logger.error(f"JSON decode error from {exchange_name}: {e}")
                                except Exception as e:
                                    self.logger.error(f"Error handling message from {exchange_name}: {e}")
                            
                            # Handle BINARY messages (BingX and HTX use GZIP compression)
                            elif msg.type == WSMsgType.BINARY:
                                try:
                                    # Decompress GZIP data
                                    decompressed = gzip.decompress(msg.data).decode('utf-8').strip()
                                    
                                    # Check if it's a server ping from BingX - respond with Pong
                                    if decompressed.lower() == "ping":
                                        await ws.send_str("Pong")
                                        self.logger.debug(f"Received Ping (binary) from {exchange_name}, sent Pong")
                                        continue
                                    
                                    # Check if it's a Pong echo or empty
                                    if not decompressed or decompressed.lower() == "pong":
                                        self.logger.debug(f"Received Pong/empty from {exchange_name}")
                                        continue
                                    
                                    # Try to parse as JSON
                                    try:
                                        data = json.loads(decompressed)
                                        
                                        # HTX sends ping in JSON format, respond with pong
                                        if exchange_name == 'htx' and 'ping' in data:
                                            pong_msg = {"pong": data["ping"]}
                                            await ws.send_json(pong_msg)
                                            self.logger.debug(f"Received HTX ping {data['ping']}, sent pong")
                                            continue
                                        
                                        await self._handle_message(exchange_name, data)
                                    except json.JSONDecodeError:
                                        # Not JSON, might be another heartbeat format
                                        self.logger.debug(f"Non-JSON message from {exchange_name}: {decompressed[:50]}")
                                        continue
                                        
                                except gzip.BadGzipFile as e:
                                    self.logger.error(f"GZIP decompression error from {exchange_name}: {e}")
                                except Exception as e:
                                    self.logger.error(f"Error handling binary message from {exchange_name}: {e}")
                            
                            elif msg.type == WSMsgType.ERROR:
                                self.logger.error(f"WebSocket error from {exchange_name}: {ws.exception()}")
                                break
                            
                            elif msg.type == WSMsgType.CLOSED:
                                self.logger.warning(f"WebSocket closed by {exchange_name}")
                                break
                        
                        # Connection closed
                        heartbeat_task.cancel()
                        self.connection_status[exchange_name] = False
                        self.event_bus.emit_connection_status(exchange_name, False)
                        
            except aiohttp.ClientError as e:
                self.logger.error(f"Connection error to {exchange_name}: {e}")
                self.connection_status[exchange_name] = False
                self.event_bus.emit_connection_status(exchange_name, False)
                
            except Exception as e:
                self.logger.error(f"Unexpected error with {exchange_name}: {e}")
                self.connection_status[exchange_name] = False
                self.event_bus.emit_connection_status(exchange_name, False)
            
            # Reconnect logic
            if self.running:
                attempt += 1
                if attempt < max_attempts:
                    delay = reconnect_delay * (2 ** (attempt - 1))  # Exponential backoff
                    self.logger.info(f"Reconnecting to {exchange_name} in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(f"Max reconnection attempts reached for {exchange_name}")
                    break
    
    async def _subscribe_symbols(
        self,
        exchange_name: str,
        ws: aiohttp.ClientWebSocketResponse,
        symbols: List[str]
    ) -> None:
        """
        Subscribe to ticker streams for given symbols.
        
        Args:
            exchange_name: Exchange identifier
            ws: WebSocket connection
            symbols: List of symbols to subscribe
        """
        if exchange_name == 'bingx':
            # BingX subscription format
            for symbol in symbols:
                # Convert BTC/USDT to BTC-USDT for BingX
                bingx_symbol = symbol.replace('/', '-')
                subscribe_msg = {
                    "id": f"sub_{bingx_symbol}",
                    "reqType": "sub",
                    "dataType": f"{bingx_symbol}@ticker"
                }
                await ws.send_json(subscribe_msg)
                self.logger.debug(f"Subscribed to {bingx_symbol} on BingX")
            
            self.subscribed_symbols['bingx'] = symbols
        
        elif exchange_name == 'bybit':
            # Bybit subscription format
            bybit_symbols = [symbol.replace('/', '') for symbol in symbols]  # BTCUSDT
            subscribe_msg = {
                "op": "subscribe",
                "args": [f"tickers.{s}" for s in bybit_symbols]
            }
            await ws.send_json(subscribe_msg)
            self.logger.debug(f"Subscribed to {len(symbols)} symbols on Bybit")
            
            self.subscribed_symbols['bybit'] = symbols
        
        elif exchange_name == 'bitget':
            # Bitget USDT-M Futures subscription format
            args = []
            for symbol in symbols:
                # Convert BTC/USDT or BTC/USDT:USDT to BTCUSDT
                bitget_symbol = symbol.replace('/', '').replace(':USDT', '')
                args.append({
                    "instType": "USDT-FUTURES",
                    "channel": "ticker",
                    "instId": bitget_symbol
                })
            subscribe_msg = {"op": "subscribe", "args": args}
            await ws.send_json(subscribe_msg)
            self.logger.debug(f"Subscribed to {len(symbols)} symbols on Bitget")
            
            self.subscribed_symbols['bitget'] = symbols
        
        elif exchange_name == 'gateio':
            # Gate.io Futures subscription format
            for symbol in symbols:
                # Convert BTC/USDT to BTC_USDT
                gateio_symbol = symbol.replace('/', '_').replace(':USDT', '')
                subscribe_msg = {
                    "time": int(time.time()),
                    "channel": "futures.tickers",
                    "event": "subscribe",
                    "payload": [gateio_symbol]
                }
                await ws.send_json(subscribe_msg)
            self.logger.debug(f"Subscribed to {len(symbols)} symbols on Gate.io")
            
            self.subscribed_symbols['gateio'] = symbols
        
        elif exchange_name == 'htx':
            # HTX Linear Swap subscription format
            for symbol in symbols:
                # Convert BTC/USDT to BTC-USDT
                htx_symbol = symbol.replace('/', '-').replace(':USDT', '')
                subscribe_msg = {
                    "sub": f"market.{htx_symbol}.detail",  # Use 'detail' not 'ticker'
                    "id": f"detail_{htx_symbol}"
                }
                await ws.send_json(subscribe_msg)
            self.logger.debug(f"Subscribed to {len(symbols)} symbols on HTX")
            
            self.subscribed_symbols['htx'] = symbols
        
        elif exchange_name == 'phemex':
            # Phemex subscription format - use orderbook for bid/ask updates
            for symbol in symbols:
                # Convert BTC/USDT to BTCUSD (Phemex uses USD suffix for perpetuals)
                phemex_symbol = symbol.replace('/', '').replace(':USDT', '').replace('USDT', 'USD')
                subscribe_msg = {
                    "id": 1,
                    "method": "orderbook.subscribe",
                    "params": [phemex_symbol]
                }
                await ws.send_json(subscribe_msg)
            self.logger.debug(f"Subscribed to {len(symbols)} symbols on Phemex")
            
            self.subscribed_symbols['phemex'] = symbols
        
        elif exchange_name == 'mexc':
            # MEXC Futures subscription format
            for symbol in symbols:
                # Convert BTC/USDT to BTC_USDT
                mexc_symbol = symbol.replace('/', '_').replace(':USDT', '')
                subscribe_msg = {
                    "method": "sub.ticker",
                    "param": {"symbol": mexc_symbol}
                }
                await ws.send_json(subscribe_msg)
            self.logger.debug(f"Subscribed to {len(symbols)} symbols on MEXC")
            
            self.subscribed_symbols['mexc'] = symbols
    
    async def _handle_message(self, exchange_name: str, message: dict) -> None:
        """
        Parse and normalize incoming WebSocket messages.
        
        Args:
            exchange_name: Exchange identifier
            message: Raw message from exchange
        """
        try:
            normalized_data = None
            
            if exchange_name == 'bingx':
                # Debug: Log raw BingX payload
                self.logger.debug(f"Raw BingX payload: {message}")
                
                # BingX message format
                if 'dataType' in message and '@ticker' in message.get('dataType', ''):
                    data = message.get('data', {})
                    symbol_raw = message['dataType'].split('@')[0]
                    symbol = symbol_raw.replace('-', '/')  # Convert back to BTC/USDT
                    
                    # Log available keys in first message
                    if not hasattr(self, '_bingx_keys_logged'):
                        self.logger.info(f"BingX ticker keys: {list(data.keys())}")
                        self._bingx_keys_logged = True
                    
                    # BingX uses uppercase keys:
                    # A = Ask price, B = Bid price, c = Close/last price
                    # a = ask size, b = bid size (lowercase are sizes, not prices!)
                    bid = self._safe_float(data.get('B', 0))  # Uppercase B for bid price
                    ask = self._safe_float(data.get('A', 0))  # Uppercase A for ask price
                    last = self._safe_float(data.get('c', 0))  # Close/last price
                    
                    # Fallback to last price if bid/ask not available
                    if bid == 0:
                        bid = last
                    if ask == 0:
                        ask = last
                    
                    normalized_data = {
                        'exchange': 'bingx',
                        'symbol': symbol,
                        'bid': bid,
                        'ask': ask,
                        'last': last,
                        'timestamp': int(data.get('E', 0)),  # Event time
                        'local_timestamp': time.time()
                    }
            
            elif exchange_name == 'bybit':
                # Bybit message format
                if message.get('topic', '').startswith('tickers.'):
                    data = message.get('data', {})
                    symbol_raw = data.get('symbol', '')
                    
                    # Convert BTCUSDT to BTC/USDT
                    if symbol_raw.endswith('USDT'):
                        base = symbol_raw[:-4]
                        symbol = f"{base}/USDT"
                    else:
                        symbol = symbol_raw
                    
                    # Bybit sends two types of messages:
                    # - "snapshot": Full data (first message)
                    # - "delta": Partial updates (only changed fields)
                    # We need to merge delta updates with cached data
                    
                    # Get cached data or create new entry
                    cached = self.latest_prices.get(exchange_name, {}).get(symbol, {})
                    
                    # Extract values, use cached if not present in delta
                    bid = self._safe_float(data.get('bid1Price')) or cached.get('bid', 0)
                    ask = self._safe_float(data.get('ask1Price')) or cached.get('ask', 0)
                    last = self._safe_float(data.get('lastPrice')) or cached.get('last', 0)
                    
                    normalized_data = {
                        'exchange': 'bybit',
                        'symbol': symbol,
                        'bid': bid,
                        'ask': ask,
                        'last': last,
                        'timestamp': int(message.get('ts', 0)),
                        'local_timestamp': time.time()
                    }
            
            elif exchange_name == 'bitget':
                # Bitget message format for USDT-M Futures
                if 'data' in message and message.get('action') in ['snapshot', 'update']:
                    data_list = message.get('data', [])
                    if data_list:
                        data = data_list[0]
                        symbol_raw = data.get('instId', '')
                        
                        # Convert BTCUSDT to BTC/USDT
                        if symbol_raw.endswith('USDT'):
                            base = symbol_raw[:-4]
                            symbol = f"{base}/USDT"
                        else:
                            symbol = symbol_raw
                        
                        bid = float(data.get('bidPr', 0) or data.get('bestBid', 0))
                        ask = float(data.get('askPr', 0) or data.get('bestAsk', 0))
                        last = float(data.get('lastPr', 0) or data.get('last', 0))
                        
                        normalized_data = {
                            'exchange': 'bitget',
                            'symbol': symbol,
                            'bid': bid,
                            'ask': ask,
                            'last': last,
                            'timestamp': int(data.get('ts', 0)),
                            'local_timestamp': time.time()
                        }
            
            elif exchange_name == 'gateio':
                # Gate.io Futures message format
                if message.get('event') == 'update' and message.get('channel') == 'futures.tickers':
                    result = message.get('result', [])
                    if result:
                        data = result[0] if isinstance(result, list) else result
                        symbol_raw = data.get('contract', '')
                        
                        # Convert BTC_USDT to BTC/USDT
                        symbol = symbol_raw.replace('_', '/')
                        
                        bid = float(data.get('highest_bid', 0) or 0)
                        ask = float(data.get('lowest_ask', 0) or 0)
                        last = float(data.get('last', 0) or 0)
                        
                        # Fallback to last if bid/ask missing
                        if bid == 0:
                            bid = last
                        if ask == 0:
                            ask = last
                        
                        normalized_data = {
                            'exchange': 'gateio',
                            'symbol': symbol,
                            'bid': bid,
                            'ask': ask,
                            'last': last,
                            'timestamp': int(message.get('time', 0) * 1000),
                            'local_timestamp': time.time()
                        }
            
            elif exchange_name == 'htx':
                # HTX Linear Swap message format
                # HTX sends ping messages that need pong response
                if 'ping' in message:
                    # Respond to ping (handled in connect_exchange)
                    pass
                elif 'tick' in message:
                    data = message.get('tick', {})
                    ch = message.get('ch', '')
                    
                    # Parse symbol from channel: market.BTC-USDT.detail
                    parts = ch.split('.')
                    if len(parts) >= 2:
                        symbol_raw = parts[1]
                        symbol = symbol_raw.replace('-', '/')
                        
                        # HTX returns bid/ask as [price, size] arrays
                        bid_arr = data.get('bid', [0])
                        ask_arr = data.get('ask', [0])
                        bid = float(bid_arr[0]) if isinstance(bid_arr, list) else float(bid_arr)
                        ask = float(ask_arr[0]) if isinstance(ask_arr, list) else float(ask_arr)
                        last = float(data.get('close', 0))
                        
                        normalized_data = {
                            'exchange': 'htx',
                            'symbol': symbol,
                            'bid': bid,
                            'ask': ask,
                            'last': last,
                            'timestamp': int(message.get('ts', 0)),
                            'local_timestamp': time.time()
                        }
            
            elif exchange_name == 'phemex':
                # Phemex orderbook message format
                if 'book' in message:
                    book = message.get('book', {})
                    symbol_raw = message.get('symbol', '')
                    
                    # Convert BTCUSD to BTC/USDT
                    if symbol_raw.endswith('USD'):
                        base = symbol_raw[:-3]
                        symbol = f"{base}/USDT"
                    else:
                        symbol = symbol_raw
                    
                    # Phemex orderbook uses scaled prices (divide by 10000)
                    # Format: [[price, size], ...]
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    
                    if bids and asks:
                        # First element is best bid/ask, format: [price_scaled, size]
                        bid = float(bids[0][0]) / 10000  # Scale down
                        ask = float(asks[0][0]) / 10000  # Scale down
                        last = (bid + ask) / 2  # Approximate last as mid price
                        
                        normalized_data = {
                            'exchange': 'phemex',
                            'symbol': symbol,
                            'bid': bid,
                            'ask': ask,
                            'last': last,
                            'timestamp': int(message.get('timestamp', 0)),
                            'local_timestamp': time.time()
                        }
            
            elif exchange_name == 'mexc':
                # MEXC Futures message format
                if message.get('channel') == 'push.ticker':
                    data = message.get('data', {})
                    symbol_raw = message.get('symbol', '')
                    
                    # Convert BTC_USDT to BTC/USDT
                    symbol = symbol_raw.replace('_', '/')
                    
                    bid = float(data.get('bid1', 0) or data.get('bestBidPrice', 0))
                    ask = float(data.get('ask1', 0) or data.get('bestAskPrice', 0))
                    last = float(data.get('lastPrice', 0) or data.get('last', 0))
                    
                    # Fallback if bid/ask missing
                    if bid == 0:
                        bid = last
                    if ask == 0:
                        ask = last
                    
                    normalized_data = {
                        'exchange': 'mexc',
                        'symbol': symbol,
                        'bid': bid,
                        'ask': ask,
                        'last': last,
                        'timestamp': int(data.get('timestamp', 0)),
                        'local_timestamp': time.time()
                    }
            
            # Store and distribute normalized data
            if normalized_data and normalized_data['bid'] > 0 and normalized_data['ask'] > 0:
                # Update cache
                self.latest_prices[exchange_name][normalized_data['symbol']] = normalized_data
                
                # Put in queue (non-blocking)
                try:
                    self.message_queue.put_nowait(normalized_data)
                except asyncio.QueueFull:
                    self.logger.warning(f"Message queue full, dropping message for {normalized_data['symbol']}")
                
                self.logger.debug(
                    f"{exchange_name} {normalized_data['symbol']}: "
                    f"bid={normalized_data['bid']}, ask={normalized_data['ask']}"
                )
        
        except Exception as e:
            self.logger.error(f"Error normalizing message from {exchange_name}: {e}")
    
    async def _heartbeat(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        exchange_name: str
    ) -> None:
        """
        Send periodic ping messages to keep connection alive.
        
        Args:
            ws: WebSocket connection
            exchange_name: Exchange identifier
        """
        ping_interval = self.config['websocket']['ping_interval']
        
        try:
            while not ws.closed:
                await asyncio.sleep(ping_interval)
                
                if exchange_name == 'bingx':
                    # BingX ping format: raw string "Ping"
                    await ws.send_str("Ping")
                    self.logger.debug(f"Sent Ping (raw string) to {exchange_name}")
                
                elif exchange_name == 'bybit':
                    # Bybit ping format: JSON
                    await ws.send_json({"op": "ping"})
                    self.logger.debug(f"Sent ping (JSON) to {exchange_name}")
                
                elif exchange_name == 'bitget':
                    # Bitget ping format: raw string "ping"
                    await ws.send_str("ping")
                    self.logger.debug(f"Sent ping to {exchange_name}")
                
                elif exchange_name == 'gateio':
                    # Gate.io ping format: JSON
                    await ws.send_json({
                        "time": int(time.time()),
                        "channel": "futures.ping"
                    })
                    self.logger.debug(f"Sent ping to {exchange_name}")
                
                elif exchange_name == 'htx':
                    # HTX: Server sends ping, we just need to respond with pong
                    # Pong is handled in message processing loop
                    pass
                
                elif exchange_name == 'phemex':
                    # Phemex ping format: JSON
                    await ws.send_json({
                        "id": 0,
                        "method": "server.ping",
                        "params": []
                    })
                    self.logger.debug(f"Sent ping to {exchange_name}")
                
                elif exchange_name == 'mexc':
                    # MEXC ping format: JSON
                    await ws.send_json({"method": "ping"})
                    self.logger.debug(f"Sent ping to {exchange_name}")
        
        except asyncio.CancelledError:
            self.logger.debug(f"Heartbeat cancelled for {exchange_name}")
        except Exception as e:
            self.logger.error(f"Heartbeat error for {exchange_name}: {e}")
    
    async def subscribe(self, symbols: List[str], exchanges: List[str] = None) -> None:
        """
        Dynamically subscribe to additional symbols on active connections.
        
        Args:
            symbols: List of normalized symbols (e.g., ["BTC/USDT", "ETH/USDT"])
            exchanges: Optional list of exchange names to subscribe on. If None, subscribes on all active connections.
        """
        if not symbols:
            return
        
        # Update active symbols set
        new_symbols = set(symbols) - self.active_symbols
        if not new_symbols:
            self.logger.debug(f"All symbols already subscribed: {symbols}")
            return
        
        self.active_symbols.update(new_symbols)
        self.logger.info(f"Adding {len(new_symbols)} symbols to active subscriptions: {list(new_symbols)}")
        
        # Subscribe on target connections
        target_exchanges = exchanges if exchanges else list(self.connections.keys())
        
        for exchange_name in target_exchanges:
            ws = self.connections.get(exchange_name)
            if ws and not ws.closed and self.connection_status.get(exchange_name, False):
                try:
                    await self._subscribe_symbols(exchange_name, ws, list(new_symbols))
                    self.logger.info(f"Successfully subscribed to {len(new_symbols)} symbols on {exchange_name}")
                except Exception as e:
                    self.logger.error(f"Failed to subscribe on {exchange_name}: {e}")
    
    async def unsubscribe(self, symbols: List[str]) -> None:
        """
        Dynamically unsubscribe from symbols.
        
        Args:
            symbols: List of normalized symbols to unsubscribe from (e.g., ["BTC/USDT"])
        """
        if not symbols:
            return
        
        # Remove from active symbols set
        symbols_to_remove = set(symbols) & self.active_symbols
        if not symbols_to_remove:
            self.logger.debug(f"Symbols not in active subscriptions: {symbols}")
            return
        
        self.active_symbols -= symbols_to_remove
        self.logger.info(f"Removing {len(symbols_to_remove)} symbols from active subscriptions: {list(symbols_to_remove)}")
        
        # Unsubscribe from all active connections
        for exchange_name, ws in self.connections.items():
            if not ws.closed and self.connection_status.get(exchange_name, False):
                try:
                    if exchange_name == 'bingx':
                        # BingX unsubscription format
                        for symbol in symbols_to_remove:
                            bingx_symbol = symbol.replace('/', '-')
                            unsub_msg = {
                                "id": f"unsub_{bingx_symbol}",
                                "reqType": "unsub",
                                "dataType": f"{bingx_symbol}@ticker"
                            }
                            await ws.send_json(unsub_msg)
                            self.logger.debug(f"Sent unsubscribe for {bingx_symbol} on BingX")
                    
                    elif exchange_name == 'bybit':
                        # Bybit unsubscription format (batched)
                        bybit_symbols = [symbol.replace('/', '') for symbol in symbols_to_remove]
                        unsub_msg = {
                            "op": "unsubscribe",
                            "args": [f"tickers.{s}" for s in bybit_symbols]
                        }
                        await ws.send_json(unsub_msg)
                        self.logger.debug(f"Sent unsubscribe for {len(bybit_symbols)} symbols on Bybit")
                    
                    self.logger.info(f"Successfully unsubscribed from {len(symbols_to_remove)} symbols on {exchange_name}")
                except Exception as e:
                    self.logger.error(f"Failed to unsubscribe on {exchange_name}: {e}")
    
    def get_queue(self) -> asyncio.Queue:
        """
        Get the message queue for consuming price updates.
        
        Returns:
            Async queue with normalized price data
        """
        return self.message_queue
    
    def get_latest_price(self, exchange: str, symbol: str) -> Optional[Dict]:
        """
        Get last known price for a symbol (synchronous access).
        
        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            
        Returns:
            Latest price data or None
        """
        return self.latest_prices.get(exchange, {}).get(symbol)
    
    def get_connection_status(self) -> Dict[str, bool]:
        """
        Get connection status for all exchanges.
        
        Returns:
            Dictionary of exchange: connected status
        """
        return self.connection_status.copy()
    
    async def start(self, symbols: List[str], exchanges: List[str] = None) -> None:
        """
        Start WebSocket connections for specifying enabled exchanges or all enabled if none given.
        
        Args:
            symbols: List of normalized symbols to monitor (e.g., ["BTC/USDT"])
            exchanges: Optional list of exchange names to connect to. If None, connects to all enabled in config.
        """
        self.running = True
        
        # Initialize active symbols
        self.active_symbols = set(symbols)
        
        # Filter exchanges to start
        all_supported = ['bingx', 'bybit', 'bitget', 'gateio', 'htx', 'phemex', 'mexc']
        
        if exchanges:
            # Only use exchanges that were specifically requested AND supported
            exchanges_to_start = [e for e in exchanges if e in all_supported]
            self.logger.info(f"Starting WebSocket Manager for {len(symbols)} symbols on requested exchanges: {exchanges_to_start}")
        else:
            # Default behavior: use all supported exchanges
            exchanges_to_start = all_supported
            self.logger.info(f"Starting WebSocket Manager for {len(symbols)} symbols on all supported exchanges")
        
        # Start connection tasks for each TARGET exchange
        # Note: connect_exchange will auto-subscribe to active_symbols on connection
        for exchange_name in exchanges_to_start:
            # Still check the 'enabled' flag in config for each exchange
            if self.config['websocket'].get(exchange_name, {}).get('enabled', True):
                task = asyncio.create_task(
                    self.connect_exchange(exchange_name)
                )
                self.tasks.append(task)
            else:
                self.logger.debug(f"Skipping {exchange_name} (disabled in config)")
        
        self.logger.info("WebSocket Manager started")
    
    async def stop(self) -> None:
        """
        Gracefully stop all WebSocket connections.
        """
        self.logger.info("Stopping WebSocket Manager...")
        self.running = False
        
        # Close all WebSocket connections
        for exchange_name, ws in self.connections.items():
            if not ws.closed:
                await ws.close()
                self.logger.info(f"Closed connection to {exchange_name}")
        
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Clear queue
        while not self.message_queue.empty():
            try:
                self.message_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        
        self.logger.info("WebSocket Manager stopped")


async def main():
    """Test WebSocket Manager standalone."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test WebSocket Manager')
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
    
    args = parser.parse_args()
    
    # Create manager
    manager = WebSocketManager(config_path=args.config)
    
    # Start connections
    await manager.start(args.symbols)
    
    # Consume messages
    print(f"\n{'='*60}")
    print(f"WebSocket Manager - Monitoring {', '.join(args.symbols)}")
    print(f"{'='*60}\n")
    print("Press Ctrl+C to stop\n")
    
    try:
        queue = manager.get_queue()
        while True:
            try:
                # Get message with timeout
                data = await asyncio.wait_for(queue.get(), timeout=1.0)
                
                print(
                    f"[{data['exchange'].upper()}] {data['symbol']}: "
                    f"Bid={data['bid']:.2f}, Ask={data['ask']:.2f}, "
                    f"Spread={data['ask'] - data['bid']:.2f}"
                )
            
            except asyncio.TimeoutError:
                # Check connection status
                status = manager.get_connection_status()
                if not any(status.values()):
                    print("All connections lost!")
                    break
    
    except KeyboardInterrupt:
        print("\n\nShutting down...")
    
    finally:
        await manager.stop()


if __name__ == '__main__':
    asyncio.run(main())
