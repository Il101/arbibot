"""
Telegram Signal Manager

Listens for trading signals from Telegram, validates them using historical ADF tests,
and confirms real-time viability using live spreads (Z-Score and Net Spread).
Replies to signals with ✅ when confirmed.
"""

import asyncio
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from telethon import TelegramClient, events
from telethon.tl.types import Message

from services.historical_validator import HistoricalValidator
from services.live_monitor import LiveMonitor
from core.event_bus import EventBus
from utils.logger import get_logger
from utils.symbol_resolver import SymbolResolver
from utils.config import get_config

from telethon.sessions import StringSession

class TelegramSignalManager:
    """
    Manages Telegram connectivity and orchestrates signal validation.
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        self.logger = get_logger(__name__)
        self.config_path = config_path
        
        # Load config
        full_config = get_config(config_path)
        self.tg_config = full_config.get('telegram', {})
        
        self.validator = HistoricalValidator(config_path)
        self.monitor = LiveMonitor(config_path)
        self.event_bus = EventBus.instance()
        self.resolver = SymbolResolver(full_config)
        
        self.enabled = self.tg_config.get('enabled', False)
        self.api_id = self.tg_config.get('api_id')
        self.api_hash = self.tg_config.get('api_hash')
        self.session_name = self.tg_config.get('session_name', 'arbibot_session')
        self.session_string = self.tg_config.get('session_string')
        self.channels = self.tg_config.get('channels', [])
        self.signal_timeout = self.tg_config.get('signal_timeout', 1800)
        
        # Performance/Algo settings
        self.adf_candles = self.tg_config.get('adf_lookback_candles', 1000)
        self.adf_timeframe = self.tg_config.get('adf_timeframe', '15m')
        
        self.client: Optional[TelegramClient] = None
        self.active_signals: Dict[str, asyncio.Task] = {} # symbol -> monitoring task
        
        # 1. Specialized Headers: 
        # Form 1: "GAIB: ..."
        self.header_gaib_regex = re.compile(r'\b([A-Z0-9]{2,10}):', re.IGNORECASE)
        # Form 2: "PTBUSDT - ТЕК"
        self.header_tek_regex = re.compile(r'\b([A-Z0-9]{2,10})USDT\s*-\s*(?:ТЕК|ТEXT)', re.IGNORECASE)
        
        # 2. Emoji-coded Book Lines: 📗|gateio| - LONG or 📗|| - LONG (after URL stripping)
        # Extracts: emoji (color), exchange_name (optional), direction
        self.book_line_regex = re.compile(
            r'([📗📕])\s*\|\s*([^|]*)\s*\|\s*-\s*(LONG|SHORT|BUY|SELL)', 
            re.IGNORECASE
        )
        
        # 3. Refined Spread Values (Various formats)
        # Category A: Course/Target Spreads (High Priority)
        self.spread_format_course_1 = re.compile(r'(?:КУРСОВОЙ|КУРС|КУРСО|КУР):\s*(-?\d+[.,]\d+)\s*%', re.IGNORECASE)
        self.spread_format_course_2 = re.compile(r'(-?\d+[.,]\d+)\s*%\s*(?:КУРСОВОЙ|КУРС|КУРСО|КУР)', re.IGNORECASE)
        self.spread_format_course_3 = re.compile(r'СПРЕД:\s*(-?\d+[.,]\d+)\s*%', re.IGNORECASE)
        # Catch spread in the same line as the symbol: "RIVER: bitget-bybit 3.92%"
        self.spread_format_course_header = re.compile(r'^[A-Z0-9]+:.*?\s*(-?\d+[.,]\d+)\s*%', re.IGNORECASE | re.MULTILINE)
        
        # Category B: Current/Live Spreads (Low Priority Fallback)
        self.spread_format_current = re.compile(r'(?:ТЕКУЩИЙ|ТЕКУЩАЯ):\s*(-?\d+[.,]\d+)\s*%?', re.IGNORECASE)
        
        # Regular fallback for simple pairs (if needed)
        self.pair_regex = re.compile(r'\b([A-Z0-9]{2,10})/(USDT|USDC|BUSD)\b', re.IGNORECASE)
        
        # Blacklist for common false positives
        self.symbol_blacklist = {
            'HTTPS', 'HTTP', 'TRADE', 'INFO', 'HELP', 'LIMIT', 'MARKET', 'ТЕК', 'TEXT',
            'CHART', 'FOLLOW', 'GRAPH', 'ГРАФИК', 'ГРАФ', 'СЛЕДИТЬ'
        }
        
        # Exchange name mapping: signal name -> internal exchange ID
        # Maps variations of exchange names in signals to our internal identifiers
        self.exchange_name_map = {
            # BingX
            'bingx': 'bingx',
            # Bybit
            'bybit': 'bybit',
            # Bitget
            'bitget': 'bitget',
            # Gate.io (various formats)
            'gate.io': 'gateio',
            'gateio': 'gateio',
            'gate': 'gateio',
            # Huobi / HTX
            'huobi': 'htx',
            'htx': 'htx',
            # Phemex
            'phemex': 'phemex',
            # MEXC
            'mexc': 'mexc',
        }
        
        # All supported exchanges for strict matching
        self.supported_exchanges = set(self.exchange_name_map.values())
        
        self.logger.info(f"TelegramSignalManager initialized with {len(self.supported_exchanges)} supported exchanges")

    async def start(self):
        """Start the Telegram client and listeners."""
        if not self.enabled:
            self.logger.warning("Telegram integration is disabled in config")
            return

        if not self.api_id or not self.api_hash:
            self.logger.error("Telegram API credentials missing. Please check config/config.yaml or environment variables")
            return

        if self.session_string:
            self.logger.info("Connecting to Telegram using StringSession...")
            self.client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
        else:
            self.logger.info(f"Connecting to Telegram (Session File: {self.session_name})...")
            self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        
        @self.client.on(events.NewMessage(chats=self.channels))
        async def handle_new_message(event: events.NewMessage.Event):
            chat = await event.get_chat()
            self.logger.info(f"📨 NEW MESSAGE from '{chat.title if hasattr(chat, 'title') else chat.username}' (ID: {event.chat_id})")
            await self._process_message(event.message)

        await self.client.start()
        self.logger.info("✅ Telegram connected and listening for signals")
        
        # Keep it running
        await self.client.run_until_disconnected()

    async def stop(self):
        """Stop the client and all active monitoring tasks."""
        if self.client:
            await self.client.disconnect()
        
        for task in self.active_signals.values():
            task.cancel()
        
        await self.monitor.stop()
        self.logger.info("TelegramSignalManager stopped")

    async def _process_message(self, message: Message):
        """Parse message for symbols and start validation if found."""
        if not message.text:
            return
            
        text = message.text.upper()
        
        # Pre-process: Strip Telegram Markdown & URLs (ORDER MATTERS!)
        # Step 1: Convert Markdown links to plain text FIRST: [bingx](url) -> bingx
        text = re.sub(r'\[([^\]]+)\]\([^)]*\)', r'\1', text)
        # Step 2: Remove bold/italic markers (**, *, _, ~)
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)  # **bold** -> bold
        text = re.sub(r'[_*~]', '', text)  # Remove remaining markers
        # Step 3: Remove backticks (code formatting)
        text = re.sub(r'`', '', text)
        # Step 4: Clean up any remaining URLs
        text = re.sub(r'HTTPS?://\S+', '', text)
        
        self.logger.debug(f"📩 Processing message from {message.chat_id}: {text[:100]}...")
        self.logger.info(f"🔍 RAW MESSAGE from chat {message.chat_id}: {message.text[:200]}")  # NEW: Full visibility
        self.logger.info(f"🧹 CLEANED TEXT (after preprocessing): {text[:200]}")  # NEW: Show processed text
        
        symbols_found = set()
        
        # --- Parsing Strategy ---
        
        # 1. Check for Structured Headers
        # Use findall to find all candidates and pick the first one not in blacklist
        gaib_matches = self.header_gaib_regex.findall(text)
        tek_matches = self.header_tek_regex.findall(text)
        
        base_token = None
        # Check Form 1
        for token_candidate in gaib_matches:
            token_candidate = token_candidate.upper()
            if token_candidate not in self.symbol_blacklist:
                base_token = token_candidate
                break
        
        # Check Form 2 if Form 1 didn't find anything
        if not base_token:
            for token_candidate in tek_matches:
                token_candidate = token_candidate.upper()
                if token_candidate not in self.symbol_blacklist:
                    base_token = token_candidate
                    break
            
        if base_token:
            symbols_found.add(f"{base_token}/USDT")
            self.logger.debug(f"💎 Found structured header for token: {base_token}")
            self.logger.info(f"✅ HEADER MATCH: {base_token}/USDT")  # NEW: Confirm header detection
        else:
            # Fallback for simple "BTC/USDT" format
            pair_matches = self.pair_regex.findall(text)
            for base, quote in pair_matches:
                if base not in self.symbol_blacklist:
                    symbols_found.add(f"{base}/{quote}")

        if not symbols_found:
            self.logger.debug(f"ℹ️ No trading symbols found in message")
            self.logger.info(f"⚠️ NO SYMBOLS DETECTED. Text preview: {text[:150]}")  # NEW: Show why nothing matched
            return

        # 2. Extract Specialized Metadata
        
        # Extract reported spread strictly preferring "Курсовой:"
        reported_spread = 0.0
        spread_match = (
            self.spread_format_course_1.search(text) or 
            self.spread_format_course_2.search(text) or 
            self.spread_format_course_3.search(text) or
            self.spread_format_course_header.search(text)
        )
        
        if spread_match:
            try:
                val_str = spread_match.group(1).replace(',', '.')
                reported_spread = float(val_str) / 100
                self.logger.debug(f"📊 Extracted Курсовой spread: {reported_spread:.2%}")
            except (ValueError, IndexError):
                pass
        
        # Direction logic: First exchange in text is LONG, second is SHORT.
        # Find all occurrences of supported exchanges in order of appearance.
        exchanges_mentioned = []
        # Create a combined regex for all supported exchange names
        ex_names = sorted(list(self.exchange_name_map.keys()), key=len, reverse=True)
        ex_regex = re.compile(r'\b(' + '|'.join(map(re.escape, ex_names)) + r')\b', re.IGNORECASE)
        
        found_ex_matches = ex_regex.finditer(text)
        
        for m in found_ex_matches:
            ex_name_raw = m.group(1).lower()
            internal_name = self.exchange_name_map[ex_name_raw]
            if internal_name not in exchanges_mentioned:
                exchanges_mentioned.append(internal_name)
        
        # Strict Rule: 1st=LONG, 2nd=SHORT
        direction = "SHORT" # Signal recommendation is for the bot's action (usually the second side)
        if len(exchanges_mentioned) >= 2:
            self.logger.info(f"🏛️ Exchange order: 1st={exchanges_mentioned[0]} (LONG), 2nd={exchanges_mentioned[1]} (SHORT)")
        elif exchanges_mentioned:
            self.logger.info(f"🏛️ Single exchange detected: {exchanges_mentioned[0]}")
        
        # Log detected exchanges
        if exchanges_mentioned:
            self.logger.info(f"🏛️ Exchanges in signal: {exchanges_mentioned}")

        for symbol in symbols_found:
            # Check for manual mapping in config first
            manual_map = self.tg_config.get('symbol_mapping', {})
            base, quote = symbol.split('/')
            if base in manual_map:
                symbol = f"{manual_map[base]}/{quote}"
                self.logger.info(f"🔄 Symbol remapped via config: {base} -> {manual_map[base]}")

            # Resolve exchange-specific symbols
            ex_a, ex_b = pair if 'pair' in locals() else ('bingx', 'bybit')
            ex_a_obj = self.validator.exchanges.get(ex_a)
            ex_b_obj = self.validator.exchanges.get(ex_b)
            
            # Resolve if obj exists, otherwise validator.analyze will handle it later
            # but we need it here for immediate logging if possible
            # To be safe, we just use the names for now as resolve() needs exchange objects
            # which might not be initialized yet in validator.
            
            self.logger.info(
                f"📍 Signal detected for {symbol} | Direction: {direction} | Spread: {reported_spread:.2%}"
            )
            
            # Start asynchronous validation/monitoring flow
            if symbol not in self.active_signals or self.active_signals[symbol].done():
                # Determine exchange pair (Strict Rule: 1st=LONG, 2nd=SHORT)
                if len(exchanges_mentioned) >= 2:
                    pair = (exchanges_mentioned[0], exchanges_mentioned[1])
                    self.logger.info(f"⚖️ Arbitrage Pair: LONG on {pair[0]} | SHORT on {pair[1]}")
                elif len(exchanges_mentioned) == 1:
                    # One exchange mentioned, use bybit as fallback second exchange (SHORT side)
                    pair = (exchanges_mentioned[0], 'bybit') if exchanges_mentioned[0] != 'bybit' else ('bingx', 'bybit')
                else:
                    pair = ('bingx', 'bybit')
                
                metadata = {
                    'direction': direction, 
                    'reported_spread': reported_spread,
                    'pair': pair,
                    'is_custom_pair': len(exchanges_mentioned) >= 2
                }
                task = asyncio.create_task(self._validate_and_confirm(symbol, message, metadata))
                self.active_signals[symbol] = task

    async def _validate_and_confirm(self, symbol: str, original_msg: Message, metadata: dict):
        """
        Flow:
        1. Historical ADF check
        2. Filter based on reported spread (if configured)
        3. If passed -> Start WebSockets for live monitoring
        4. Wait for Z-Score/Spread confirmation + Direction Match
        5. If confirmed -> Reply ✅
        """
        try:
            # Check reported spread filter (using absolute value for magnitude)
            min_repo_spread = self.tg_config.get('min_signal_spread_pct', 0.0)
            if abs(metadata['reported_spread']) < min_repo_spread:
                self.logger.info(
                    f"⏩ {symbol} reported spread {metadata['reported_spread']:.2%} "
                    f"is below minimum {min_repo_spread:.2%}. Skipping."
                )
                return

            # 1. Historical Validation (ADF test)
            ex_a, ex_b = metadata.get('pair', ('bingx', 'bybit'))
            self.logger.info(f"🔍 Running ADF test for {symbol} on {ex_a}/{ex_b}...")
            results = await self.validator.analyze(
                symbol=symbol,
                ex_a=ex_a,
                ex_b=ex_b,
                timeframe=self.adf_timeframe, 
                limit=self.adf_candles
            )
            
            if not results.get('is_stationary', False):
                self.logger.info(f"❌ {symbol} failed ADF stationarity check. Ignoring signal.")
                return

            self.logger.info(f"✅ {symbol} passed ADF test. Starting live monitoring...")

            # 2. Live Monitoring (Z-Score + Spread)
            pair = metadata.get('pair', ('bingx', 'bybit'))
            self.logger.info(f"🚀 Starting live monitoring for {symbol} on {pair}...")
            
            # Add symbol to live monitor (ensure monitor is running)
            if not self.monitor.running:
                await self.monitor.start([symbol], pair=pair)
            else:
                await self.monitor.start([symbol], pair=pair) 
            # 3. Wait loop for confirmation
            start_time = time.time()
            confirmed = False
            last_stats = None
            status_msg = None
            
            while time.time() - start_time < self.signal_timeout:
                stats = self.monitor.get_current_stats(symbol)
                if stats:
                    last_stats = stats
                    z_score = stats.get('z_score', 0)
                    net_spread_pct = stats.get('net_spread', 0)
                    
                    # Send status message once we have the first Z-score
                    if status_msg is None:
                        try:
                            self.logger.info(f"📤 Sending initial monitoring message for {symbol} to chat {original_msg.chat_id}...")
                            status_msg = await self.client.send_message(
                                original_msg.chat_id,
                                f"🔍 Monitoring {symbol}...\n"
                                f"Initial Z-Score: `{z_score:.2f}`\n"
                                f"Target Z-Score: > `{self.tg_config.get('z_score_entry', 2.5)}`",
                                reply_to=original_msg.id
                            )
                        except Exception as e:
                            self.logger.error(f"Failed to send initial message: {e}")
                            # Continue anyway, we might still confirm later
                            status_msg = False # Mark as attempted but failed

                    # Check conditions
                    z_threshold = self.tg_config.get('z_score_entry', 2.5)
                    z_cond = abs(z_score) > z_threshold
                    spread_cond = net_spread_pct > 0
                    
                    self.logger.debug(
                        f"👀 Checking {symbol}: Z={z_score:.2f} (Target > {z_threshold}), "
                        f"Spread={net_spread_pct:.2f}% (Target > 0%)"
                    )
                    
                    # Direction Match Check
                    dir_cond = True
                    if self.tg_config.get('require_direction_match', False) and metadata['direction']:
                        # Logic to verify if Z-score direction matches signal recommendation
                        pass
                    
                    if z_cond and spread_cond and dir_cond:
                        confirmed = True
                        break
                
                await asyncio.sleep(5)
            
            if confirmed and last_stats:
                z_score = last_stats.get('z_score', 0)
                net_spread = last_stats.get('net_spread', 0)
                self.logger.info(f"🚀 Signal CONFIRMED for {symbol}! Replying to Telegram...")
                
                conf_text = f"✅ **Confirmed!**\nZ-Score: `{z_score:.2f}`\nNet Spread: `{net_spread:.2f}%`"
                if status_msg and status_msg is not True:
                    try:
                        await self.client.edit_message(
                            original_msg.chat_id,
                            status_msg.id,
                            conf_text
                        )
                        self.logger.info(f"✅ Confirmation message edited for {symbol}")
                    except Exception as e:
                        self.logger.warning(f"Failed to edit message, sending new reply: {e}")
                        await self.client.send_message(original_msg.chat_id, conf_text, reply_to=original_msg.id)
                else:
                    await self.client.send_message(original_msg.chat_id, conf_text, reply_to=original_msg.id)
                    self.logger.info(f"✅ Confirmation message sent for {symbol}")
            else:
                self.logger.info(f"⏳ Signal for {symbol} timed out without confirmation.")
                if status_msg and status_msg is not True:
                    try:
                        last_z = f"`{last_stats['z_score']:.2f}`" if last_stats else "N/A"
                        await self.client.edit_message(
                            original_msg.chat_id,
                            status_msg.id,
                            f"⏳ Monitoring timed out for {symbol}.\nLast Z-Score: {last_z}"
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to edit timeout message: {e}")

        except Exception as e:
            self.logger.error(f"Error in signal confirmation flow for {symbol}: {e}", exc_info=True)
        finally:
            # Cleanup if this was the only symbol or handle as needed
            # For now we keep monitoring until stopped or next signal
            pass

if __name__ == "__main__":
    # Test stub
    import sys
    loop = asyncio.get_event_loop()
    manager = TelegramSignalManager()
    try:
        loop.run_until_complete(manager.start())
    except KeyboardInterrupt:
        loop.run_until_complete(manager.stop())
