#!/usr/bin/env python3
"""
ArbiBot - Cryptocurrency Arbitrage Bot
Main entry point and orchestrator
"""

import sys
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.logger import setup_logger

# Setup logger
logger = setup_logger('arbibot', level='INFO')


def main():
    """
    Main entry point for ArbiBot
    """
    parser = argparse.ArgumentParser(
        description='ArbiBot - Cryptocurrency Arbitrage Trading Bot'
    )
    
    parser.add_argument(
        'mode',
        choices=['gui', 'scan', 'analyze', 'live', 'telegram'],
        help='Operation mode'
    )
    
    parser.add_argument(
        '--pair',
        type=str,
        help='Trading pair (e.g., BTC/USDT) for analyze mode'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Update log level if debug
    if args.debug:
        logger.setLevel('DEBUG')
        logger.debug('Debug mode enabled')
    
    logger.info(f"Starting ArbiBot in '{args.mode}' mode")
    logger.info(f"Using config: {args.config}")
    
    try:
        if args.mode == 'gui':
            run_gui(args)
        elif args.mode == 'scan':
            run_scanner(args)
        elif args.mode == 'analyze':
            run_analysis(args)
        elif args.mode == 'live':
            run_live_monitor(args)
        elif args.mode == 'telegram':
            run_telegram_manager(args)
    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


def run_gui(args):
    """
    Launch PyQt6 GUI application with qasync integration.
    
    Uses qasync to run asyncio and Qt event loops together,
    allowing WebSocket connections to work alongside the GUI.
    """
    logger.info("Launching GUI...")
    try:
        import asyncio
        from PyQt6.QtWidgets import QApplication
        import qasync
        from gui.main_window import MainWindow
        
        # Create Qt application
        app = QApplication(sys.argv)
        app.setApplicationName("ArbiBot")
        app.setOrganizationName("ArbiBot")
        
        # Setup qasync event loop
        loop = qasync.QEventLoop(app)
        asyncio.set_event_loop(loop)
        
        # Create main window
        window = MainWindow()
        window.show()
        
        # Run event loop
        with loop:
            loop.run_forever()
            
    except ImportError as e:
        logger.error(f"GUI dependencies not installed: {e}")
        logger.error("Install with: pip install PyQt6 pyqtgraph qasync")
        sys.exit(1)
    except Exception as e:
        logger.error(f"GUI error: {e}", exc_info=True)
        sys.exit(1)



def run_scanner(args):
    """
    Run market scanner to find profitable pairs
    """
    logger.info("Running market scanner...")
    try:
        from services.market_scanner import MarketScanner
        
        scanner = MarketScanner(config_path=args.config)
        results = scanner.scan()
        
        logger.info(f"Scan complete. Found {len(results)} profitable pairs")
        logger.info("Results saved to config/whitelist.json")
    except ImportError as e:
        logger.error(f"Scanner module not ready: {e}")
        sys.exit(1)


def run_analysis(args):
    """
    Run historical analysis on a specific pair
    """
    if not args.pair:
        logger.error("--pair argument required for analyze mode")
        logger.error("Example: python main.py analyze --pair BTC/USDT")
        sys.exit(1)
    
    logger.info(f"Analyzing {args.pair}...")
    try:
        from services.historical_validator import HistoricalValidator
        
        validator = HistoricalValidator(config_path=args.config)
        results = validator.analyze(args.pair)
        
        # Display results
        logger.info("\n" + "="*50)
        logger.info(f"Analysis Results for {args.pair}")
        logger.info("="*50)
        logger.info(f"Stationary: {results.get('is_stationary', False)}")
        logger.info(f"ADF P-Value: {results.get('adf_pvalue', 'N/A'):.4f}")
        logger.info(f"Max Spread: {results.get('max_spread_pct', 0):.2%}")
        logger.info(f"Z-Score Signals: {results.get('z_score_signals', 0)}")
        logger.info(f"Profitable: {results.get('is_profitable', False)}")
        logger.info("="*50)
        
    except ImportError as e:
        logger.error(f"Analysis module not ready: {e}")
        sys.exit(1)


def run_telegram_manager(args):
    """
    Run Telegram Signal Manager to listen for signals.
    Includes graceful shutdown with position cleanup.
    """
    logger.info("Starting Telegram Signal Manager...")
    try:
        import asyncio
        import signal as sig
        from services.telegram_manager import TelegramSignalManager
        from services.execution import ExecutionEngine
        
        manager = TelegramSignalManager(config_path=args.config)
        
        # Initialize Execution Engine (Handles Paper/Live automatically based on config)
        execution_engine = ExecutionEngine(
            config_path=args.config,
            ws_manager=manager.monitor.ws_manager
        )
        
        # Graceful shutdown handler
        async def graceful_shutdown():
            logger.warning("🔻 Graceful shutdown initiated...")
            try:
                await execution_engine.shutdown()
            except Exception as e:
                logger.error(f"Error during execution engine shutdown: {e}")
            try:
                await manager.stop()
            except Exception as e:
                logger.error(f"Error during telegram manager stop: {e}")
            logger.info("✅ Shutdown complete.")
        
        def signal_handler(signum, frame):
            logger.warning(f"Received signal {signum}, initiating graceful shutdown...")
            loop = asyncio.get_event_loop()
            loop.create_task(graceful_shutdown())
        
        # Register signal handlers
        sig.signal(sig.SIGTERM, signal_handler)
        sig.signal(sig.SIGINT, signal_handler)
        
        # Run async manager
        try:
            asyncio.run(manager.start())
        except KeyboardInterrupt:
            asyncio.run(graceful_shutdown())
            
    except Exception as e:
        logger.error(f"Telegram manager error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
