"""
Historical Validator Service

Validates arbitrage opportunities using historical data analysis.
Performs stationarity tests, Z-Score analysis, and profitability assessment.
"""

import sys
from pathlib import Path
from typing import Dict, Optional, Tuple
from datetime import datetime
import argparse

import ccxt
import pandas as pd
import numpy as np
import matplotlib
import os

# Set non-interactive backend for headless environments (Railway)
if os.environ.get('DISPLAY') is None:
    matplotlib.use('Agg')

import matplotlib.pyplot as plt
import seaborn as sns
import yaml

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from utils.metrics import calculate_z_score, adf_test, calculate_spread, calculate_spread_stats
from utils.logger import get_logger
from utils.symbol_resolver import SymbolResolver


class HistoricalValidator:
    """
    Validates arbitrage pairs using historical OHLCV data.
    
    Performs statistical analysis including:
    - Stationarity testing (ADF test)
    - Z-Score calculation for mean reversion
    - Profitability assessment based on spread vs fees
    """
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialize the Historical Validator.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.logger = get_logger(__name__)
        self.config = self._load_config(config_path)
        self.exchanges = self._setup_exchanges()
        self.resolver = SymbolResolver(self.config)
        
        self.logger.info("HistoricalValidator initialized successfully")
    
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
        """Return default configuration if config file is missing."""
        return {
            'trading': {
                'z_score_entry': 2.5,
                'min_spread_pct': 0.003,
                'estimated_fee': 0.002
            },
            'validation': {
                'timeframe': '15m',
                'candles_limit': 1000,
                'adf_pvalue_threshold': 0.05,
                'z_score_window': 20
            },
            'exchanges': {
                'bingx': {'enabled': True, 'default_type': 'swap'},
                'bybit': {'enabled': True, 'default_type': 'linear'}
            }
        }
    
    def _setup_exchanges(self) -> Dict[str, ccxt.Exchange]:
        """
        Initialize CCXT exchange instances.
        
        Returns:
            Dictionary of exchange instances
        """
        exchanges = {}
        
        try:
            # BingX setup
            if self.config['exchanges']['bingx']['enabled']:
                exchanges['bingx'] = ccxt.bingx({
                    'enableRateLimit': True,
                    'options': {
                        'defaultType': self.config['exchanges']['bingx']['default_type']
                    }
                })
                self.logger.debug("BingX exchange initialized")
            
            # Bybit setup
            if self.config['exchanges']['bybit']['enabled']:
                exchanges['bybit'] = ccxt.bybit({
                    'enableRateLimit': True,
                    'options': {
                        'defaultType': self.config['exchanges']['bybit']['default_type']
                    }
                })
                self.logger.debug("Bybit exchange initialized")
            
            # Pre-load markets for all exchanges
            for name, ex in exchanges.items():
                self.logger.info(f"🔄 Pre-loading markets for {name}...")
                ex.load_markets()
                
            return exchanges
            
        except Exception as e:
            self.logger.error(f"Error setting up exchanges: {e}")
            raise
    
    async def fetch_ohlcv(
        self,
        exchange: ccxt.Exchange,
        symbol: str,
        timeframe: str,
        limit: int
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical OHLCV data from exchange with pagination support.
        """
        ex_id = exchange.id
        
        # Resolve exchange-specific symbol
        resolved_symbol = await self.resolver.resolve(exchange, symbol)
        if not resolved_symbol:
            self.logger.error(f"❌ Could not resolve {symbol} for {ex_id}")
            return None
            
        self.logger.info(f"Fetching {limit} candles of {resolved_symbol} from {ex_id}")
        
        try:
            all_ohlcv = []
            duration_ms = exchange.parse_timeframe(timeframe) * 1000
            now = exchange.milliseconds()
            since = now - (limit * duration_ms)
            
            # Pagination loop
            while len(all_ohlcv) < limit:
                fetch_limit = min(limit - len(all_ohlcv), 1000)
                try:
                    ohlcv = exchange.fetch_ohlcv(
                        resolved_symbol,
                        timeframe,
                        since=since,
                        limit=fetch_limit
                    )
                    if not ohlcv:
                        break
                    
                    all_ohlcv.extend(ohlcv)
                    since = ohlcv[-1][0] + duration_ms
                    if since > now:
                        break
                except Exception as e:
                    self.logger.error(f"Error fetching chunk from {ex_id}: {e}")
                    break
            
            if not all_ohlcv:
                return None

            # Convert to DataFrame
            df = pd.DataFrame(
                all_ohlcv,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.drop_duplicates(subset='timestamp')
            
            self.logger.info(f"Successfully fetched {len(df)} candles from {ex_id}")
            return df[['timestamp', 'close']]
            
        except Exception as e:
            self.logger.error(f"Critical error in fetch_ohlcv for {ex_id}: {e}")
            return None
    
    async def analyze(
        self,
        symbol: str,
        ex_a: str = 'bingx',
        ex_b: str = 'bybit',
        timeframe: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict:
        """
        Perform comprehensive arbitrage analysis on a trading pair.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            ex_a: First exchange ID
            ex_b: Second exchange ID
            timeframe: Candle timeframe (defaults to config value)
            limit: Number of candles (defaults to config value)
        """
        # Ensure exchanges are available
        for ex_id in [ex_a, ex_b]:
            if ex_id not in self.exchanges:
                self.logger.info(f"Adding {ex_id} to validator dynamically...")
                try:
                    # Generic CCXT setup for additional exchanges
                    ex_class = getattr(ccxt, ex_id)
                    ex_config = self.config.get('exchanges', {}).get(ex_id, {})
                    
                    self.exchanges[ex_id] = ex_class({
                        'enableRateLimit': True,
                        'options': {
                            'defaultType': ex_config.get('default_type', 'swap')
                        }
                    })
                    await self.exchanges[ex_id].load_markets()
                except Exception as e:
                    self.logger.error(f"Failed to initialize {ex_id}: {e}")
                    return {'symbol': symbol, 'error': f'Exchange {ex_id} not supported'}

        # Use config defaults if not provided
        if timeframe is None:
            timeframe = self.config['validation']['timeframe']
        if limit is None:
            limit = self.config['validation']['candles_limit']
        
        self.logger.info(f"Analyzing {symbol} on {ex_a}/{ex_b} | {timeframe}, {limit} candles")
        
        # Fetch data from both exchanges
        df_a = await self.fetch_ohlcv(self.exchanges[ex_a], symbol, timeframe, limit)
        df_b = await self.fetch_ohlcv(self.exchanges[ex_b], symbol, timeframe, limit)
        
        if df_a is None or df_b is None:
            self.logger.error(f"Failed to fetch data from {ex_a} or {ex_b}")
            return {
                'symbol': symbol,
                'error': 'Data fetch failed',
                'is_stationary': False,
                'adf_pvalue': 1.0,
                'max_spread_pct': 0.0,
                'z_score_signals': 0,
                'is_profitable': False
            }
        
        # Merge dataframes on timestamp (inner join)
        df_a = df_a.rename(columns={'close': f'{ex_a}_close'})
        df_b = df_b.rename(columns={'close': f'{ex_b}_close'})
        
        df = pd.merge(df_a, df_b, on='timestamp', how='inner')
        df = df.set_index('timestamp')
        
        self.logger.info(f"Data aligned: {len(df)} overlapping periods")
        
        if len(df) < 50:
            self.logger.warning("Insufficient overlapping data for analysis")
            return {
                'symbol': symbol,
                'error': 'Insufficient data',
                'is_stationary': False,
                'adf_pvalue': 1.0,
                'max_spread_pct': 0.0,
                'z_score_signals': 0,
                'is_profitable': False
            }
        
        # Calculate spread
        df['spread'] = df[f'{ex_a}_close'] - df[f'{ex_b}_close']
        df['spread_pct'] = df['spread'].abs() / df[f'{ex_a}_close']
        
        # Run ADF test for stationarity
        is_stationary, adf_pvalue, adf_details = adf_test(df['spread'])
        
        self.logger.info(f"ADF Test: p-value={adf_pvalue:.4f}, stationary={is_stationary}")
        
        # Calculate Z-Score
        z_score_window = self.config['validation']['z_score_window']
        df['z_score'] = calculate_z_score(df['spread'], window=z_score_window)
        
        # Count signals where |Z-Score| > threshold
        z_threshold = self.config['trading']['z_score_entry']
        z_score_signals = len(df[df['z_score'].abs() > z_threshold])
        
        # Calculate max spread percentage
        max_spread_pct = df['spread_pct'].max()
        
        # Determine profitability
        min_spread = self.config['trading']['min_spread_pct']
        is_profitable = max_spread_pct > min_spread
        
        self.logger.info(
            f"Analysis complete: max_spread={max_spread_pct*100:.4f}%, "
            f"signals={z_score_signals}, profitable={is_profitable}"
        )
        
        # Store metadata for plotting
        self._last_analysis_df = df
        self._last_ex_a = ex_a
        self._last_ex_b = ex_b
        
        return {
            'symbol': symbol,
            'is_stationary': is_stationary,
            'adf_pvalue': adf_pvalue,
            'max_spread_pct': max_spread_pct,
            'z_score_signals': z_score_signals,
            'is_profitable': is_profitable,
            'data_points': len(df)
        }
    
    def plot_analysis(
        self,
        symbol: str,
        save_path: Optional[str] = None,
        show: bool = True
    ) -> None:
        """
        Generate visualization of analysis results.
        
        Creates a 3-subplot figure showing:
        1. Price history from both exchanges
        2. Spread over time
        3. Z-Score with entry/exit thresholds
        
        Args:
            symbol: Trading pair symbol
            save_path: Path to save plot (default: 'analysis_plot.png')
            show: Whether to display plot interactively
        """
        if not hasattr(self, '_last_analysis_df'):
            self.logger.error("No analysis data available. Run analyze() first.")
            return
        
        df = self._last_analysis_df
        
        if save_path is None:
            save_path = 'analysis_plot.png'
        
        self.logger.info(f"Generating plot for {symbol}")
        
        # Setup plot style
        sns.set_theme(style="darkgrid")
        fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
        
        # Plot 1: Prices
        axes[0].plot(
            df.index,
            df[f'{self._last_ex_a}_close'],
            label=self._last_ex_a.capitalize(),
            color='blue',
            linewidth=2,
            alpha=0.6
        )
        axes[0].plot(
            df.index,
            df[f'{self._last_ex_b}_close'],
            label=self._last_ex_b.capitalize(),
            color='orange',
            linewidth=1,
            linestyle='--',
            alpha=0.9
        )
        axes[0].set_title(f'{symbol} Price History (Perpetual Futures)')
        axes[0].set_ylabel('Price (USDT)')
        axes[0].legend()
        
        # Plot 2: Spread
        axes[1].plot(
            df.index,
            df['spread'],
            label=f'Spread ({self._last_ex_a} - {self._last_ex_b})',
            color='purple',
            linewidth=1
        )
        axes[1].axhline(0, color='black', linestyle='-', alpha=0.3)
        axes[1].set_title('Price Spread (USDT)')
        axes[1].set_ylabel('Spread (USDT)')
        axes[1].legend()
        
        # Plot 3: Z-Score
        z_threshold = self.config['trading']['z_score_entry']
        
        axes[2].plot(
            df.index,
            df['z_score'],
            label=f'Z-Score ({self.config["validation"]["z_score_window"]} period)',
            color='green',
            linewidth=1
        )
        axes[2].axhline(z_threshold, color='red', linestyle='--', alpha=0.6, label=f'Entry Threshold (±{z_threshold})')
        axes[2].axhline(-z_threshold, color='red', linestyle='--', alpha=0.6)
        axes[2].axhline(0, color='black', linestyle='-', alpha=0.4, label='Mean')
        
        # Highlight entry zones
        axes[2].fill_between(
            df.index,
            z_threshold,
            df['z_score'],
            where=(df['z_score'] > z_threshold),
            color='red',
            alpha=0.3
        )
        axes[2].fill_between(
            df.index,
            -z_threshold,
            df['z_score'],
            where=(df['z_score'] < -z_threshold),
            color='red',
            alpha=0.3
        )
        
        axes[2].set_title('Z-Score of Spread')
        axes[2].set_ylabel('Z-Score')
        axes[2].set_xlabel('Time')
        axes[2].legend(loc='upper left')
        
        # Add summary text
        max_spread_pct = df['spread_pct'].max()
        z_score_signals = len(df[df['z_score'].abs() > z_threshold])
        min_spread = self.config['trading']['min_spread_pct']
        estimated_fee = self.config['trading']['estimated_fee']
        
        summary_text = (
            f"ANALYSIS SUMMARY:\n"
            f"- Estimated Fee Threshold: {estimated_fee*100:.2f}%\n"
            f"- Min Spread Required: {min_spread*100:.2f}%\n"
            f"- Max Spread Found: {max_spread_pct*100:.4f}%\n"
            f"- Z-Score Signals (|Z|>{z_threshold}): {z_score_signals}\n"
            f"- Data Points: {len(df)}"
        )
        
        plt.figtext(
            0.1, 0.02,
            summary_text,
            fontsize=10,
            bbox={"facecolor": "white", "alpha": 0.8, "pad": 5}
        )
        
        # Adjust layout
        plt.subplots_adjust(bottom=0.15)
        plt.tight_layout()
        
        # Save plot
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        self.logger.info(f"Plot saved to {save_path}")
        
        # Show plot
        if show:
            plt.show()
        
        plt.close()


def main():
    """Command-line interface for historical validation."""
    parser = argparse.ArgumentParser(
        description='Analyze arbitrage opportunities using historical data'
    )
    parser.add_argument(
        'symbol',
        nargs='?',
        default='BTC/USDT',
        help='Trading pair symbol (default: BTC/USDT)'
    )
    parser.add_argument(
        '--timeframe',
        default='15m',
        help='Candle timeframe (default: 15m)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=1000,
        help='Number of candles to fetch (default: 1000)'
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to config file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--no-plot',
        action='store_true',
        help='Skip plot generation'
    )
    parser.add_argument(
        '--save-plot',
        default='analysis_plot.png',
        help='Path to save plot (default: analysis_plot.png)'
    )
    
    args = parser.parse_args()
    
    # Create validator
    validator = HistoricalValidator(config_path=args.config)
    
    # Run analysis
    print(f"\n{'='*60}")
    print(f"Historical Analysis: {args.symbol}")
    print(f"{'='*60}\n")
    
    results = validator.analyze(
        symbol=args.symbol,
        timeframe=args.timeframe,
        limit=args.limit
    )
    
    # Print results
    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"Symbol:              {results['symbol']}")
    print(f"Data Points:         {results.get('data_points', 'N/A')}")
    print(f"Stationary:          {results['is_stationary']}")
    print(f"ADF P-Value:         {results['adf_pvalue']:.6f}")
    print(f"Max Spread:          {results['max_spread_pct']*100:.4f}%")
    print(f"Z-Score Signals:     {results['z_score_signals']}")
    print(f"Profitable:          {results['is_profitable']}")
    
    if 'error' in results:
        print(f"Error:               {results['error']}")
    
    print(f"{'='*60}\n")
    
    # Generate plot
    if not args.no_plot and 'error' not in results:
        validator.plot_analysis(
            symbol=args.symbol,
            save_path=args.save_plot,
            show=True
        )


if __name__ == '__main__':
    main()
