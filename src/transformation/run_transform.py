import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
from datetime import datetime, timedelta

class StockDataTransformer:
    def __init__(self, db_connection: str):
        self.engine = create_engine(db_connection)
        self.logger = logging.getLogger(__name__)

    def create_analytics_tables(self):
        """
        Create tables for transformed data.
        """
        try:
            # Create daily_metrics table
            daily_metrics_table = """
            CREATE TABLE IF NOT EXISTS daily_metrics (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open_price FLOAT,
                close_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                volume BIGINT,
                avg_price FLOAT,
                price_volatility FLOAT,
                day_change_percent FLOAT,
                vwap FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (symbol, date)
            );
            """

            # Create technical_indicators table
            technical_indicators_table = """
            CREATE TABLE IF NOT EXISTS technical_indicators (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                sma_20 FLOAT,
                sma_50 FLOAT,
                ema_12 FLOAT,
                ema_26 FLOAT,
                macd FLOAT,
                macd_signal FLOAT,
                rsi FLOAT,
                bollinger_upper FLOAT,
                bollinger_lower FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (symbol, date)
            );
            """

            with self.engine.connect() as conn:
                conn.execute(text(daily_metrics_table))
                conn.execute(text(technical_indicators_table))
                conn.commit()

            self.logger.info("Analytics tables created successfully")

        except Exception as e:
            self.logger.error(f"Error creating analytics tables: {str(e)}")
            raise

    def calculate_daily_metrics(self, days_back: int = 7):
        """
        Calculate daily metrics from stock data.
        """
        try:
            query = """
            SELECT *
            FROM stock_data
            WHERE timestamp >= NOW() - INTERVAL ':days_back days'
            """

            df = pd.read_sql_query(
                text(query), 
                self.engine, 
                params={'days_back': days_back}
            )

            if df.empty:
                self.logger.warning("No data found for processing")
                return

            # Calculate daily metrics
            daily = df.groupby(['symbol', pd.Grouper(key='timestamp', freq='D')]).agg({
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'volume': 'sum'
            }).reset_index()

            # Calculate additional metrics
            daily['avg_price'] = (daily['high'] + daily['low']) / 2
            daily['day_change_percent'] = ((daily['close'] - daily['open']) / daily['open']) * 100
            daily['price_volatility'] = (daily['high'] - daily['low']) / daily['open'] * 100

            # Calculate VWAP
            daily['vwap'] = (daily['volume'] * ((daily['high'] + daily['low'] + daily['close']) / 3)).cumsum() / daily['volume'].cumsum()

            # Prepare for database
            daily_metrics = daily.rename(columns={
                'open': 'open_price',
                'close': 'close_price',
                'high': 'high_price',
                'low': 'low_price',
                'timestamp': 'date'
            })

            # Store in database
            daily_metrics.to_sql(
                'daily_metrics',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

            self.logger.info(f"Processed daily metrics for {len(df['symbol'].unique())} symbols")
            return daily_metrics

        except Exception as e:
            self.logger.error(f"Error calculating daily metrics: {str(e)}")
            raise

    def calculate_technical_indicators(self, days_back: int = 30):
        """
        Calculate technical indicators from stock data.
        """
        try:
            query = """
            SELECT *
            FROM stock_data
            WHERE timestamp >= NOW() - INTERVAL ':days_back days'
            """

            df = pd.read_sql_query(
                text(query), 
                self.engine, 
                params={'days_back': days_back}
            )

            if df.empty:
                self.logger.warning("No data found for technical analysis")
                return

            results = []
            for symbol in df['symbol'].unique():
                symbol_data = df[df['symbol'] == symbol].sort_values('timestamp')

                # Calculate indicators
                sma_20 = symbol_data['close'].rolling(window=20).mean()
                sma_50 = symbol_data['close'].rolling(window=50).mean()
                ema_12 = symbol_data['close'].ewm(span=12, adjust=False).mean()
                ema_26 = symbol_data['close'].ewm(span=26, adjust=False).mean()
                
                # MACD
                macd = ema_12 - ema_26
                macd_signal = macd.ewm(span=9, adjust=False).mean()

                # Bollinger Bands
                bb_middle = symbol_data['close'].rolling(window=20).mean()
                bb_std = symbol_data['close'].rolling(window=20).std()
                bb_upper = bb_middle + (bb_std * 2)
                bb_lower = bb_middle - (bb_std * 2)

                # RSI
                delta = symbol_data['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))

                # Combine indicators
                indicators = pd.DataFrame({
                    'symbol': symbol,
                    'date': symbol_data['timestamp'].dt.date,
                    'sma_20': sma_20,
                    'sma_50': sma_50,
                    'ema_12': ema_12,
                    'ema_26': ema_26,
                    'macd': macd,
                    'macd_signal': macd_signal,
                    'rsi': rsi,
                    'bollinger_upper': bb_upper,
                    'bollinger_lower': bb_lower
                })

                results.append(indicators)

            # Combine all results
            all_indicators = pd.concat(results, ignore_index=True)

            # Store in database
            all_indicators.to_sql(
                'technical_indicators',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

            self.logger.info(f"Processed technical indicators for {len(df['symbol'].unique())} symbols")
            return all_indicators

        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {str(e)}")
            raise

    def run_transformations(self):
        """
        Run all transformations.
        """
        try:
            # Create tables if they don't exist
            self.create_analytics_tables()

            # Calculate metrics and indicators
            daily_metrics = self.calculate_daily_metrics()
            technical_indicators = self.calculate_technical_indicators()

            self.logger.info("All transformations completed successfully")
            
            return {
                'daily_metrics': daily_metrics,
                'technical_indicators': technical_indicators
            }

        except Exception as e:
            self.logger.error(f"Error in transformation pipeline: {str(e)}")
            raise