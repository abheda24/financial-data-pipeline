import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine
import time
from typing import Dict, Any, Optional

class StockDataIngestion:
    def __init__(self, db_connection: str):
        """
        Initialize the stock data ingestion system.
        """
        self.engine = create_engine(db_connection)
        self.logger = logging.getLogger(__name__)

    def fetch_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Fetch stock data using yfinance with appropriate interval.
        """
        try:
            # Get stock ticker
            ticker = yf.Ticker(symbol)
            
            # Get data for last 7 days with 5-minute intervals
            # This is within yfinance's limitations
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            df = ticker.history(
                start=start_date,
                end=end_date,
                interval='5m',  # Using 5-minute intervals instead of 1-minute
                prepost=True    # Include pre and post market data
            )
            
            if df.empty:
                self.logger.warning(f"No data retrieved for {symbol}")
                return None

            # Add symbol column
            df['symbol'] = symbol
            
            # Add timestamp column
            df['timestamp'] = df.index
            
            # Reset index
            df.reset_index(drop=True, inplace=True)
            
            # Calculate technical indicators
            df = self.calculate_technical_indicators(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators for the stock data.
        """
        try:
            # Volatility (20-period rolling standard deviation of returns)
            df['returns'] = df['Close'].pct_change()
            df['volatility'] = df['returns'].rolling(window=20).std() * np.sqrt(252)
            
            # Volume moving average
            df['volume_ma'] = df['Volume'].rolling(window=20).mean()
            
            # Price moving averages
            df['price_ma_50'] = df['Close'].rolling(window=50).mean()
            df['price_ma_200'] = df['Close'].rolling(window=200).mean()
            
            # RSI
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            # Clean up NaN values
            df = df.fillna(0)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {str(e)}")
            raise

    def store_stock_data(self, df: pd.DataFrame, table_name: str = 'stock_data'):
        """
        Store stock data in PostgreSQL database.
        """
        try:
            if df is None or df.empty:
                self.logger.warning("No data to store")
                return

            # Remove any infinite values
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.fillna(0)
            
            # Select and rename columns for database
            df_to_store = df[[
                'symbol', 'timestamp', 'Open', 'High', 'Low', 'Close', 
                'Volume', 'volatility', 'volume_ma', 'price_ma_50', 
                'price_ma_200', 'rsi'
            ]].copy()
            
            # Convert column names to lowercase
            df_to_store.columns = df_to_store.columns.str.lower()
            
            # Store data
            df_to_store.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            self.logger.info(f"Successfully stored {len(df_to_store)} records for {df['symbol'].iloc[0]}")
            
        except Exception as e:
            self.logger.error(f"Error storing data: {str(e)}")
            raise

    def get_company_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch company information.
        """
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            company_info = {
                'symbol': symbol,
                'name': info.get('longName', ''),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'market_cap': info.get('marketCap', 0),
                'pe_ratio': info.get('forwardPE', 0),
                'dividend_yield': info.get('dividendYield', 0),
                'beta': info.get('beta', 0),
                'updated_at': datetime.now()
            }
            
            return company_info
            
        except Exception as e:
            self.logger.error(f"Error fetching company info for {symbol}: {str(e)}")
            return None

    def store_company_info(self, company_info: Dict[str, Any], table_name: str = 'company_info'):
        """
        Store company information in PostgreSQL database.
        """
        try:
            if not company_info:
                self.logger.warning("No company info to store")
                return

            # Convert to DataFrame
            df = pd.DataFrame([company_info])
            
            # Store data
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False
            )
            
            self.logger.info(f"Successfully stored company info for {company_info['symbol']}")
            
        except Exception as e:
            self.logger.error(f"Error storing company info: {str(e)}")
            raise

    def process_stocks(self, symbols: list):
        """
        Process multiple stocks and store their data.
        """
        for symbol in symbols:
            try:
                # Fetch and store stock data
                df = self.fetch_stock_data(symbol)
                if df is not None:
                    self.store_stock_data(df)
                
                # Fetch and store company info
                company_info = self.get_company_info(symbol)
                if company_info:
                    self.store_company_info(company_info)
                
                # Sleep to avoid hitting API limits
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {str(e)}")
                continue