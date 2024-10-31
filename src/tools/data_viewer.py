import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataViewer:
    def __init__(self, db_connection: str):
        self.engine = create_engine(db_connection)
    
    def view_latest_stock_data(self):
        """View the most recent stock data."""
        query = """
        SELECT 
            symbol,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            rsi
        FROM stock_data
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
        ORDER BY timestamp DESC
        LIMIT 10;
        """
        
        df = pd.read_sql(query, self.engine)
        print("\nLatest Stock Data:")
        print(df)
        return df
    
    def view_company_info(self):
        """View company information."""
        query = "SELECT * FROM company_info;"
        df = pd.read_sql(query, self.engine)
        print("\nCompany Information:")
        print(df)
        return df
    
    def view_summary_stats(self):
        """View summary statistics."""
        query = """
        SELECT 
            symbol,
            COUNT(*) as records,
            MIN(close) as min_price,
            MAX(close) as max_price,
            AVG(close) as avg_price,
            AVG(volume) as avg_volume
        FROM stock_data
        GROUP BY symbol;
        """
        
        df = pd.read_sql(query, self.engine)
        print("\nSummary Statistics:")
        print(df)
        return df

if __name__ == "__main__":
    # Replace with your database connection string
    db_connection = "postgresql://postgres:1234@localhost/stockdb"
    
    viewer = DataViewer(db_connection)
    
    print("\n=== Stock Data Pipeline Viewer ===")
    viewer.view_latest_stock_data()
    viewer.view_company_info()
    viewer.view_summary_stats()