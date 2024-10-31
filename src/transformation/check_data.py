import pandas as pd
from sqlalchemy import create_engine, text
import logging

class DataChecker:
    def __init__(self, db_connection: str):
        self.engine = create_engine(db_connection)
        self.logger = logging.getLogger(__name__)
    
    def check_collected_data(self):
        """
        Check the data that has been collected so far.
        """
        try:
            # Check stock_data table
            stock_query = """
            SELECT 
                symbol,
                COUNT(*) as record_count,
                MIN(timestamp) as earliest_record,
                MAX(timestamp) as latest_record,
                COUNT(DISTINCT DATE(timestamp)) as number_of_days
            FROM stock_data
            GROUP BY symbol;
            """
            
            stock_data = pd.read_sql_query(text(stock_query), self.engine)
            
            # Check company_info table
            company_query = "SELECT * FROM company_info;"
            company_data = pd.read_sql_query(text(company_query), self.engine)
            
            print("\nStock Data Summary:")
            print(stock_data)
            print("\nCompany Info Summary:")
            print(company_data)
            
            return stock_data, company_data
            
        except Exception as e:
            self.logger.error(f"Error checking data: {str(e)}")
            raise

if __name__ == "__main__":
    # Use the same database connection as your main script
    DB_CONNECTION = "postgresql://postgres:1234@localhost/stockdb"
    
    checker = DataChecker(DB_CONNECTION)
    stock_summary, company_summary = checker.check_collected_data()