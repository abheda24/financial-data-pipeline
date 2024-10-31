import sys
import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineTester:
    def __init__(self, db_connection: str):
        """
        Initialize pipeline tester with database connection.
        """
        self.engine = create_engine(db_connection)
        
    def test_database_connection(self):
        """Test database connectivity and show existing tables."""
        try:
            # Get list of all tables
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            """
            
            tables = pd.read_sql(query, self.engine)
            logger.info("\n=== Database Tables ===")
            for table in tables['table_name']:
                # Get record count for each table
                count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", self.engine)
                logger.info(f"Table: {table} - Records: {count['count'].iloc[0]}")
                
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {str(e)}")
            return False

    def test_data_ingestion(self):
        """Test if data is being ingested properly."""
        try:
            query = """
            SELECT 
                symbol,
                COUNT(*) as record_count,
                MIN(timestamp) as earliest_record,
                MAX(timestamp) as latest_record
            FROM stock_data
            GROUP BY symbol
            ORDER BY symbol;
            """
            
            results = pd.read_sql(query, self.engine)
            
            logger.info("\n=== Data Ingestion Test ===")
            logger.info("\nStock Data Summary:")
            logger.info(results)
            
            if len(results) > 0:
                return True
            return False
            
        except Exception as e:
            logger.error(f"Data ingestion test failed: {str(e)}")
            return False

    def test_data_quality(self):
        """Test data quality metrics."""
        try:
            query = """
            SELECT 
                symbol,
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE close IS NULL) as null_prices,
                COUNT(*) FILTER (WHERE volume = 0) as zero_volume,
                COUNT(DISTINCT DATE(timestamp)) as unique_days,
                ROUND(AVG(volume)) as avg_volume
            FROM stock_data
            GROUP BY symbol;
            """
            
            quality_metrics = pd.read_sql(query, self.engine)
            
            logger.info("\n=== Data Quality Metrics ===")
            logger.info(quality_metrics)
            
            # Check for data quality issues
            has_issues = (
                quality_metrics['null_prices'].sum() > 0 or
                quality_metrics['zero_volume'].sum() / quality_metrics['total_records'].sum() > 0.1
            )
            
            if has_issues:
                logger.warning("Data quality issues detected!")
            else:
                logger.info("No major data quality issues found.")
                
            return not has_issues
            
        except Exception as e:
            logger.error(f"Data quality test failed: {str(e)}")
            return False

    def test_latest_data(self):
        """Test if we're getting recent data."""
        try:
            query = """
            SELECT 
                symbol,
                MAX(timestamp) as last_update,
                EXTRACT(MINUTE FROM NOW() - MAX(timestamp)) as minutes_ago
            FROM stock_data
            GROUP BY symbol;
            """
            
            latest_data = pd.read_sql(query, self.engine)
            
            logger.info("\n=== Latest Data Check ===")
            logger.info(latest_data)
            
            # Check if data is recent (within last hour)
            is_recent = latest_data['minutes_ago'].max() < 60
            
            if not is_recent:
                logger.warning("Data may not be up to date!")
            else:
                logger.info("Data is current.")
                
            return is_recent
            
        except Exception as e:
            logger.error(f"Latest data test failed: {str(e)}")
            return False

    def test_technical_indicators(self):
        """Test if technical indicators are being calculated correctly."""
        try:
            query = """
            SELECT 
                symbol,
                timestamp,
                close,
                rsi,
                price_ma_50,
                price_ma_200,
                volume_ma
            FROM stock_data
            WHERE timestamp = (
                SELECT MAX(timestamp) 
                FROM stock_data
            )
            ORDER BY symbol;
            """
            
            indicators = pd.read_sql(query, self.engine)
            
            logger.info("\n=== Technical Indicators Check ===")
            logger.info(indicators)
            
            # Verify indicators are being calculated
            has_indicators = (
                not indicators['rsi'].isnull().all() and
                not indicators['price_ma_50'].isnull().all()
            )
            
            if not has_indicators:
                logger.warning("Some technical indicators are missing!")
            else:
                logger.info("Technical indicators are being calculated properly.")
                
            return has_indicators
            
        except Exception as e:
            logger.error(f"Technical indicators test failed: {str(e)}")
            return False

def run_all_tests():
    """Run all pipeline tests."""
    try:
        # Database connection string
        db_connection = "postgresql://postgres:1234@localhost/stockdb"
        
        # Initialize tester
        tester = PipelineTester(db_connection)
        
        # Run all tests
        tests = {
            "Database Connection": tester.test_database_connection,
            "Data Ingestion": tester.test_data_ingestion,
            "Data Quality": tester.test_data_quality,
            "Latest Data": tester.test_latest_data,
            "Technical Indicators": tester.test_technical_indicators
        }
        
        results = {}
        logger.info("\n=== Starting Pipeline Tests ===\n")
        
        for test_name, test_func in tests.items():
            try:
                result = test_func()
                results[test_name] = result
                status = "✅ PASSED" if result else "❌ FAILED"
                logger.info(f"\n{test_name} Test: {status}")
            except Exception as e:
                results[test_name] = False
                logger.error(f"\n{test_name} Test: ❌ FAILED - {str(e)}")
        
        # Summary
        logger.info("\n=== Test Summary ===")
        for test_name, result in results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            logger.info(f"{test_name}: {status}")
            
        return all(results.values())
        
    except Exception as e:
        logger.error(f"Error running tests: {str(e)}")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    if success:
        logger.info("\n🎉 All tests passed successfully!")
    else:
        logger.error("\n⚠️ Some tests failed! Please check the logs above.")