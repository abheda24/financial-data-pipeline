import pandas as pd
from sqlalchemy import create_engine, text
import logging
from sqlalchemy.orm import sessionmaker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataQualityAnalyzer:
    def __init__(self, db_connection: str):
        self.engine = create_engine(db_connection)
        self.Session = sessionmaker(bind=self.engine)
    
    def analyze_missing_values(self):
        """Check for missing values in the dataset."""
        query = """
        SELECT 
            symbol,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE close IS NULL) as null_close,
            COUNT(*) FILTER (WHERE volume IS NULL) as null_volume,
            COUNT(*) FILTER (WHERE open IS NULL) as null_open,
            COUNT(*) FILTER (WHERE high IS NULL) as null_high,
            COUNT(*) FILTER (WHERE low IS NULL) as null_low
        FROM stock_data
        GROUP BY symbol;
        """
        
        results = pd.read_sql_query(text(query), self.engine)
        logger.info("\nMissing Values Analysis:")
        logger.info(results)
        return results

    def analyze_price_consistency(self):
        """Analyze price consistency issues."""
        query = """
        WITH price_checks AS (
            SELECT 
                symbol,
                timestamp,
                CASE 
                    WHEN close > high THEN 1
                    WHEN close < low THEN 1
                    WHEN open > high THEN 1
                    WHEN open < low THEN 1
                    ELSE 0
                END as has_price_issue,
                CASE 
                    WHEN volume <= 0 THEN 1
                    ELSE 0
                END as has_volume_issue
            FROM stock_data
        )
        SELECT 
            symbol,
            SUM(has_price_issue) as price_inconsistencies,
            SUM(has_volume_issue) as volume_issues,
            COUNT(*) as total_records
        FROM price_checks
        GROUP BY symbol;
        """
        
        results = pd.read_sql_query(text(query), self.engine)
        logger.info("\nPrice Consistency Analysis:")
        logger.info(results)
        return results

    def fix_data_quality_issues(self):
        """Fix data quality issues with proper transaction management."""
        try:
            session = self.Session()
            
            # 1. Fix price consistency issues
            price_fix_query = """
            UPDATE stock_data
            SET 
                high = GREATEST(COALESCE(open, close), COALESCE(high, close), COALESCE(low, close), close),
                low = LEAST(COALESCE(open, close), COALESCE(high, close), COALESCE(low, close), close)
            WHERE high < low 
                OR close > high 
                OR close < low 
                OR open > high 
                OR open < low;
            """
            
            # 2. Fix volume issues
            volume_fix_query = """
            UPDATE stock_data
            SET volume = CASE 
                WHEN volume <= 0 THEN 
                    (SELECT AVG(volume) FROM stock_data s2 
                     WHERE s2.symbol = stock_data.symbol 
                     AND volume > 0)::integer
                ELSE volume
                END
            WHERE volume <= 0;
            """
            
            # Execute fixes within transaction
            with session.begin():
                session.execute(text(price_fix_query))
                session.execute(text(volume_fix_query))
            
            logger.info("Successfully fixed data quality issues")
            
        except Exception as e:
            logger.error(f"Error fixing data quality issues: {str(e)}")
            raise
        finally:
            session.close()

    def verify_fixes(self):
        """Verify that the fixes were successful."""
        query = """
        SELECT 
            symbol,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE close > high OR close < low) as remaining_price_issues,
            COUNT(*) FILTER (WHERE volume <= 0) as remaining_volume_issues
        FROM stock_data
        GROUP BY symbol;
        """
        
        results = pd.read_sql_query(text(query), self.engine)
        logger.info("\nVerification of Fixes:")
        logger.info(results)
        return results

    def analyze_time_gaps(self):
        """Analyze time gaps between records."""
        query = """
        WITH time_gaps AS (
            SELECT 
                symbol,
                timestamp,
                LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_timestamp,
                EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp)))/60 as gap_minutes
            FROM stock_data
        )
        SELECT 
            symbol,
            COUNT(*) FILTER (WHERE gap_minutes > 5) as gaps_count,
            MAX(gap_minutes) as max_gap_minutes,
            AVG(gap_minutes) FILTER (WHERE gap_minutes > 5) as avg_gap_minutes
        FROM time_gaps
        WHERE gap_minutes IS NOT NULL
        GROUP BY symbol;
        """
        
        results = pd.read_sql_query(text(query), self.engine)
        logger.info("\nTime Gaps Analysis:")
        logger.info(results)
        return results

def main():
    # Replace with your actual database connection
    db_connection = "postgresql://postgres:1234@localhost/stockdb"
    
    analyzer = DataQualityAnalyzer(db_connection)
    
    # Initial analysis
    logger.info("=== Initial Data Quality Analysis ===")
    analyzer.analyze_missing_values()
    analyzer.analyze_price_consistency()
    analyzer.analyze_time_gaps()
    
    # Fix issues
    logger.info("\n=== Fixing Data Quality Issues ===")
    analyzer.fix_data_quality_issues()
    
    # Verify fixes
    logger.info("\n=== Verification After Fixes ===")
    analyzer.verify_fixes()
    
    logger.info("\n=== Final Time Gap Analysis ===")
    analyzer.analyze_time_gaps()

if __name__ == "__main__":
    main()