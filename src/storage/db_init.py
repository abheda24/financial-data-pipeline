import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

class DatabaseInitializer:
    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.logger = logging.getLogger(__name__)

    def create_database(self):
        """
        Create the database if it doesn't exist.
        """
        try:
            # Connect to default PostgreSQL database
            conn = psycopg2.connect(
                host=self.db_params['host'],
                user=self.db_params['user'],
                password=self.db_params['password'],
                database='postgres'  # Connect to default postgres database
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{self.db_params['database']}'")
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(f"CREATE DATABASE {self.db_params['database']}")
                self.logger.info(f"Created database {self.db_params['database']}")
            else:
                self.logger.info(f"Database {self.db_params['database']} already exists")
                
            cursor.close()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error creating database: {str(e)}")
            raise

    def create_tables(self):
        """
        Create necessary tables in the database.
        """
        try:
            # Connect to the specific database
            conn = psycopg2.connect(
                host=self.db_params['host'],
                user=self.db_params['user'],
                password=self.db_params['password'],
                database=self.db_params['database']
            )
            cursor = conn.cursor()

            # Create stock_data table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL,
                volume BIGINT NOT NULL,
                volatility FLOAT,
                volume_ma FLOAT,
                price_ma_50 FLOAT,
                price_ma_200 FLOAT,
                rsi FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # Create company_info table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_info (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                name VARCHAR(255),
                sector VARCHAR(100),
                industry VARCHAR(100),
                market_cap BIGINT,
                pe_ratio FLOAT,
                dividend_yield FLOAT,
                beta FLOAT,
                updated_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_symbol_timestamp ON stock_data(symbol, timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_company_symbol ON company_info(symbol);")

            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info("Successfully created tables and indexes")
            
        except Exception as e:
            self.logger.error(f"Error creating tables: {str(e)}")
            raise