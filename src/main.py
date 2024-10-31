import logging
from datetime import datetime
import time
from src.ingestion.stock_ingestion import StockDataIngestion
from src.streaming.kafka_producer import MarketDataProducer
from src.streaming.spark_consumer import StockDataStreamProcessor
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the pipeline manually (without Airflow).
    This is useful for development and testing.
    """
    try:
        # Database connection
        db_connection = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DB')}"
        
        # Initialize components
        ingestion = StockDataIngestion(db_connection)
        producer = MarketDataProducer(
            bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS')]
        )
        processor = StockDataStreamProcessor()
        
        # Process stocks
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
        
        while True:
            try:
                # Run ingestion
                ingestion.process_stocks(symbols)
                
                # Run streaming
                producer.produce_messages(symbols, interval=60)
                
                # Sleep before next cycle
                time.sleep(300)  # 5 minutes
                
            except Exception as e:
                logger.error(f"Error in pipeline cycle: {str(e)}")
                time.sleep(60)
                
    except KeyboardInterrupt:
        logger.info("Stopping pipeline")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()