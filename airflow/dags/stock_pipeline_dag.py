from airflow import DAG
from airflow.operators.python import PythonOperator  
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.ingestion.stock_ingestion import StockDataIngestion
from src.streaming.kafka_producer import MarketDataProducer
from src.streaming.real_time_processor import RealTimeMarketAnalytics

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'stock_pipeline',
    default_args=default_args,
    description='Stock market data pipeline',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    catchup=False
)

def start_data_ingestion(**context):
    """Start stock data ingestion."""
    db_connection = "postgresql://postgres:your_password@localhost/stockdb"
    ingestion = StockDataIngestion(db_connection)
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    ingestion.process_stocks(symbols)

def start_kafka_producer(**context):
    """Start Kafka producer for real-time data."""
    producer = MarketDataProducer(
        bootstrap_servers=['localhost:9092']
    )
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    producer.produce_messages(symbols, interval=60)

def start_spark_processing(**context):
    """Start Spark streaming processor."""
    processor = RealTimeMarketAnalytics()
    processor.process_stream()

# Create tasks
ingest_task = PythonOperator(
    task_id='ingest_stock_data',
    python_callable=start_data_ingestion,
    dag=dag
)

kafka_task = PythonOperator(
    task_id='start_kafka_producer',
    python_callable=start_kafka_producer,
    dag=dag
)

spark_task = PythonOperator(
    task_id='start_spark_processing',
    python_callable=start_spark_processing,
    dag=dag
)

# Define task dependencies
ingest_task >> kafka_task >> spark_task