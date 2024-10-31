from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import logging
import yfinance as yf
import time
from datetime import datetime
import pandas as pd
from typing import Dict, List
import traceback
from prometheus_client import Counter, Gauge, start_http_server

class MarketDataProducer:
    def __init__(self, bootstrap_servers: List[str], topic: str = 'market_data'):
        self.logger = logging.getLogger(__name__)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        
        # Initialize metrics
        self.messages_produced = Counter(
            'market_data_messages_produced_total',
            'Total number of messages produced'
        )
        self.producer_errors = Counter(
            'market_data_producer_errors_total',
            'Total number of producer errors'
        )
        self.message_size = Gauge(
            'market_data_message_size_bytes',
            'Size of produced messages in bytes'
        )
        
        # Initialize producer with robust configuration
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=5,   # Retry on failure
            retry_backoff_ms=500,
            batch_size=16384,
            linger_ms=100,  # Wait to batch messages
            compression_type='gzip'  # Compress messages
        )
        
        # Ensure topic exists
        self._ensure_topic_exists()
        
    def _ensure_topic_exists(self):
        """Ensure Kafka topic exists, create if it doesn't."""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
            
            existing_topics = admin_client.list_topics()
            if self.topic not in existing_topics:
                topic = NewTopic(
                    name=self.topic,
                    num_partitions=3,  # Multiple partitions for scalability
                    replication_factor=1  # Adjust based on cluster size
                )
                admin_client.create_topics([topic])
                self.logger.info(f"Created topic: {self.topic}")
                
            admin_client.close()
            
        except Exception as e:
            self.logger.error(f"Error managing Kafka topic: {str(e)}")
            raise

    def get_market_data(self, symbol: str) -> Dict:
        """Get real-time market data with enhanced error handling."""
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # Get last price and volume
            hist = stock.history(period='1d', interval='1m')
            if hist.empty:
                raise ValueError(f"No data available for {symbol}")
            
            last_price = hist['Close'].iloc[-1]
            last_volume = hist['Volume'].iloc[-1]
            
            data = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'price': last_price,
                'volume': last_volume,
                'high': hist['High'].iloc[-1],
                'low': hist['Low'].iloc[-1],
                'open': hist['Open'].iloc[-1],
                'additional_info': {
                    'market_cap': info.get('marketCap'),
                    'pe_ratio': info.get('forwardPE'),
                    'fifty_day_avg': info.get('fiftyDayAverage')
                }
            }
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {str(e)}")
            self.producer_errors.inc()
            return None

    def produce_messages(self, symbols: List[str], interval: int = 1):
        """Continuously produce market data messages."""
        self.logger.info(f"Starting market data production for symbols: {symbols}")
        
        while True:
            try:
                for symbol in symbols:
                    data = self.get_market_data(symbol)
                    if data:
                        # Add metadata for tracking
                        data['metadata'] = {
                            'producer_timestamp': datetime.now().isoformat(),
                            'source': 'yfinance',
                            'version': '1.0'
                        }
                        
                        # Send message with callback
                        message_size = len(json.dumps(data).encode('utf-8'))
                        self.message_size.set(message_size)
                        
                        self.producer.send(
                            self.topic, 
                            data
                        ).add_callback(
                            self.on_send_success
                        ).add_errback(
                            self.on_send_error
                        )
                        
                        self.messages_produced.inc()
                        
                # Flush messages periodically
                self.producer.flush()
                
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"Error in message production: {str(e)}")
                self.logger.error(traceback.format_exc())
                self.producer_errors.inc()
                time.sleep(5)  # Wait before retrying

    def on_send_success(self, record_metadata):
        """Callback for successful message production."""
        self.logger.debug(
            f"Message delivered to topic {record_metadata.topic} "
            f"partition {record_metadata.partition} "
            f"offset {record_metadata.offset}"
        )

    def on_send_error(self, excp):
        """Callback for message production errors."""
        self.logger.error(f"Error producing message: {str(excp)}")
        self.producer_errors.inc()

    def close(self):
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Start Prometheus metrics server
    start_http_server(8000)
    
    # Initialize producer
    producer = MarketDataProducer(
        bootstrap_servers=['localhost:9092'],
        topic='market_data'
    )
    
    try:
        # Start producing messages
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
        producer.produce_messages(symbols)
    except KeyboardInterrupt:
        producer.close()
        logging.info("Market data producer stopped")

if __name__ == "__main__":
    main()