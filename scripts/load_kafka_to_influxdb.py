import os
import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaToInfluxDBPipeline:
    """Pipeline to consume Twitter data from Kafka and write to InfluxDB."""
    
    def __init__(self):
        """Initialize the pipeline with configuration from environment variables."""
        load_dotenv()
        
        # Kafka configuration
        self.topic_name = os.getenv('KAFKA_TOPIC', 'twitterdata-clean')
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'twitter-group')
        
        # InfluxDB configuration
        self.influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        self.influx_token = os.getenv('influxdb_token')
        self.influx_org = os.getenv('influxdb_org')
        self.influx_bucket = os.getenv('influxdb_bucket')
        
        # Validate configuration
        self._validate_config()
        
        # Initialize clients (to be set in run method)
        self.consumer: Optional[KafkaConsumer] = None
        self.influx_client: Optional[InfluxDBClient] = None
        self.write_api: Optional[WriteApi] = None
        
        # Statistics
        self.messages_processed = 0
        self.messages_failed = 0
    
    def _validate_config(self) -> None:
        """Validate required configuration parameters."""
        required_vars = {
            'influxdb_token': self.influx_token,
            'influxdb_org': self.influx_org,
            'influxdb_bucket': self.influx_bucket
        }
        
        missing = [key for key, value in required_vars.items() if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def _initialize_kafka_consumer(self) -> None:
        """Initialize Kafka consumer with proper configuration."""
        try:
            self.consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=self.group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                max_poll_records=100,  # Batch processing
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info(f"Kafka consumer initialized for topic: {self.topic_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def _initialize_influxdb_client(self) -> None:
        """Initialize InfluxDB client and write API."""
        try:
            self.influx_client = InfluxDBClient(
                url=self.influx_url,
                token=self.influx_token,
                org=self.influx_org
            )
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            logger.info("InfluxDB client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize InfluxDB client: {e}")
            raise
    
    def _create_data_point(self, message_value: Dict[str, Any]) -> Point:
        """
        Create an InfluxDB Point from message data.
        
        Args:
            message_value: Parsed message value dictionary
            
        Returns:
            InfluxDB Point object
        """
        return (Point("nba_tweet")
                .tag("polarity", str(message_value.get('polarity', 'unknown')))
                .field("polarity_v", float(message_value.get('polarity_v', 0.0)))
                .field("subjectivity_v", float(message_value.get('subjectivity_v', 0.0)))
                .field("text", str(message_value.get('text', '')))
                .time(message_value.get('ts')))
    
    def _process_message(self, message) -> bool:
        """
        Process a single Kafka message and write to InfluxDB.
        
        Args:
            message: Kafka message object
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Extract message metadata
            message_value = message.value
            message_key = message.key
            partition = message.partition
            offset = message.offset
            
            logger.debug(
                f"Processing message - Partition: {partition}, "
                f"Offset: {offset}, Key: {message_key}"
            )
            
            # Create and write data point
            data_point = self._create_data_point(message_value)
            self.write_api.write(bucket=self.influx_bucket, record=data_point)
            
            self.messages_processed += 1
            
            if self.messages_processed % 100 == 0:
                logger.info(f"Processed {self.messages_processed} messages")
            
            return True
            
        except KeyError as e:
            logger.error(f"Missing required field in message: {e}")
            self.messages_failed += 1
            return False
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.messages_failed += 1
            return False
    
    def run(self) -> None:
        """Run the pipeline to consume messages and write to InfluxDB."""
        try:
            # Initialize clients
            self._initialize_kafka_consumer()
            self._initialize_influxdb_client()
            
            logger.info("Starting message consumption...")
            
            # Consume messages
            for message in self.consumer:
                self._process_message(message)
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal...")
        except Exception as e:
            logger.error(f"Unexpected error in pipeline: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources and close connections."""
        logger.info(
            f"Pipeline statistics - "
            f"Processed: {self.messages_processed}, "
            f"Failed: {self.messages_failed}"
        )
        
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        
        if self.write_api:
            try:
                self.write_api.close()
                logger.info("InfluxDB write API closed")
            except Exception as e:
                logger.error(f"Error closing InfluxDB write API: {e}")
        
        if self.influx_client:
            try:
                self.influx_client.close()
                logger.info("InfluxDB client closed")
            except Exception as e:
                logger.error(f"Error closing InfluxDB client: {e}")


def main():
    """Main entry point for the script."""
    try:
        pipeline = KafkaToInfluxDBPipeline()
        pipeline.run()
    except Exception as e:
        logger.error(f"Failed to run pipeline: {e}")
        exit(1)


if __name__ == "__main__":
    main()