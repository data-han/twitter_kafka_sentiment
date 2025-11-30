import os
import json
import logging
import time
from typing import Optional
from datetime import datetime
import tweepy
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('twitter_kafka_stream.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TwitterToKafkaPipeline:
    """Pipeline to stream Twitter data and publish to Kafka."""
    
    def __init__(self):
        """Initialize the pipeline with configuration from environment variables."""
        load_dotenv()
        
        # Twitter API credentials
        self.consumer_key = os.getenv('twitter_consumer_key')
        self.consumer_secret = os.getenv('twitter_consumer_secret')
        self.access_token = os.getenv('twitter_access_token')
        self.access_token_secret = os.getenv('twitter_access_token_secret')
        
        # Kafka configuration
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
        self.topic_name = os.getenv('KAFKA_TOPIC', 'twitterdata')
        
        # Streaming configuration
        self.search_terms = os.getenv('TWITTER_SEARCH_TERMS', 'nba, #NBA, NBA, #nba, Nba').split(',')
        self.search_terms = [term.strip() for term in self.search_terms]
        
        # Validate configuration
        self._validate_config()
        
        # Initialize components
        self.producer: Optional[KafkaProducer] = None
        self.stream: Optional[tweepy.Stream] = None
        
        # Statistics
        self.tweets_sent = 0
        self.tweets_failed = 0
        self.start_time = None
    
    def _validate_config(self) -> None:
        """Validate required configuration parameters."""
        required_twitter_vars = {
            'twitter_consumer_key': self.consumer_key,
            'twitter_consumer_secret': self.consumer_secret,
            'twitter_access_token': self.access_token,
            'twitter_access_token_secret': self.access_token_secret
        }
        
        missing = [key for key, value in required_twitter_vars.items() if not value]
        if missing:
            raise ValueError(f"Missing required Twitter credentials: {', '.join(missing)}")
        
        logger.info("Configuration validated successfully")
    
    def _initialize_kafka_producer(self) -> None:
        """Initialize Kafka producer with proper configuration."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip',
                buffer_memory=33554432,  # 32MB
                batch_size=16384,
                linger_ms=10,
                request_timeout_ms=30000,
                api_version=(0, 10, 1)
            )
            logger.info(f"Kafka producer initialized. Servers: {self.kafka_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def _on_send_success(self, record_metadata):
        """Callback for successful message delivery."""
        logger.debug(
            f"Message sent to topic: {record_metadata.topic}, "
            f"partition: {record_metadata.partition}, "
            f"offset: {record_metadata.offset}"
        )
    
    def _on_send_error(self, exception):
        """Callback for failed message delivery."""
        logger.error(f"Failed to send message to Kafka: {exception}")
        self.tweets_failed += 1
    
    def _create_stream_listener(self) -> tweepy.Stream:
        """Create and configure Twitter stream listener."""
        
        class CustomStreamListener(tweepy.Stream):
            def __init__(listener_self, consumer_key, consumer_secret, 
                        access_token, access_token_secret, pipeline_instance):
                super().__init__(consumer_key, consumer_secret, 
                               access_token, access_token_secret)
                listener_self.pipeline = pipeline_instance
            
            def on_data(listener_self, raw_data):
                """Handle incoming tweet data."""
                try:
                    # Parse tweet data
                    tweet_data = json.loads(raw_data)
                    
                    # Extract relevant fields
                    processed_data = listener_self.pipeline._process_tweet(tweet_data)
                    
                    if processed_data:
                        # Send to Kafka with async callback
                        future = listener_self.pipeline.producer.send(
                            listener_self.pipeline.topic_name,
                            key=processed_data.get('id_str'),
                            value=processed_data
                        )
                        
                        # Add callbacks
                        future.add_callback(listener_self.pipeline._on_send_success)
                        future.add_errback(listener_self.pipeline._on_send_error)
                        
                        listener_self.pipeline.tweets_sent += 1
                        
                        # Log progress every 100 tweets
                        if listener_self.pipeline.tweets_sent % 100 == 0:
                            listener_self.pipeline._log_statistics()
                        
                        logger.debug(f"Tweet sent: {processed_data.get('text', '')[:50]}...")
                    
                    return True
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse tweet JSON: {e}")
                    return True
                except Exception as e:
                    logger.error(f"Error processing tweet: {e}")
                    listener_self.pipeline.tweets_failed += 1
                    return True
            
            def on_error(listener_self, status_code):
                """Handle stream errors."""
                logger.error(f"Twitter Stream error: {status_code}")
                
                # Handle rate limiting
                if status_code == 420:
                    logger.warning("Rate limit exceeded. Backing off...")
                    return False  # Stop stream
                
                # Handle other errors
                if status_code in [401, 403, 404]:
                    logger.error(f"Authentication/Authorization error: {status_code}")
                    return False
                
                return True
            
            def on_disconnect(listener_self, notice):
                """Handle disconnection."""
                logger.warning(f"Stream disconnected: {notice}")
                return False
            
            def on_connection_error(listener_self):
                """Handle connection errors."""
                logger.error("Connection error occurred")
                return True
        
        return CustomStreamListener(
            self.consumer_key,
            self.consumer_secret,
            self.access_token,
            self.access_token_secret,
            self
        )
    
    def _process_tweet(self, tweet_data: dict) -> Optional[dict]:
        """
        Process and extract relevant fields from tweet data.
        
        Args:
            tweet_data: Raw tweet data from Twitter API
            
        Returns:
            Processed tweet dictionary or None if processing fails
        """
        try:
            # Handle extended tweet
            if 'extended_tweet' in tweet_data:
                text = tweet_data['extended_tweet']['full_text']
            elif 'text' in tweet_data:
                text = tweet_data['text']
            else:
                return None
            
            # Extract relevant fields
            processed = {
                'id_str': tweet_data.get('id_str'),
                'created_at': tweet_data.get('created_at'),
                'timestamp': datetime.now().isoformat(),
                'text': text,
                'user_id': tweet_data.get('user', {}).get('id_str'),
                'user_name': tweet_data.get('user', {}).get('screen_name'),
                'user_followers': tweet_data.get('user', {}).get('followers_count'),
                'retweet_count': tweet_data.get('retweet_count', 0),
                'favorite_count': tweet_data.get('favorite_count', 0),
                'lang': tweet_data.get('lang'),
                'source': tweet_data.get('source'),
                'hashtags': [tag['text'] for tag in tweet_data.get('entities', {}).get('hashtags', [])],
                'is_retweet': 'retweeted_status' in tweet_data
            }
            
            return processed
            
        except Exception as e:
            logger.error(f"Error processing tweet data: {e}")
            return None
    
    def _log_statistics(self) -> None:
        """Log pipeline statistics."""
        elapsed_time = time.time() - self.start_time if self.start_time else 0
        tweets_per_second = self.tweets_sent / elapsed_time if elapsed_time > 0 else 0
        
        logger.info(
            f"Statistics - Sent: {self.tweets_sent}, "
            f"Failed: {self.tweets_failed}, "
            f"Rate: {tweets_per_second:.2f} tweets/sec, "
            f"Uptime: {elapsed_time:.0f}s"
        )
    
    def run(self) -> None:
        """Run the Twitter to Kafka streaming pipeline."""
        try:
            # Initialize Kafka producer
            self._initialize_kafka_producer()
            
            # Create stream listener
            self.stream = self._create_stream_listener()
            
            # Start streaming
            logger.info(f"Starting Twitter stream for terms: {self.search_terms}")
            logger.info(f"Publishing to Kafka topic: {self.topic_name}")
            
            self.start_time = time.time()
            
            # Start filtering stream
            self.stream.filter(
                track=self.search_terms,
                languages=['en'],  # Optional: filter by language
                stall_warnings=True
            )
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal...")
        except Exception as e:
            logger.error(f"Unexpected error in pipeline: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources and close connections."""
        logger.info("Cleaning up resources...")
        
        self._log_statistics()
        
        # Disconnect stream
        if self.stream:
            try:
                self.stream.disconnect()
                logger.info("Twitter stream disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting stream: {e}")
        
        # Flush and close producer
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=10)
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")


def main():
    """Main entry point for the script."""
    try:
        pipeline = TwitterToKafkaPipeline()
        pipeline.run()
    except Exception as e:
        logger.error(f"Failed to run pipeline: {e}")
        exit(1)


if __name__ == "__main__":
    main()