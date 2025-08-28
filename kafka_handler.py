#!/usr/bin/env python3
"""
Kafka Handler for SQL Server Error Log Simulator
Handles publishing log entries to Kafka topics
"""

import json
import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime

# Only import Kafka libraries if we're going to use them
KAFKA_AVAILABLE = False
USE_CONFLUENT = False
Producer = None
KafkaProducer = None
KafkaError = None

def _import_kafka_libraries():
    """Import Kafka libraries only when needed"""
    global KAFKA_AVAILABLE, USE_CONFLUENT, Producer, KafkaProducer, KafkaError
    
    try:
        from confluent_kafka import Producer, KafkaError
        KAFKA_AVAILABLE = True
        USE_CONFLUENT = True
        KafkaProducer = None
        return True
    except ImportError:
        try:
            from kafka import KafkaProducer
            from kafka.errors import KafkaError
            KAFKA_AVAILABLE = True
            USE_CONFLUENT = False
            Producer = None
            return True
        except ImportError:
            KAFKA_AVAILABLE = False
            KafkaProducer = None
            KafkaError = None
            USE_CONFLUENT = False
            Producer = None
            return False


class KafkaLogPublisher:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.producer = None
        self.logger = logging.getLogger(__name__)
        kafka_config = config.get('kafka', {})
        self.enabled = kafka_config.get('enabled', False)
        
        if not self.enabled:
            self.logger.info("Kafka publishing is disabled - continuing with file-only logging")
            return
            
        # Only initialize if explicitly enabled
        if self.enabled:
            # Import Kafka libraries only when needed
            if not _import_kafka_libraries():
                self.logger.error("Kafka library not available. Install with: pip install kafka-python")
                self.logger.info("Continuing with file-only logging")
                self.enabled = False
                return
            
            self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Kafka producer with configuration"""
        try:
            kafka_config = self.config.get('kafka', {})
            bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
            topic = kafka_config.get('topic', 'sql-server-raw-errorlogs')
            
            if USE_CONFLUENT and Producer is not None:
                # Use confluent-kafka
                producer_config = {
                    'bootstrap.servers': bootstrap_servers,
                    'client.id': 'sql_error_log_simulator',
                    'socket.timeout.ms': 5000,  # 5 second timeout
                    'request.timeout.ms': 5000,  # 5 second timeout
                    'delivery.timeout.ms': 10000,  # 10 second timeout
                    'max.block.ms': 5000,  # 5 second timeout
                    'reconnect.backoff.ms': 1000,  # 1 second backoff
                    'reconnect.backoff.max.ms': 5000,  # Max 5 second backoff
                    'retry.backoff.ms': 1000,  # 1 second retry backoff
                    'max.in.flight.requests.per.connection': 1,  # Prevent reordering
                    'enable.idempotence': False,  # Disable idempotence for simplicity
                    'log.connection.close': False,  # Reduce log noise
                    'log.queue': False,  # Reduce log noise
                    'log.thread.name': False,  # Reduce log noise
                    'log.client.id': False,  # Reduce log noise
                    'log.connection.close': False,  # Reduce log noise
                    'log.queue': False,  # Reduce log noise
                    'log.thread.name': False,  # Reduce log noise
                    'log.client.id': False  # Reduce log noise
                }
                # Convert kafka-python options to confluent-kafka format
                kafka_options = kafka_config.get('producer_options', {})
                if 'batch_size' in kafka_options:
                    producer_config['batch.size'] = kafka_options['batch_size']
                if 'linger_ms' in kafka_options:
                    producer_config['linger.ms'] = kafka_options['linger_ms']
                if 'buffer_memory' in kafka_options:
                    producer_config['buffer.memory'] = kafka_options['buffer_memory']
                if 'acks' in kafka_options:
                    producer_config['acks'] = kafka_options['acks']
                if 'retries' in kafka_options:
                    producer_config['retries'] = kafka_options['retries']
                
                self.producer = Producer(producer_config)
                self.use_confluent = True
                
                # Test connection with a timeout
                self._test_kafka_connection()
                
            elif KafkaProducer is not None:
                # Use kafka-python
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    request_timeout_ms=5000,
                    retries=1,
                    **kafka_config.get('producer_options', {})
                )
                self.use_confluent = False
                
                # Test connection
                self._test_kafka_connection()
            else:
                raise Exception("No Kafka library available")
            
            self.topic = topic
            self.logger.info(f"Kafka producer initialized for topic: {topic}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            self.enabled = False
    
    def _test_kafka_connection(self):
        """Test Kafka connection and disable if not reachable"""
        try:
            if hasattr(self, 'use_confluent') and self.use_confluent:
                # For confluent-kafka, try to get metadata with timeout
                self.producer.poll(0)  # Trigger any pending operations
                # Try to get metadata to test connection
                self.producer.list_topics(timeout=3)
            else:
                # For kafka-python, try to get metadata
                self.producer.metrics()
            
            self.logger.info("Kafka connection test successful")
            
        except Exception as e:
            self.logger.warning(f"Kafka is not reachable: {e}")
            self.logger.info("Disabling Kafka publishing - continuing with file-only logging")
            self.enabled = False
            if self.producer:
                try:
                    self.producer.close()
                except:
                    pass
                self.producer = None
    
    def publish_log_entry(self, log_entry: str, server_num: int, error_type: str, 
                         timestamp: datetime, metadata: Optional[Dict[str, Any]] = None):
        """Publish log entry to Kafka topic"""
        if not self.enabled or not self.producer:
            return
            
        try:
            message = {
                'log_entry': log_entry,
                'server_num': server_num,
                'server_name': f"Server{server_num}",
                'error_type': error_type,
                'timestamp': timestamp.isoformat(),
                'metadata': metadata or {}
            }
            
            key = f"Server{server_num}"
            message_json = json.dumps(message, default=str)
            
            if hasattr(self, 'use_confluent') and self.use_confluent:
                # Use confluent-kafka
                self.producer.produce(
                    topic=self.topic,
                    key=key.encode('utf-8'),
                    value=message_json.encode('utf-8'),
                    callback=self._on_send_success
                )
                self.producer.poll(0)  # Trigger delivery
            else:
                # Use kafka-python
                future = self.producer.send(self.topic, key=key, value=message)
                future.add_callback(self._on_send_success_kafka_python)
                future.add_errback(self._on_send_error)
            
            self.logger.debug(f"Published log entry to Kafka: {error_type} for Server{server_num}")
            
        except Exception as e:
            # Check if this is a connection error and disable Kafka if so
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['connection', 'timeout', 'unreachable', 'refused']):
                if not hasattr(self, '_connection_error_logged'):
                    self.logger.warning(f"Kafka connection error detected: {e}")
                    self.logger.info("Disabling Kafka publishing due to connection issues - continuing with file-only logging")
                    self._connection_error_logged = True
                    self.enabled = False
                    if self.producer:
                        try:
                            self.producer.close()
                        except:
                            pass
                        self.producer = None
            else:
                # Don't log every single error to avoid spam
                if not hasattr(self, '_last_error_log') or time.time() - self._last_error_log > 10:
                    self.logger.error(f"Failed to publish log entry to Kafka: {e}")
                    self._last_error_log = time.time()
    
    def _on_send_success(self, err, msg):
        """Callback for successful message send"""
        if err is not None:
            error_str = str(err).lower()
            if any(keyword in error_str for keyword in ['connection', 'timeout', 'unreachable', 'refused']):
                if not hasattr(self, '_connection_error_logged'):
                    self.logger.warning(f"Kafka connection error in callback: {err}")
                    self.logger.info("Disabling Kafka publishing due to connection issues - continuing with file-only logging")
                    self._connection_error_logged = True
                    self.enabled = False
                    if self.producer:
                        try:
                            self.producer.close()
                        except:
                            pass
                        self.producer = None
            else:
                self.logger.error(f"Failed to send message to Kafka: {err}")
        else:
            # confluent-kafka format
            self.logger.debug(f"Message sent to Kafka successfully: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
    
    def _on_send_success_kafka_python(self, record_metadata):
        """Callback for successful message send (kafka-python format)"""
        self.logger.debug(f"Message sent to Kafka: topic={record_metadata.topic}, "
                         f"partition={record_metadata.partition}, offset={record_metadata.offset}")
    
    def _on_send_error(self, excp):
        """Callback for failed message send"""
        error_str = str(excp).lower()
        if any(keyword in error_str for keyword in ['connection', 'timeout', 'unreachable', 'refused']):
            if not hasattr(self, '_connection_error_logged'):
                self.logger.warning(f"Kafka connection error in error callback: {excp}")
                self.logger.info("Disabling Kafka publishing due to connection issues - continuing with file-only logging")
                self._connection_error_logged = True
                self.enabled = False
                if self.producer:
                    try:
                        self.producer.close()
                    except:
                        pass
                    self.producer = None
        else:
            self.logger.error(f"Failed to send message to Kafka: {excp}")
    
    def flush(self):
        """Flush pending messages"""
        if self.producer:
            if hasattr(self, 'use_confluent') and self.use_confluent:
                self.producer.flush()
            else:
                self.producer.flush()
    
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            try:
                self.flush()
                if hasattr(self.producer, 'close'):
                    self.producer.close(timeout=5)  # 5 second timeout
                self.logger.info("Kafka producer closed")
            except Exception as e:
                self.logger.warning(f"Error closing Kafka producer: {e}")
            finally:
                self.producer = None
                self.enabled = False
