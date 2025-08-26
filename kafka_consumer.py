#!/usr/bin/env python3
"""
Simple Kafka Consumer for SQL Server Error Log Simulator
Monitors messages from the simulator
"""

import json
import sys
from datetime import datetime

try:
    from confluent_kafka import Consumer, KafkaError
    USE_CONFLUENT = True
except ImportError:
    try:
        from kafka import KafkaConsumer
        USE_CONFLUENT = False
    except ImportError:
        print("❌ No Kafka library found. Install with: pip install confluent-kafka")
        sys.exit(1)

def consume_messages():
    """Consume messages from Kafka topic"""
    
    bootstrap_servers = "localhost:29092"
    topic = "sql-server-raw-errorlogs"
    
    print(f"Starting Kafka consumer for topic: {topic}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print("Press Ctrl+C to stop")
    print("-" * 50)
    
    try:
        if USE_CONFLUENT:
            consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': 'sql_log_monitor',
                'auto.offset.reset': 'latest'
            })
            consumer.subscribe([topic])
        else:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='sql_log_monitor'
            )
        
        message_count = 0
        
        while True:
            try:
                if USE_CONFLUENT:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        print(f"Consumer error: {msg.error()}")
                        continue
                    
                    message_data = json.loads(msg.value().decode('utf-8'))
                else:
                    msg = next(consumer)
                    message_data = msg.value
                
                message_count += 1
                timestamp = datetime.now().strftime("%H:%M:%S")
                
                print(f"[{timestamp}] Message #{message_count}")
                print(f"  Server: {message_data.get('server_name', 'Unknown')}")
                print(f"  Error Type: {message_data.get('error_type', 'Unknown')}")
                print(f"  Log Entry: {message_data.get('log_entry', 'No content')[:100]}...")
                print("-" * 30)
                
            except KeyboardInterrupt:
                print("\nStopping consumer...")
                break
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
        
        consumer.close()
        print(f"Consumer stopped. Total messages received: {message_count}")
        
    except Exception as e:
        print(f"Failed to start consumer: {e}")
        print("\nPossible solutions:")
        print("1. Make sure Kafka is running on localhost:29092")
        print("2. Check if topic exists: kafka-topics.sh --list --bootstrap-server localhost:29092")
        print("3. Create topic: kafka-topics.sh --create --topic sql-server-raw-errorlogs --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1")

if __name__ == "__main__":
    consume_messages()
