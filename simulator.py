#!/usr/bin/env python3
"""
MS SQL Server Error Log Simulator
Generates realistic SQL Server ERRORLOG files for testing purposes
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
import logging

from utils import (
    load_templates, load_sample_data, generate_timestamp,
    select_weighted_error_type, generate_error_entry,
    setup_logging, create_server_directories
)
# Conditional import of Kafka handler
KafkaLogPublisher = None

class SQLServerLogSimulator:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.templates = load_templates()
        self.sample_data = load_sample_data()
        self.running = False
        self.stop_event = threading.Event()
        self.threads = []
        
        setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Conditionally initialize Kafka publisher
        self.kafka_publisher = self._initialize_kafka_publisher()
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _initialize_kafka_publisher(self):
        """Conditionally initialize Kafka publisher only if enabled"""
        kafka_config = self.config.get('kafka', {})
        if not kafka_config.get('enabled', False):
            self.logger.info("Kafka publishing is disabled - continuing with file-only logging")
            return None
        
        try:
            from kafka_handler import KafkaLogPublisher
            return KafkaLogPublisher(self.config)
        except Exception as e:
            self.logger.warning(f"Failed to initialize Kafka publisher: {e}")
            self.logger.info("Continuing with file-only logging")
            return None
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.stop_simulation()
        print("Shutdown complete.")
        sys.exit(0)

    def start_simulation(self, runtime_minutes=None):
        """Start the simulation for all servers"""
        self.running = True
        self.stop_event.clear()
        create_server_directories(self.config['simulation']['server_count'])
        
        self.logger.info(f"Starting simulation for {self.config['simulation']['server_count']} servers")
        
        # Create a thread for each server
        for server_num in range(1, self.config['simulation']['server_count'] + 1):
            thread = threading.Thread(
                target=self._simulate_server,
                args=(server_num,),
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
        
        # Handle max runtime - use command line argument if provided, otherwise use config
        max_runtime = runtime_minutes if runtime_minutes is not None else self.config['simulation'].get('max_runtime_minutes', 0)
        if max_runtime > 0:
            self.logger.info(f"Simulation will run for {max_runtime} minutes")
            def runtime_complete():
                self.logger.info(f"Runtime of {max_runtime} minutes completed, stopping simulation...")
                self.stop_simulation()
            threading.Timer(max_runtime * 60, runtime_complete).start()
        else:
            self.logger.info("Simulation will run indefinitely until manually stopped")
    
    def _simulate_server(self, server_num):
        """Simulate log generation for a single server"""
        server_dir = Path(f"Server{server_num}")
        log_file = server_dir / "ERRORLOG"
        
        self.logger.info(f"Starting simulation for Server{server_num}")

        bursts_config = self.config.get('bursts', {})
        burst_enabled = bursts_config.get('enabled', False)
        burst_chance = bursts_config.get('chance_per_entry', 0.0)
        burst_duration_range = tuple(bursts_config.get('duration_seconds_range', [10, 30]))
        burst_interval_multiplier = bursts_config.get('interval_multiplier', 0.25)
        burst_weight_override = bursts_config.get('error_type_weights_override', {})

        in_burst = False
        burst_ends_at = None

        while self.running and not self.stop_event.is_set():
            try:
                now = datetime.now()

                if in_burst and now >= burst_ends_at:
                    in_burst = False
                    burst_ends_at = None

                if (not in_burst) and burst_enabled and random.random() < burst_chance:
                    duration = random.uniform(burst_duration_range[0], burst_duration_range[1])
                    burst_ends_at = now + timedelta(seconds=duration)
                    in_burst = True

                # Generate log entry
                error_types_config = self._compose_error_weights(burst_weight_override) if in_burst else self.config['error_types']
                error_type = select_weighted_error_type(error_types_config)
                log_entry = generate_error_entry(
                    error_type, 
                    self.templates, 
                    self.sample_data, 
                    self.config,
                    server_num
                )
                
                # Write to log file
                self._write_log_entry(log_file, log_entry)
                
                # Publish to Kafka if enabled
                self._publish_to_kafka(log_entry, server_num, error_type)
                
                # Wait for next entry with interruptible sleep
                base_interval, variation = self._server_interval(server_num)
                sleep_time = base_interval + random.uniform(-variation, variation)
                if in_burst:
                    sleep_time *= max(0.01, burst_interval_multiplier)
                sleep_time = max(0.1, sleep_time)  # Minimum 0.1 seconds
                
                # Use interruptible sleep
                if self.stop_event.wait(timeout=sleep_time):
                    break
                
            except Exception as e:
                self.logger.error(f"Error in server {server_num} simulation: {e}")
                if self.stop_event.wait(timeout=1):
                    break
    
    def _write_log_entry(self, log_file, entry):
        """Write log entry to file with proper encoding"""
        encoding = self.config['output']['encoding']
        
        # Handle log rotation if enabled
        if self.config['output']['log_rotation']['enabled']:
            self._handle_log_rotation(log_file)
        
        # Check if entry contains multiple lines (paired logs)
        if '\n' in entry:
            # Split into individual lines and write each one
            lines = entry.split('\n')
            with open(log_file, 'a', encoding=encoding, newline='\r\n') as f:
                for line in lines:
                    if line.strip():  # Only write non-empty lines
                        f.write(line + '\n')
        else:
            # Single line entry
            with open(log_file, 'a', encoding=encoding, newline='\r\n') as f:
                f.write(entry + '\n')
    
    def _handle_log_rotation(self, log_file):
        """Handle log file rotation if file size exceeds limit"""
        max_size = self.config['output']['log_rotation']['max_size_mb'] * 1024 * 1024
        max_files = self.config['output']['log_rotation']['max_files']
        
        if log_file.exists() and log_file.stat().st_size > max_size:
            # Rotate files
            for i in range(max_files - 1, 0, -1):
                old_file = log_file.parent / f"ERRORLOG.{i}"
                new_file = log_file.parent / f"ERRORLOG.{i + 1}"
                if old_file.exists():
                    if new_file.exists():
                        new_file.unlink()
                    old_file.rename(new_file)
            
            # Move current log to .1
            backup_file = log_file.parent / "ERRORLOG.1"
            if backup_file.exists():
                backup_file.unlink()
            log_file.rename(backup_file)
    
    def _publish_to_kafka(self, log_entry, server_num, error_type):
        """Publish log entry to Kafka topic"""
        # Only attempt to publish if Kafka publisher is available and enabled
        if self.kafka_publisher is None:
            return
            
        try:
            timestamp = datetime.now()
            metadata = {
                'source': 'sql_error_log_simulator',
                'version': '1.0'
            }
            self.kafka_publisher.publish_log_entry(log_entry, server_num, error_type, timestamp, metadata)
        except Exception as e:
            self.logger.error(f"Failed to publish to Kafka: {e}")

    def stop_simulation(self):
        """Stop the simulation"""
        self.logger.info("Stopping simulation...")
        self.running = False
        self.stop_event.set()
        
        # Wait for threads with longer timeout to ensure proper cleanup
        for thread in self.threads:
            thread.join(timeout=5)
        
        # Close Kafka producer with timeout
        if hasattr(self, 'kafka_publisher') and self.kafka_publisher is not None:
            try:
                self.kafka_publisher.close()
            except Exception as e:
                self.logger.warning(f"Error closing Kafka publisher: {e}")
        
        self.logger.info("Simulation stopped")

    def _server_interval(self, server_num):
        sim = self.config.get('simulation', {})
        per_server = sim.get('per_server', {})
        key = str(server_num)
        base = sim.get('log_interval_seconds', 5)
        var = sim.get('log_interval_variation', 0)
        if key in per_server:
            o = per_server[key]
            base = o.get('log_interval_seconds', base)
            var = o.get('log_interval_variation', var)
        return base, var

    def _compose_error_weights(self, overrides):
        if not overrides:
            return self.config['error_types']
        merged = {}
        for name, cfg in self.config['error_types'].items():
            if name in overrides:
                merged[name] = {
                    'weight': overrides[name],
                    'enabled': cfg.get('enabled', True)
                }
            else:
                merged[name] = cfg
        for name, weight in overrides.items():
            if name not in merged:
                merged[name] = { 'weight': weight, 'enabled': True }
        return merged

def main():
    parser = argparse.ArgumentParser(description='MS SQL Server Error Log Simulator')
    parser.add_argument('--runtime', type=int, help='Runtime in minutes (overrides config setting)')
    parser.add_argument('--config', type=str, default='config.json', help='Configuration file path')
    args = parser.parse_args()
    
    simulator = SQLServerLogSimulator(args.config)
    
    simulator.start_simulation(runtime_minutes=args.runtime)
    print("SQL Server Log Simulator started. Press Ctrl+C to stop...")
    
    try:
        # Wait for simulation to complete or be interrupted
        while simulator.running and not simulator.stop_event.is_set():
            time.sleep(0.5)
        
        # Ensure proper cleanup when runtime completes
        if not simulator.running:
            simulator.stop_simulation()
        
        print("Simulation completed.")
            
    except KeyboardInterrupt:
        pass  # Signal handler will take care of shutdown

if __name__ == "__main__":
    main()