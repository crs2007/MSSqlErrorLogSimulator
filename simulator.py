#!/usr/bin/env python3
"""
MS SQL Server Error Log Simulator
Generates realistic SQL Server ERRORLOG files for testing purposes
"""

import json
import os
import random
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

class SQLServerLogSimulator:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.templates = load_templates()
        self.sample_data = load_sample_data()
        self.running = False
        self.threads = []
        
        setup_logging()
        self.logger = logging.getLogger(__name__)
        
    def start_simulation(self):
        """Start the simulation for all servers"""
        self.running = True
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
        
        # Handle max runtime
        max_runtime = self.config['simulation'].get('max_runtime_minutes', 0)
        if max_runtime > 0:
            threading.Timer(max_runtime * 60, self.stop_simulation).start()
    
    def _simulate_server(self, server_num):
        """Simulate log generation for a single server"""
        server_dir = Path(f"Server{server_num}")
        log_file = server_dir / "ERRORLOG"
        
        self.logger.info(f"Starting simulation for Server{server_num}")
        
        while self.running:
            try:
                # Generate log entry
                error_type = select_weighted_error_type(self.config['error_types'])
                log_entry = generate_error_entry(
                    error_type, 
                    self.templates, 
                    self.sample_data, 
                    self.config,
                    server_num
                )
                
                # Write to log file
                self._write_log_entry(log_file, log_entry)
                
                # Wait for next entry
                base_interval = self.config['simulation']['log_interval_seconds']
                variation = self.config['simulation'].get('log_interval_variation', 0)
                sleep_time = base_interval + random.uniform(-variation, variation)
                sleep_time = max(0.1, sleep_time)  # Minimum 0.1 seconds
                
                time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.error(f"Error in server {server_num} simulation: {e}")
                time.sleep(1)
    
    def _write_log_entry(self, log_file, entry):
        """Write log entry to file with proper encoding"""
        encoding = self.config['output']['encoding']
        
        # Handle log rotation if enabled
        if self.config['output']['log_rotation']['enabled']:
            self._handle_log_rotation(log_file)
        
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
    
    def stop_simulation(self):
        """Stop the simulation"""
        self.logger.info("Stopping simulation...")
        self.running = False
        
        for thread in self.threads:
            thread.join(timeout=5)
        
        self.logger.info("Simulation stopped")

def main():
    simulator = SQLServerLogSimulator()
    
    try:
        simulator.start_simulation()
        print("SQL Server Log Simulator started. Press Ctrl+C to stop...")
        
        while simulator.running:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        simulator.stop_simulation()

if __name__ == "__main__":
    main()