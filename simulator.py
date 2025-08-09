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

        bursts_config = self.config.get('bursts', {})
        burst_enabled = bursts_config.get('enabled', False)
        burst_chance = bursts_config.get('chance_per_entry', 0.0)
        burst_duration_range = tuple(bursts_config.get('duration_seconds_range', [10, 30]))
        burst_interval_multiplier = bursts_config.get('interval_multiplier', 0.25)
        burst_weight_override = bursts_config.get('error_type_weights_override', {})

        in_burst = False
        burst_ends_at = None

        while self.running:
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
                
                # Wait for next entry
                base_interval, variation = self._server_interval(server_num)
                sleep_time = base_interval + random.uniform(-variation, variation)
                if in_burst:
                    sleep_time *= max(0.01, burst_interval_multiplier)
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