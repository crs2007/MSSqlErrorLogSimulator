# MS SQL Server Error Log Simulator - Project Instructions

## 📋 Project Overview
This project simulates Microsoft SQL Server error log files to aid in testing and validating monitoring tools, alerting systems, and log parsers without relying on live SQL Server instances. It creates realistic log entries for multiple servers at configurable intervals, mimicking real SQL Server ERRORLOG files.

## 🎯 Key Features
- **Multi-server simulation**: Generate logs for multiple SQL Server instances
- **Realistic log formats**: Matches actual SQL Server ERRORLOG structure and content
- **Configurable intervals**: Adjustable timing between log entries
- **Per-server rates**: Different log rates per server via `simulation.per_server`
- **Random burst spikes**: Short, high-volume periods with biased error types
- **Diverse error types**: Various SQL Server scenarios including startup, deadlocks, login failures, etc.
- **Proper encoding**: UTF-16 LE encoding to match real SQL Server logs
- **Random generation**: Realistic randomization of timestamps, SPIDs, and error scenarios

---

## 📁 Project Structure
```
/SqlErrorLogSimulator/
│
├── config.json                     # Main configuration file
├── config_kafka_example.json       # Example config with Kafka enabled
├── simulator.py                    # Main simulator script
├── utils.py                        # Utility functions and helpers
├── kafka_handler.py                # Kafka publishing handler
├── requirements.txt                # Python dependencies
├── templates/                      # Error message templates
│   ├── startup.txt                 # Server startup messages
│   ├── deadlock.txt               # Deadlock scenarios
│   ├── login_failed.txt           # Authentication failures
│   ├── io_error.txt               # I/O related errors
│   ├── timeout.txt                # Connection timeouts
│   ├── replication.txt            # Replication errors
│   ├── availability_group.txt     # Always On AG messages
│   ├── service_broker.txt         # Service Broker messages
│   ├── trace_events.txt           # SQL Trace events
│   └── maintenance.txt            # Database maintenance messages
├── data/                          # Sample data files
│   ├── server_names.txt           # Pool of server names
│   ├── database_names.txt         # Pool of database names
│   ├── user_names.txt             # Pool of user names
│   └── ip_addresses.txt           # Pool of IP addresses
└── /Server1/                      # Generated server log folders
    └── ERRORLOG                   # Simulated log file (no extension)
```

---

## ⚙️ Configuration (config.json)
```json
{
  "simulation": {
    "server_count": 3,
    "log_interval_seconds": 5,
    "log_interval_variation": 2,
    "max_runtime_minutes": 0,
    "timezone_offset": "+03:00",
    "per_server": {
      "1": { "log_interval_seconds": 5, "log_interval_variation": 2 },
      "2": { "log_interval_seconds": 3, "log_interval_variation": 1 },
      "3": { "log_interval_seconds": 7, "log_interval_variation": 2 }
    }
  },
  "error_types": {
    "startup": {
      "weight": 5,
      "enabled": true
    },
    "deadlock": {
      "weight": 15,
      "enabled": true
    },
    "login_failed": {
      "weight": 25,
      "enabled": true
    },
    "timeout": {
      "weight": 20,
      "enabled": true
    },
    "io_error": {
      "weight": 10,
      "enabled": true
    },
    "replication": {
      "weight": 8,
      "enabled": true
    },
    "availability_group": {
      "weight": 7,
      "enabled": true
    },
    "service_broker": {
      "weight": 5,
      "enabled": true
    },
    "trace_events": {
      "weight": 3,
      "enabled": true
    },
    "maintenance": {
      "weight": 2,
      "enabled": true
    }
  },
  "output": {
    "encoding": "utf-16le",
    "log_rotation": {
      "enabled": true,
      "max_size_mb": 10,
      "max_files": 5
    }
  },
  "kafka": {
    "enabled": false,
    "bootstrap_servers": "localhost:29092",
    "topic": "sql-server-raw-errorlogs",
    "producer_options": {
      "acks": "all",
      "retries": 3,
      "batch_size": 16384,
      "linger_ms": 10,
      "buffer_memory": 33554432
    }
  },
  "bursts": {
    "enabled": false,
    "chance_per_entry": 0.02,
    "duration_seconds_range": [10, 30],
    "interval_multiplier": 0.25,
    "error_type_weights_override": {
      "login_failed": 40,
      "deadlock": 25,
      "timeout": 25
    }
  },
  "randomization": {
    "spid_range": [50, 3000],
    "severity_levels": [10, 11, 14, 16, 17, 18, 20],
    "state_range": [1, 127],
    "error_numbers": {
      "login_failed": [18456],
      "deadlock": [1205],
      "timeout": [2, 258, 1222],
      "io_error": [823, 824, 825],
      "replication": [14151, 20032, 21075],
      "availability_group": [35201, 35202, 35206]
    }
  }
}
```

### Configuration Parameters:
- **server_count**: Number of simulated servers (creates Server1, Server2, etc.)
- **log_interval_seconds**: Base time between log entries for each server
- **log_interval_variation**: Random variation added/subtracted from interval
- **per_server**: Optional per-server overrides for `log_interval_seconds` and `log_interval_variation` keyed by server number as string
- **error_types**: Weighted probability for different error types
- **encoding**: Output file encoding (use `utf-16le` to mimic real SQL Server logs)
- **timezone_offset**: Timezone offset for timestamps
- **kafka**: Kafka publishing configuration (see Kafka Integration section below)
- **bursts**: Optional random burst spikes configuration
  - `enabled`: Turn bursts on/off
  - `chance_per_entry`: Probability to start a burst when generating an entry
  - `duration_seconds_range`: Min/max seconds a burst lasts
  - `interval_multiplier`: Multiply sleep time during burst (e.g., 0.25 makes it 4x faster)
  - `error_type_weights_override`: Override weights while in burst to bias specific errors

---

## 🚀 Kafka Integration

### Overview
The simulator can optionally publish log entries to Kafka topics for real-time streaming and integration with data pipelines. This feature is controlled by the `kafka.enabled` configuration parameter.

### Configuration
```json
{
  "kafka": {
    "enabled": false,
    "bootstrap_servers": "localhost:29092",
    "topic": "sql-server-raw-errorlogs",
    "producer_options": {
      "acks": "all",
      "retries": 3,
      "batch_size": 16384,
      "linger_ms": 10,
      "buffer_memory": 33554432
    }
  }
}
```

### Kafka Configuration Parameters:
- **enabled**: Enable/disable Kafka publishing (default: false)
- **bootstrap_servers**: Comma-separated list of Kafka brokers (default: localhost:29092)
- **topic**: Target Kafka topic for log entries (default: sql-server-raw-errorlogs)
- **producer_options**: Additional Kafka producer configuration options

### Message Format
Each log entry published to Kafka includes:
```json
{
  "log_entry": "2025-08-02 08:54:30.34 spid35s     Deadlock encountered...",
  "server_num": 1,
  "server_name": "Server1",
  "error_type": "deadlock",
  "timestamp": "2025-08-02T08:54:30.340000",
  "metadata": {
    "source": "sql_error_log_simulator",
    "version": "1.0"
  }
}
```

### Setup Instructions
1. **Install Kafka Dependencies**:
   ```bash
   pip install kafka-python
   ```

2. **Configure Kafka Connection**:
   - Set `kafka.enabled: true` in config.json
   - Update `bootstrap_servers` to point to your Kafka cluster
   - Optionally customize the topic name

3. **Create Kafka Topic** (if using local Kafka):
   ```bash
   kafka-topics.sh --create --topic sql-server-raw-errorlogs \
     --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1
   ```

4. **Run Simulator**:
   ```bash
   # Use default config (Kafka disabled)
   python simulator.py
   
   # Or use example config with Kafka enabled
   cp config_kafka_example.json config.json
   python simulator.py
   ```

### Consumer Example
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'sql-server-raw-errorlogs',
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    log_data = message.value
    print(f"Server: {log_data['server_name']}")
    print(f"Error Type: {log_data['error_type']}")
    print(f"Log Entry: {log_data['log_entry']}")
    print("---")
```

### Error Handling
- Kafka connection failures are logged but don't stop the simulation
- Failed messages are logged with error details
- Producer automatically retries failed sends based on configuration
- Graceful shutdown ensures all pending messages are flushed

---

## 📂 Template Examples

### templates/startup.txt
```
{timestamp} Server      Microsoft SQL Server {version} - {build_info}
{timestamp} Server      UTC adjustment: {timezone}
{timestamp} Server      (c) Microsoft Corporation.
{timestamp} Server      All rights reserved.
{timestamp} Server      Server process ID is {process_id}.
{timestamp} Server      Authentication mode is {auth_mode}.
{timestamp} Server      Logging SQL Server messages in file '{log_path}'.
{timestamp} Server      The service account is '{service_account}'. This is an informational message; no user action is required.
{timestamp} Server      SQL Server is now ready for client connections. This is an informational message; no user action is required.
```

### templates/deadlock.txt
```
{timestamp} spid{spid}s     Deadlock encountered .... Printing deadlock information
{timestamp} spid{spid}s     Wait-for graph
{timestamp} spid{spid}s     Node:1
{timestamp} spid{spid}s     RID: {db_id}:1:{page_id}:0                 CleanCnt:2 Mode:X Flags: 0x3
{timestamp} spid{spid}s      Grant List 1:
{timestamp} spid{spid}s        Owner:0x{owner_id} Mode: X        Flg:0x40 Ref:1 Life:02000000 SPID:{victim_spid} ECID:0
{timestamp} spid{spid}s        SPID: {victim_spid} ECID: 0 Statement Type: UPDATE Line #: {line_number}
{timestamp} spid{spid}s        Input Buf: Language Event: {sql_statement}
{timestamp} spid{spid}s     Victim Resource Owner:
{timestamp} spid{spid}s      ResType:LockOwner Mode: U SPID:{victim_spid} Cost:(0/{cost})
```

### templates/login_failed.txt
```
{timestamp} Logon        Error: 18456, Severity: 14, State: {state}.
{timestamp} Logon        Login failed for user '{username}'. Reason: {failure_reason} [CLIENT: {client_ip}]
```

### templates/io_error.txt
```
{timestamp} spid{spid}        Error: {error_number}, Severity: {severity}, State: {state}.
{timestamp} spid{spid}        {io_error_message}
{timestamp} spid{spid}        Possible failure reasons: Problems with the hardware, the device driver, or network-related issues.
```

---

## 🔧 Core Implementation Files

### requirements.txt
```
python-dateutil>=2.8.0
kafka-python>=2.0.2
```

### kafka_handler.py (Kafka Publishing)
```python
#!/usr/bin/env python3
"""
Kafka Handler for SQL Server Error Log Simulator
Handles publishing log entries to Kafka topics
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

class KafkaLogPublisher:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.producer = None
        self.logger = logging.getLogger(__name__)
        self.enabled = config.get('enabled', False)
        
        if not self.enabled:
            self.logger.info("Kafka publishing is disabled")
            return
            
        if not KAFKA_AVAILABLE:
            self.logger.error("Kafka library not available")
            self.enabled = False
            return
            
        self._initialize_producer()
    
    def publish_log_entry(self, log_entry: str, server_num: int, 
                         error_type: str, timestamp: datetime, 
                         metadata: Optional[Dict[str, Any]] = None):
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
            
            key = f"server_{server_num}"
            future = self.producer.send(self.topic, key=key, value=message)
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
        except Exception as e:
            self.logger.error(f"Failed to publish log entry to Kafka: {e}")
    
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Kafka producer closed")
```

### simulator.py (Main Script)
```python
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
from kafka_handler import KafkaLogPublisher

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
        
        self.kafka_publisher = KafkaLogPublisher(self.config)
        
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
                
                # Publish to Kafka if enabled
                self._publish_to_kafka(log_entry, server_num, error_type)
                
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
```

---

## 🛠️ Utility Functions (utils.py)

### Key Functions:
1. **load_templates()**: Load all error message templates
2. **generate_timestamp()**: Create SQL Server formatted timestamps
3. **select_weighted_error_type()**: Choose error type based on weights
4. **generate_error_entry()**: Create complete log entries with variable substitution
5. **setup_logging()**: Configure Python logging
6. **create_server_directories()**: Create server folder structure

---

## 📊 Sample Data Files

### data/server_names.txt
```
TestServer
ProductionDB
DevServer
StagingSQL
BackupServer
ReportingDB
```

### data/database_names.txt
```
MainDB
UserDB
InventoryDB
OrdersDB
ReportsDB
TempDB
LoggingDB
```

### data/user_names.txt
```
admin
sa
backup_user
report_user
app_service
web_user
sync_agent
```

---

## ▶️ Running the Simulator

### Basic Usage:
```bash
# Install dependencies
pip install -r requirements.txt

# Run the simulator
python simulator.py
```

### Advanced Usage:
```bash
# Run with custom config
python simulator.py --config custom_config.json

# Run for specific duration
python simulator.py --runtime 60  # Run for 60 minutes

# Run in background
nohup python simulator.py &
```

---

## 📈 Generated Log Examples

### Startup Sequence:
```
2025-08-02 08:49:47.02 Server      Microsoft SQL Server 2022 (RTM-GDR) (KB5058712) - 16.0.1140.6 (X64)
2025-08-02 08:49:47.05 Server      UTC adjustment: 3:00
2025-08-02 08:49:47.06 Server      Server process ID is 6256.
2025-08-02 08:49:52.34 spid42s     SQL Server is now ready for client connections.
```

### Deadlock Event:
```
2025-08-02 08:54:30.34 spid35s     Deadlock encountered .... Printing deadlock information
2025-08-02 08:54:30.34 spid35s     Wait-for graph
2025-08-02 08:54:30.34 spid35s     Victim Resource Owner: SPID:91 Cost:(0/300)
```

### Login Failure:
```
2025-08-02 09:11:08.430 Logon      Error: 18456, Severity: 14, State: 38.
2025-08-02 09:11:08.430 Logon      Login failed for user 'test_user'. Reason: Could not find a login matching the name provided. [CLIENT: 192.168.1.100]
```

---

## 🔧 Customization Options

### Adding New Error Types:
1. Create new template file in `templates/`
2. Add error type to `config.json`
3. Update weight distribution
4. Add specific error numbers if needed

### Custom Server Names:
- Modify folder naming pattern in `create_server_directories()`
- Update server identification logic

### Different Time Zones:
- Adjust `timezone_offset` in configuration
- Modify timestamp generation in `utils.py`

---

## 🎯 Use Cases

### Testing Scenarios:
- **Log Parser Validation**: Test log parsing tools against realistic data
- **Monitoring System Testing**: Validate alerting rules and thresholds
- **Performance Testing**: Generate high-volume logs for stress testing
- **Training**: Educational tool for SQL Server log analysis

### Integration Examples:
- **Splunk/ELK Integration**: Forward logs to centralized logging
- **PowerBI Dashboards**: Create monitoring visualizations
- **Custom Alerting**: Test notification systems
- **Automated Analysis**: Train machine learning models
- **Kafka Streaming**: Real-time log streaming to data pipelines
- **Apache Spark**: Process logs in real-time analytics workflows
- **Flink/Storm**: Stream processing and complex event processing
- **Data Lakes**: Ingestion into data lake architectures

---

## 📚 Advanced Features

### Log Rotation:
- Automatic file rotation based on size
- Configurable number of backup files
- Maintains realistic file structure

### Performance Optimization:
- Multi-threaded server simulation
- Configurable batch writing
- Memory-efficient template processing

### Extensibility:
- Plugin architecture for custom error types
- External data source integration
- REST API for runtime control

---

## 🛡️ Security Considerations

- **No Real Credentials**: Uses only fake usernames and passwords
- **Sanitized IP Addresses**: Uses private IP ranges only
- **No Sensitive Data**: Templates contain no real server information
- **Safe File Operations**: Proper error handling for file I/O

---

## 🔍 Troubleshooting

### Common Issues:
1. **Permission Errors**: Ensure write access to output directories
2. **Encoding Issues**: Verify UTF-16 LE support in text editors
3. **High CPU Usage**: Reduce log frequency or server count
4. **Disk Space**: Monitor output file sizes
5. **Kafka Connection Issues**: Verify Kafka broker connectivity and topic existence
6. **Kafka Dependencies**: Ensure kafka-python is installed: `pip install kafka-python`

### Debug Mode:
```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 📋 Future Enhancements

- **Web Interface**: Browser-based configuration and monitoring
- **Real-time Streaming**: Live log streaming via websockets
- **Cloud Integration**: Azure/AWS log forwarding
- **Machine Learning**: Intelligent error pattern generation
- **Performance Metrics**: Built-in monitoring and statistics

---
> **Created by**: Sharon Rimer | **Version**: 1.2 | **Last Updated**: August 2025