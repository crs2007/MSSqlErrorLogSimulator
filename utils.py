#!/usr/bin/env python3
"""
Utility functions for MS SQL Server Error Log Simulator
Contains helper functions for template loading, data generation, and file operations
"""

import json
import os
import random
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path
import string

def setup_logging():
    """Configure logging for the simulator"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('simulator.log'),
            logging.StreamHandler()
        ]
    )

def create_server_directories(server_count):
    """Create directory structure for all servers"""
    for i in range(1, server_count + 1):
        server_dir = Path(f"Server{i}")
        server_dir.mkdir(exist_ok=True)
        
        # Create initial ERRORLOG file if it doesn't exist
        log_file = server_dir / "ERRORLOG"
        if not log_file.exists():
            log_file.touch()

def load_templates():
    """Load all error message templates from the templates directory"""
    templates = {}
    templates_dir = Path("templates")
    
    if not templates_dir.exists():
        templates_dir.mkdir(exist_ok=True)
        create_default_templates()
    
    for template_file in templates_dir.glob("*.txt"):
        template_name = template_file.stem
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                templates[template_name] = f.read().strip()
        except Exception as e:
            logging.error(f"Failed to load template {template_file}: {e}")
    
    return templates

def load_sample_data():
    """Load sample data from data directory"""
    sample_data = {}
    data_dir = Path("data")
    
    if not data_dir.exists():
        data_dir.mkdir(exist_ok=True)
        create_default_sample_data()
    
    # Load each data file
    data_files = {
        'server_names': 'server_names.txt',
        'database_names': 'database_names.txt', 
        'user_names': 'user_names.txt',
        'ip_addresses': 'ip_addresses.txt'
    }
    
    for key, filename in data_files.items():
        file_path = data_dir / filename
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    sample_data[key] = [line.strip() for line in f if line.strip()]
            else:
                sample_data[key] = get_default_data(key)
        except Exception as e:
            logging.error(f"Failed to load {filename}: {e}")
            sample_data[key] = get_default_data(key)
    
    return sample_data

def get_default_data(data_type):
    """Get default data for various categories"""
    defaults = {
        'server_names': ['TestServer', 'ProductionDB', 'DevServer', 'StagingSQL', 'BackupServer'],
        'database_names': ['MainDB', 'UserDB', 'InventoryDB', 'OrdersDB', 'ReportsDB', 'TempDB'],
        'user_names': ['admin', 'sa', 'backup_user', 'report_user', 'app_service', 'web_user'],
        'ip_addresses': ['192.168.1.100', '192.168.1.101', '192.168.1.102', '10.0.0.50', '172.16.1.25']
    }
    return defaults.get(data_type, [])

def generate_timestamp(timezone_offset="+03:00"):
    """Generate SQL Server formatted timestamp"""
    now = datetime.now()
    
    # Add some random variation (-30 seconds to +30 seconds)
    variation = random.randint(-30, 30)
    timestamp = now + timedelta(seconds=variation)
    
    # Format: 2025-08-02 08:49:47.02
    formatted = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]  # Remove last 4 digits of microseconds
    
    return formatted

def select_weighted_error_type(error_types_config):
    """Select error type based on configured weights"""
    enabled_types = {k: v for k, v in error_types_config.items() if v.get('enabled', True)}
    
    if not enabled_types:
        return 'login_failed'  # fallback
    
    # Create weighted list
    weighted_types = []
    for error_type, config in enabled_types.items():
        weight = config.get('weight', 1)
        weighted_types.extend([error_type] * weight)
    
    return random.choice(weighted_types)

def generate_spid():
    """Generate realistic SPID (SQL Process ID)"""
    return random.randint(50, 3000)

def generate_error_number(error_type, config):
    """Generate appropriate error number for error type"""
    error_numbers = config.get('randomization', {}).get('error_numbers', {})
    
    if error_type in error_numbers:
        return random.choice(error_numbers[error_type])
    
    # Default error numbers for common types
    defaults = {
        'login_failed': 18456,
        'deadlock': 1205,
        'timeout': random.choice([2, 258, 1222]),
        'io_error': random.choice([823, 824, 825]),
        'replication': random.choice([14151, 20032, 21075]),
        'availability_group': random.choice([35201, 35202, 35206])
    }
    
    return defaults.get(error_type, 50000)

def generate_severity():
    """Generate realistic severity level"""
    return random.choice([10, 11, 14, 16, 17, 18, 20])

def generate_state():
    """Generate random state number"""
    return random.randint(1, 127)

def generate_client_ip(sample_data):
    """Generate realistic client IP address"""
    if 'ip_addresses' in sample_data and sample_data['ip_addresses']:
        return random.choice(sample_data['ip_addresses'])
    
    # Generate random private IP
    ranges = [
        "192.168.{}.{}".format(random.randint(1, 255), random.randint(1, 254)),
        "10.{}.{}.{}".format(random.randint(0, 255), random.randint(0, 255), random.randint(1, 254)),
        "172.{}.{}.{}".format(random.randint(16, 31), random.randint(0, 255), random.randint(1, 254))
    ]
    return random.choice(ranges)

def generate_hex_id(length=16):
    """Generate random hexadecimal ID"""
    return ''.join(random.choices('0123456789ABCDEF', k=length))

def generate_process_id():
    """Generate realistic process ID"""
    return random.randint(1000, 65535)

def generate_database_id():
    """Generate database ID"""
    return random.randint(1, 50)

def generate_page_id():
    """Generate page ID for deadlock scenarios"""
    return random.randint(100, 9999)

def generate_error_entry(error_type, templates, sample_data, config, server_num):
    """Generate complete error log entry based on type and templates"""
    
    if error_type not in templates:
        # Fallback to a simple error message
        timestamp = generate_timestamp(config.get('simulation', {}).get('timezone_offset', '+03:00'))
        spid = generate_spid()
        return f"{timestamp} spid{spid}        Unknown error type: {error_type}"
    
    template = templates[error_type]
    timestamp = generate_timestamp(config.get('simulation', {}).get('timezone_offset', '+03:00'))
    spid = generate_spid()
    
    # Common variables for all templates - ensure same SPID and timestamp for paired logs
    variables = {
        'timestamp': timestamp,
        'spid': spid,
        'server_num': server_num,
        'error_number': generate_error_number(error_type, config),
        'severity': generate_severity(),
        'state': generate_state(),
        'database_id': generate_database_id(),
        'db_id': generate_database_id(),  # Alternative name for database_id
        'process_id': generate_process_id(),
        'page_id': generate_page_id(),
        'client_ip': generate_client_ip(sample_data),
        'hex_id': generate_hex_id(),
        'owner_id': generate_hex_id(),
        'victim_spid': generate_spid(),
        'line_number': random.randint(1, 100),
        'cost': random.randint(100, 1000),
        'database_name': random.choice(sample_data.get('database_names', ['TestDB'])),
        'username': random.choice(sample_data.get('user_names', ['test_user'])),
        'table_name': random.choice(['TableA', 'TableB', 'Orders', 'Customers', 'Products']),
        'timeout_duration': random.randint(30, 600),
        'worker_pool_size': random.randint(4, 16),
        'build_info': f"Jun {random.randint(1, 28)} 2025 {random.randint(10, 23)}:{random.randint(10, 59)}:{random.randint(10, 59)}",
        'timezone': config.get('simulation', {}).get('timezone_offset', '+03:00'),
        'auth_mode': random.choice(['MIXED', 'WINDOWS']),
        'log_path': f"E:\\SQL\\MSSQL\\Log\\ERRORLOG",
        'service_account': f"NT Service\\MSSQL$Server{server_num}",
        'version': random.choice([
            "2022 (RTM-GDR) (KB5058712) - 16.0.1140.6 (X64)",
            "2022 (RTM-CU13-GDR) (KB5040939) - 16.0.4131.2 (X64)",
            "2019 (RTM-CU15) (KB5008996) - 15.0.4198.2 (X64)",
            "2017 (RTM-CU31) (KB5016884) - 14.0.3456.2 (X64)"
        ]),
        'service_name': random.choice(['OrderService', 'NotificationService', 'ProcessingService', 'BackupService'])
    }
    
    # Type-specific variables
    if error_type == 'startup':
        variables.update(generate_startup_variables(sample_data, server_num))
    elif error_type == 'login_failed':
        variables.update(generate_login_failure_variables(sample_data))
    elif error_type == 'deadlock':
        variables.update(generate_deadlock_variables(sample_data))
    elif error_type == 'io_error':
        variables.update(generate_io_error_variables())
    elif error_type == 'replication':
        variables.update(generate_replication_variables(sample_data))
    elif error_type == 'availability_group':
        variables.update(generate_ag_variables(sample_data))
    elif error_type == 'service_broker':
        variables.update(generate_service_broker_variables(sample_data))
    elif error_type == 'timeout':
        variables.update(generate_timeout_variables(sample_data))
    elif error_type == 'backup':
        variables.update(generate_backup_variables(sample_data))
    elif error_type == 'memory_pressure':
        variables.update(generate_memory_pressure_variables(sample_data))
    elif error_type == 'tempdb_issues':
        variables.update(generate_tempdb_variables(sample_data))
    elif error_type == 'blocking':
        variables.update(generate_blocking_variables(sample_data))
    elif error_type == 'performance_warnings':
        variables.update(generate_performance_variables(sample_data))
    elif error_type == 'connection_pool':
        variables.update(generate_connection_pool_variables(sample_data))
    elif error_type == 'corruption':
        variables.update(generate_corruption_variables(sample_data))
    elif error_type == 'index_maintenance':
        variables.update(generate_index_maintenance_variables(sample_data))
    elif error_type == 'statistics':
        variables.update(generate_statistics_variables(sample_data))
    elif error_type == 'job_failures':
        variables.update(generate_job_failure_variables(sample_data))
    elif error_type == 'linked_server':
        variables.update(generate_linked_server_variables(sample_data))
    elif error_type == 'maintenance':
        variables.update(generate_maintenance_variables(sample_data))
    
    # Replace variables in template
    try:
        return template.format(**variables)
    except KeyError as e:
        logging.warning(f"Missing variable {e} in template {error_type}")
        # Return template with unreplaced variables for debugging
        return template



def generate_startup_variables(sample_data, server_num):
    """Generate variables specific to startup messages"""
    sql_versions = [
        "2022 (RTM-GDR) (KB5058712) - 16.0.1140.6 (X64)",
        "2022 (RTM-CU13-GDR) (KB5040939) - 16.0.4131.2 (X64)",
        "2019 (RTM-CU15) (KB5008996) - 15.0.4198.2 (X64)",
        "2017 (RTM-CU31) (KB5016884) - 14.0.3456.2 (X64)"
    ]
    
    auth_modes = ["MIXED", "WINDOWS"]
    
    return {
        'version': random.choice(sql_versions),
        'build_info': f"Jun {random.randint(1, 28)} 2025 {random.randint(10, 23)}:{random.randint(10, 59)}:{random.randint(10, 59)}",
        'timezone': random.choice(["+03:00", "+00:00", "-05:00", "-08:00"]),
        'auth_mode': random.choice(auth_modes),
        'log_path': f"E:\\SQL\\MSSQL\\Log\\ERRORLOG",
        'service_account': f"NT Service\\MSSQL$Server{server_num}"
    }

def generate_login_failure_variables(sample_data):
    """Generate variables for login failure messages"""
    failure_reasons = [
        "Password did not match",
        "Could not find a login matching the name provided",
        "Login is from an untrusted domain and cannot be used with Windows authentication",
        "The login is disabled",
        "Failed to open the explicitly specified database"
    ]
    
    username = random.choice(sample_data.get('user_names', ['test_user']))
    
    return {
        'username': username,
        'failure_reason': random.choice(failure_reasons)
    }

def generate_deadlock_variables(sample_data):
    """Generate variables for deadlock messages"""
    table_names = ['TableA', 'TableB', 'Orders', 'Customers', 'Products', 'Inventory']
    
    sql_statements = [
        "UPDATE TableA SET Name = 'Updated' WHERE Id = 101",
        "UPDATE TableB SET Status = 'Active' WHERE Id = 1001",
        "DELETE FROM Orders WHERE OrderDate < '2025-01-01'",
        "INSERT INTO Customers (Name, Email) VALUES ('Test', 'test@email.com')"
    ]
    
    return {
        'table_name': random.choice(table_names),
        'sql_statement': random.choice(sql_statements)
    }

def generate_io_error_variables():
    """Generate variables for I/O error messages"""
    io_messages = [
        "I/O error (bad page ID) detected during read at offset 0x00000000000000 in file",
        "Write error during log flush",
        "Read error during checkpoint operation",
        "The operating system returned error to SQL Server during a write"
    ]
    
    return {
        'io_error_message': random.choice(io_messages)
    }

def generate_replication_variables(sample_data):
    """Generate variables for replication messages"""
    publications = ['PUB_Orders', 'PUB_Customers', 'PUB_AllData', 'PUB_Changes']
    
    return {
        'publication_name': random.choice(publications),
        'subscriber_name': random.choice(sample_data.get('server_names', ['Subscriber1']))
    }

def generate_ag_variables(sample_data):
    """Generate variables for Always On Availability Group messages"""
    ag_names = ['MainAG', 'ProductionAG', 'HADR_AG', 'TestAG']
    ag_states = ['PRIMARY_NORMAL', 'SECONDARY_NORMAL', 'RESOLVING_NORMAL', 'NOT_AVAILABLE']
    
    return {
        'ag_name': random.choice(ag_names),
        'ag_state': random.choice(ag_states),
        'replica_id': generate_hex_id(36).lower(),
        'subscriber_name': random.choice(sample_data.get('server_names', ['Subscriber1']))
    }

def generate_service_broker_variables(sample_data):
    """Generate variables for Service Broker messages"""
    return {
        'endpoint_name': 'ServiceBrokerEndpoint',
        'service_name': random.choice(['OrderService', 'NotificationService', 'ProcessingService'])
    }

def generate_timeout_variables(sample_data):
    """Generate variables for timeout messages"""
    timeout_types = [
        "Query timeout expired",
        "Login timeout expired",
        "Connection timeout expired",
        "Lock request time out period exceeded"
    ]
    
    return {
        'timeout_message': random.choice(timeout_types),
        'timeout_duration': random.randint(30, 600)
    }

def generate_backup_variables(sample_data):
    """Generate variables for backup/restore messages"""
    recovery_models = ['SIMPLE', 'FULL', 'BULK_LOGGED']
    return {
        'ag_state': random.choice(recovery_models)
    }

def generate_memory_pressure_variables(sample_data):
    """Generate variables for memory pressure messages"""
    memory_messages = [
        "A significant part of sql server process memory has been paged out. This may result in a performance degradation.",
        "Memory pressure detected. SQL Server is experiencing memory pressure and may need to release memory.",
        "Buffer pool memory pressure detected. SQL Server is experiencing buffer pool memory pressure.",
        "Memory manager is experiencing memory pressure. This may result in performance degradation."
    ]
    
    return {
        'memory_message': random.choice(memory_messages),
        'memory_percentage': random.randint(80, 95),
        'buffer_pool_size': random.randint(1024, 8192)
    }

def generate_tempdb_variables(sample_data):
    """Generate variables for tempdb issues"""
    tempdb_issues = [
        "Tempdb space usage is high. Consider adding more tempdb files or increasing file size.",
        "Tempdb contention detected. Multiple sessions are waiting for tempdb resources.",
        "Tempdb file growth is occurring frequently. Consider pre-sizing tempdb files.",
        "Tempdb log file is growing rapidly. Check for long-running transactions."
    ]
    
    return {
        'tempdb_message': random.choice(tempdb_issues),
        'tempdb_usage_percent': random.randint(70, 95),
        'tempdb_file_count': random.randint(1, 8)
    }

def generate_blocking_variables(sample_data):
    """Generate variables for blocking issues"""
    blocking_scenarios = [
        "Session {spid} is blocked by session {victim_spid} on resource {table_name}",
        "Long-running transaction detected. Session {spid} has been running for {timeout_duration} seconds",
        "Blocking chain detected. Multiple sessions are waiting for resources",
        "Deadlock victim process {victim_spid} was chosen"
    ]
    
    return {
        'blocking_message': random.choice(blocking_scenarios),
        'blocking_duration': random.randint(30, 300),
        'blocked_session_count': random.randint(1, 10)
    }

def generate_performance_variables(sample_data):
    """Generate variables for performance warnings"""
    performance_issues = [
        "Query execution time exceeded threshold. Consider optimizing the query.",
        "High CPU usage detected. SQL Server is experiencing high CPU utilization.",
        "Disk I/O bottleneck detected. Consider adding more disk spindles or using SSDs.",
        "Network latency is high. Check network connectivity and bandwidth."
    ]
    
    return {
        'performance_message': random.choice(performance_issues),
        'cpu_percentage': random.randint(70, 95),
        'io_wait_percentage': random.randint(10, 50)
    }

def generate_connection_pool_variables(sample_data):
    """Generate variables for connection pool issues"""
    pool_issues = [
        "Connection pool is exhausted. No available connections in the pool.",
        "Connection timeout occurred. Client could not establish connection within timeout period.",
        "Maximum number of connections reached. Cannot accept new connections.",
        "Connection pool pressure detected. High number of connection requests."
    ]
    
    return {
        'pool_message': random.choice(pool_issues),
        'max_connections': random.randint(100, 1000),
        'current_connections': random.randint(80, 95)
    }

def generate_corruption_variables(sample_data):
    """Generate variables for corruption issues"""
    corruption_types = [
        "Page corruption detected in database '{database_name}'",
        "Checksum failure detected on page {page_id} in database '{database_name}'",
        "Torn page detected. Page {page_id} in database '{database_name}' is corrupted",
        "Database '{database_name}' is marked as suspect due to corruption"
    ]
    
    return {
        'corruption_message': random.choice(corruption_types),
        'corrupted_page_count': random.randint(1, 10),
        'recovery_model': random.choice(['FULL', 'SIMPLE', 'BULK_LOGGED'])
    }

def generate_index_maintenance_variables(sample_data):
    """Generate variables for index maintenance issues"""
    index_issues = [
        "Index rebuild operation failed on index '{table_name}_IX' in database '{database_name}'",
        "Index fragmentation is high on index '{table_name}_IX'. Consider rebuilding the index.",
        "Index maintenance job failed. Error occurred during index reorganization.",
        "Statistics are out of date on table '{table_name}'. Consider updating statistics."
    ]
    
    return {
        'index_message': random.choice(index_issues),
        'fragmentation_percent': random.randint(20, 80),
        'index_size_mb': random.randint(10, 1000)
    }

def generate_statistics_variables(sample_data):
    """Generate variables for statistics issues"""
    stats_issues = [
        "Statistics are stale on table '{table_name}'. Last updated: {build_info}",
        "Auto-update statistics is disabled on table '{table_name}'",
        "Statistics update failed on table '{table_name}' due to insufficient permissions",
        "Outdated statistics detected. Query performance may be affected."
    ]
    
    return {
        'stats_message': random.choice(stats_issues),
        'stats_age_days': random.randint(1, 365),
        'table_row_count': random.randint(1000, 1000000)
    }

def generate_job_failure_variables(sample_data):
    """Generate variables for job failure issues"""
    job_names = ['DatabaseBackup', 'IndexMaintenance', 'StatisticsUpdate', 'LogCleanup', 'DataArchive']
    failure_reasons = [
        "Job '{service_name}' failed. Error: Login failed for user '{username}'",
        "Job '{service_name}' failed. Error: Database '{database_name}' is not accessible",
        "Job '{service_name}' failed. Error: Insufficient disk space",
        "Job '{service_name}' failed. Error: Timeout expired"
    ]
    
    return {
        'job_name': random.choice(job_names),
        'job_failure_message': random.choice(failure_reasons),
        'job_duration_minutes': random.randint(5, 120),
        'retry_count': random.randint(0, 3)
    }

def generate_linked_server_variables(sample_data):
    """Generate variables for linked server issues"""
    linked_server_names = ['RemoteServer1', 'RemoteServer2', 'LegacyServer', 'ReportingServer']
    linked_issues = [
        "Linked server '{service_name}' connection failed. Error: Login timeout expired",
        "Linked server '{service_name}' is not responding. Network connectivity issue detected",
        "Linked server '{service_name}' authentication failed. Invalid credentials provided",
        "Linked server '{service_name}' query timeout. Remote server is not responding"
    ]
    
    return {
        'linked_server_name': random.choice(linked_server_names),
        'linked_server_message': random.choice(linked_issues),
        'connection_timeout': random.randint(30, 300),
        'remote_database': random.choice(sample_data.get('database_names', ['RemoteDB']))
    }

def generate_maintenance_variables(sample_data):
    """Generate variables for maintenance operations"""
    service_names = ['AUTO_CLOSE', 'AUTO_SHRINK', 'AUTO_UPDATE_STATISTICS', 'READ_ONLY', 'SINGLE_USER']
    ag_states = ['ON', 'OFF', 'READ_ONLY', 'READ_WRITE']
    table_names = ['Users', 'Orders', 'Products', 'Inventory', 'Logs', 'Settings']
    
    return {
        'worker_pool_size': random.randint(1, 16),
        'cost': random.randint(1, 1000),
        'service_name': random.choice(service_names),
        'ag_state': random.choice(ag_states),
        'table_name': random.choice(table_names)
    }

def create_default_templates():
    """Create default template files if they don't exist"""
    templates_dir = Path("templates")
    
    default_templates = {
        'startup.txt': """{timestamp} Server      Microsoft SQL Server {version}
{timestamp} Server      UTC adjustment: {timezone}
{timestamp} Server      (c) Microsoft Corporation.
{timestamp} Server      All rights reserved.
{timestamp} Server      Server process ID is {process_id}.
{timestamp} Server      Authentication mode is {auth_mode}.
{timestamp} Server      Logging SQL Server messages in file '{log_path}'.
{timestamp} Server      The service account is '{service_account}'. This is an informational message; no user action is required.
{timestamp} Server      SQL Server is now ready for client connections. This is an informational message; no user action is required.""",

        'login_failed.txt': """{timestamp} Logon        Error: {error_number}, Severity: {severity}, State: {state}.
{timestamp} Logon        Login failed for user '{username}'. Reason: {failure_reason} [CLIENT: {client_ip}]""",

        'deadlock.txt': """{timestamp} spid{spid}s     Deadlock encountered .... Printing deadlock information
{timestamp} spid{spid}s     Wait-for graph
{timestamp} spid{spid}s     Node:1
{timestamp} spid{spid}s     RID: {db_id}:1:{page_id}:0                 CleanCnt:2 Mode:X Flags: 0x3
{timestamp} spid{spid}s      Grant List 1:
{timestamp} spid{spid}s        Owner:0x{owner_id} Mode: X        Flg:0x40 Ref:1 Life:02000000 SPID:{victim_spid} ECID:0
{timestamp} spid{spid}s        SPID: {victim_spid} ECID: 0 Statement Type: UPDATE Line #: {line_number}
{timestamp} spid{spid}s        Input Buf: Language Event: {sql_statement}
{timestamp} spid{spid}s     Victim Resource Owner:
{timestamp} spid{spid}s      ResType:LockOwner Mode: U SPID:{victim_spid} Cost:(0/{cost})
{timestamp} spid{spid}s     deadlock victim=process{victim_spid}
{timestamp} spid{spid}s      ridlock fileid=1 pageid={page_id} dbid={db_id} objectname={database_name}.dbo.{table_name} mode=X""",

        'io_error.txt': """{timestamp} spid{spid}        Error: {error_number}, Severity: {severity}, State: {state}.
{timestamp} spid{spid}        {io_error_message}
{timestamp} spid{spid}        Possible failure reasons: Problems with the hardware, the device driver, or network-related issues.""",

        'timeout.txt': """{timestamp} spid{spid}        {timeout_message}
{timestamp} spid{spid}        The timeout period elapsed prior to completion of the operation or the server is not responding.""",

        'replication.txt': """{timestamp} spid{spid}        Error: {error_number}, Severity: {severity}, State: {state}.
{timestamp} spid{spid}        Replication-Replication Snapshot Subsystem: agent failed. The replication agent had encountered an exception.
{timestamp} spid{spid}        Publication: '{publication_name}' on server '{subscriber_name}'""",

        'availability_group.txt': """{timestamp} spid{spid}s       Always On: The local replica of availability group '{ag_name}' is starting.
{timestamp} spid{spid}s       The state of the local availability replica in availability group '{ag_name}' has changed to '{ag_state}'.
{timestamp} spid{spid}s       Always On Availability Groups connection established for replica ID: {{{replica_id}}}.""",

        'service_broker.txt': """{timestamp} Logon        Service Broker login attempt failed with error: 'A previously existing connection with the same peer was detected during connection handshake. This connection lost the arbitration and it will be closed. State 80.'.  [CLIENT: {client_ip}]""",

        'trace_events.txt': """{timestamp} spid{spid}        SQL Trace ID {spid} was started by login "{username}".
{timestamp} spid{spid}        DBCC TRACEON {error_number}, server process ID (SPID) {spid}. This is an informational message only; no user action is required.""",

        'maintenance.txt': """{timestamp} spid{spid}        CHECKDB for database '{database_name}' finished without errors. This is an informational message only; no user action is required.
{timestamp} spid{spid}        Parallel redo is started for database '{database_name}' with worker pool size [{worker_pool_size}]."""
    }
    
    for filename, content in default_templates.items():
        file_path = templates_dir / filename
        if not file_path.exists():
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

def create_default_sample_data():
    """Create default sample data files if they don't exist"""
    data_dir = Path("data")
    
    default_data = {
        'server_names.txt': [
            'TestServer', 'ProductionDB', 'DevServer', 'StagingSQL', 
            'BackupServer', 'ReportingDB', 'AnalyticsDB', 'WebServer'
        ],
        'database_names.txt': [
            'MainDB', 'UserDB', 'InventoryDB', 'OrdersDB', 'ReportsDB',
            'TempDB', 'LoggingDB', 'ArchiveDB', 'TestDB', 'DevDB'
        ],
        'user_names.txt': [
            'admin', 'sa', 'backup_user', 'report_user', 'app_service',
            'web_user', 'sync_agent', 'monitor_user', 'readonly_user'
        ],
        'ip_addresses.txt': [
            '192.168.1.100', '192.168.1.101', '192.168.1.102', '192.168.1.103',
            '10.0.0.50', '10.0.0.51', '10.0.0.52', '172.16.1.25', '172.16.1.26'
        ]
    }
    
    for filename, data_list in default_data.items():
        file_path = data_dir / filename
        if not file_path.exists():
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(data_list) + '\n')

def validate_config(config):
    """Validate configuration file"""
    required_keys = ['simulation', 'error_types', 'output']
    
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
    
    # Validate simulation settings
    sim_config = config['simulation']
    if sim_config.get('server_count', 0) <= 0:
        raise ValueError("server_count must be greater than 0")
    
    if sim_config.get('log_interval_seconds', 0) <= 0:
        raise ValueError("log_interval_seconds must be greater than 0")
    
    # Validate error types
    error_types = config['error_types']
    if not error_types:
        raise ValueError("At least one error type must be configured")
    
    # Check if at least one error type is enabled
    enabled_types = [k for k, v in error_types.items() if v.get('enabled', True)]
    if not enabled_types:
        raise ValueError("At least one error type must be enabled")
    
    return True