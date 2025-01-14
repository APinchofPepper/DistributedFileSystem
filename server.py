from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import sqlite3
from datetime import datetime, timedelta
import shutil
import threading
import time
import subprocess
import atexit
import signal
import zlib
import hashlib
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from functools import wraps
import ssl
import secrets
from werkzeug.utils import secure_filename
import logging
import logging.handlers
import json
import traceback
from enum import Enum
import xxhash

CHUNK_SIZE = 4 * 1024 * 1024
VERSION_SYNC_TIMEOUT = 30  # seconds
DB_TIMEOUT = 20.0
MAX_RETRIES = 3

ENCRYPTION_KEY_FILE = "encryption.key"
SALT_FILE = "salt.key"

app = Flask(__name__)
CORS(app)

node_heartbeats = {}
HEARTBEAT_THRESHOLD = 40
current_node_index = 0

# Define storage nodes and create their directories
UPLOAD_FOLDERS = {
    "Node1": "./storage_node1",
    "Node2": "./storage_node2",
    "Node3": "./storage_node3"
}

os.makedirs('logs', exist_ok=True)
os.makedirs(UPLOAD_FOLDERS["Node1"], exist_ok=True)
os.makedirs(UPLOAD_FOLDERS["Node2"], exist_ok=True)
os.makedirs(UPLOAD_FOLDERS["Node3"], exist_ok=True)

def manage_storage_tiers():
    """Periodically check and manage storage tiers."""
    while True:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Get all files and their current tiers
                cursor.execute('''
                    SELECT id, filename, storage_tier, last_accessed, access_count,
                           upload_timestamp, size
                    FROM metadata
                    WHERE is_archived = 0
                ''')
                files = cursor.fetchall()
                
                for file in files:
                    days_since_access = (
                        datetime.now() - 
                        datetime.fromisoformat(file['last_accessed'] or file['upload_timestamp'])
                    ).days
                    
                    # Get tier configuration
                    cursor.execute(
                        'SELECT * FROM storage_tiers WHERE tier_name = ?',
                        (file['storage_tier'],)
                    )
                    current_tier = cursor.fetchone()
                    
                    # Move to warm storage if not accessed in 30 days
                    if (file['storage_tier'] == 'hot' and 
                        days_since_access > 30 and 
                        file['access_count'] < 10):
                        move_to_storage_tier(file['id'], 'warm')
                    
                    # Move to cold storage if not accessed in 90 days
                    elif (file['storage_tier'] == 'warm' and 
                          days_since_access > 90):
                        move_to_storage_tier(file['id'], 'cold')
                    
                    # Consider for archival if in cold storage over 180 days
                    elif (file['storage_tier'] == 'cold' and 
                          days_since_access > 180):
                        archive_file(file['id'])
                
            time.sleep(3600)  # Check every hour
            
        except Exception as e:
            logger.error(f"Error in storage tier management: {str(e)}")
            time.sleep(300)  # Wait 5 minutes before retry

def move_to_storage_tier(file_id: int, new_tier: str):
    """Move a file to a different storage tier."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Update metadata
            cursor.execute('''
                UPDATE metadata 
                SET storage_tier = ?,
                    last_modified = ?
                WHERE id = ?
            ''', (new_tier, datetime.now().isoformat(), file_id))
            
            # Get compression level for new tier
            cursor.execute(
                'SELECT compression_level FROM storage_tiers WHERE tier_name = ?',
                (new_tier,)
            )
            compression_level = cursor.fetchone()['compression_level']
            
            # Update chunks with new compression if needed
            cursor.execute('''
                SELECT chunk_location, chunk_index
                FROM chunks
                WHERE file_id = ? AND status = 'active'
            ''', (file_id,))
            chunks = cursor.fetchall()
            
            for chunk in chunks:
                try:
                    # Read and recompress chunk with new level
                    with open(chunk['chunk_location'], 'rb') as f:
                        data = decrypt_and_decompress_data(f.read())
                    
                    compressed = encrypt_and_compress_data(
                        data,
                        compression_level=compression_level
                    )
                    
                    # Write back
                    with open(chunk['chunk_location'], 'wb') as f:
                        f.write(compressed)
                        
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk['chunk_index']}: {str(e)}")
                    
            logger.info(f"Moved file {file_id} to {new_tier} tier")
            
    except Exception as e:
        logger.error(f"Error moving file to new tier: {str(e)}")
        raise

def manage_deduplication():
    """Find and manage duplicate files."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Find files with same content hash
            cursor.execute('''
                SELECT content_hash, COUNT(*) as count, GROUP_CONCAT(id) as file_ids
                FROM metadata
                WHERE content_hash IS NOT NULL
                GROUP BY content_hash
                HAVING count > 1
            ''')
            
            duplicates = cursor.fetchall()
            
            for dup in duplicates:
                file_ids = dup['file_ids'].split(',')
                primary_id = file_ids[0]
                
                # Update deduplication references
                for secondary_id in file_ids[1:]:
                    cursor.execute('''
                        UPDATE metadata
                        SET deduplication_ref = ?
                        WHERE id = ?
                    ''', (primary_id, secondary_id))
                    
                    # Update deduplication stats
                    cursor.execute('''
                        UPDATE deduplication
                        SET reference_count = reference_count + 1,
                            total_space_saved = total_space_saved + (
                                SELECT size FROM metadata WHERE id = ?
                            ),
                            last_reference = ?
                        WHERE content_hash = ?
                    ''', (secondary_id, datetime.now().isoformat(), dup['content_hash']))
                    
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error in deduplication management: {str(e)}")

def apply_retention_policy(file_id: int):
    """Apply retention policy to a file's versions."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get file's retention policy
            cursor.execute('''
                SELECT m.retention_policy, r.*
                FROM metadata m
                JOIN retention_policies r ON m.retention_policy = r.policy_name
                WHERE m.id = ?
            ''', (file_id,))
            
            policy = cursor.fetchone()
            if not policy:
                return  # No policy defined
                
            # Get version history
            cursor.execute('''
                SELECT version_number, timestamp
                FROM versions
                WHERE file_id = ?
                ORDER BY version_number DESC
            ''', (file_id,))
            
            versions = cursor.fetchall()
            
            if len(versions) <= policy['min_versions']:
                return  # Keep all versions if under minimum
                
            # Keep required minimum
            versions_to_keep = set(range(len(versions) - policy['min_versions'], len(versions)))
            
            # Calculate versions to remove
            current_version = versions[0]['version_number']
            for i, version in enumerate(versions):
                if i in versions_to_keep:
                    continue
                    
                version_age = (
                    datetime.now() - 
                    datetime.fromisoformat(version['timestamp'])
                ).days
                
                # Keep if within retention period
                if version_age <= policy['retention_period_days']:
                    versions_to_keep.add(i)
                
                # Never delete current version
                if version['version_number'] == current_version:
                    versions_to_keep.add(i)
            
            # Delete versions not in keep set
            for i, version in enumerate(versions):
                if i not in versions_to_keep:
                    cursor.execute('''
                        UPDATE chunks
                        SET status = 'deprecated'
                        WHERE file_id = ? AND version_number = ?
                    ''', (file_id, version['version_number']))
            
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error applying retention policy: {str(e)}")

def archive_file(file_id: int):
    """Archive a file to cold storage."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get file metadata
            cursor.execute('''
                SELECT filename, size, compressed_size, storage_tier
                FROM metadata
                WHERE id = ? AND is_archived = 0
            ''', (file_id,))
            
            file = cursor.fetchone()
            if not file:
                return  # File not found or already archived
            
            # Create archive record
            archive_location = os.path.join(
                UPLOAD_FOLDERS['Node3'],  # Use Node3 for cold storage
                'archives',
                f"{file['filename']}.archive"
            )
            
            # Ensure archive directory exists
            os.makedirs(os.path.dirname(archive_location), exist_ok=True)
            
            # Gather all chunks
            cursor.execute('''
                SELECT chunk_location, chunk_index
                FROM chunks
                WHERE file_id = ? AND status = 'active'
                ORDER BY chunk_index
            ''', (file_id,))
            chunks = cursor.fetchall()
            
            # Create archive file
            with open(archive_location, 'wb') as archive:
                for chunk in chunks:
                    with open(chunk['chunk_location'], 'rb') as f:
                        archive.write(f.read())
            
            archive_size = os.path.getsize(archive_location)
            
            # Update database
            cursor.execute('''
                INSERT INTO archives (
                    file_id, archive_date, archive_location,
                    archive_size, archive_tier
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                file_id,
                datetime.now().isoformat(),
                archive_location,
                archive_size,
                'cold'
            ))
            
            # Update file status
            cursor.execute('''
                UPDATE metadata
                SET is_archived = 1,
                    archive_date = ?,
                    storage_tier = 'cold'
                WHERE id = ?
            ''', (datetime.now().isoformat(), file_id))
            
            # Mark chunks as archived
            cursor.execute('''
                UPDATE chunks
                SET status = 'archived'
                WHERE file_id = ? AND status = 'active'
            ''', (file_id,))
            
            # Remove original chunks to free space
            for chunk in chunks:
                try:
                    os.remove(chunk['chunk_location'])
                except OSError as e:
                    logger.warning(f"Could not remove chunk {chunk['chunk_index']}: {e}")
            
            conn.commit()
            logger.info(f"Successfully archived file {file_id}")
            
    except Exception as e:
        logger.error(f"Error archiving file: {str(e)}")
        raise

def restore_archived_file(file_id: int):
    """Restore an archived file to active storage."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get archive information
            cursor.execute('''
                SELECT a.archive_location, m.filename
                FROM archives a
                JOIN metadata m ON a.file_id = m.id
                WHERE a.file_id = ? AND m.is_archived = 1
            ''', (file_id,))
            
            archive = cursor.fetchone()
            if not archive:
                return  # Archive not found
            
            # Read archive and split into chunks
            with open(archive['archive_location'], 'rb') as f:
                archive_data = f.read()
            
            chunk_size = CHUNK_SIZE
            chunks = [
                archive_data[i:i + chunk_size]
                for i in range(0, len(archive_data), chunk_size)
            ]
            
            # Store chunks in hot storage
            new_chunks = []
            for i, chunk_data in enumerate(chunks):
                target_node = select_storage_node(len(chunk_data), UPLOAD_FOLDERS)
                chunk_path = os.path.join(
                    UPLOAD_FOLDERS[target_node],
                    f"{archive['filename']}_chunk_{i}"
                )
                
                with open(chunk_path, 'wb') as f:
                    f.write(chunk_data)
                
                new_chunks.append((i, chunk_path))
            
            # Update database
            cursor.execute('''
                UPDATE metadata
                SET is_archived = 0,
                    storage_tier = 'hot',
                    last_accessed = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), file_id))
            
            # Update chunk records
            for chunk_index, chunk_path in new_chunks:
                cursor.execute('''
                    INSERT INTO chunks (
                        file_id, version_number, chunk_index,
                        chunk_location, original_size,
                        compressed_size, chunk_hash, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    file_id,
                    1,  # Restored as version 1
                    chunk_index,
                    chunk_path,
                    len(chunk_data),
                    len(chunk_data),
                    calculate_hash(chunk_data),
                    'active'
                ))
            
            # Update archive record
            cursor.execute('''
                UPDATE archives
                SET restore_count = restore_count + 1,
                    last_restore_date = ?
                WHERE file_id = ?
            ''', (datetime.now().isoformat(), file_id))
            
            conn.commit()
            logger.info(f"Successfully restored file {file_id} from archive")
            
    except Exception as e:
        logger.error(f"Error restoring archived file: {str(e)}")
        raise

# Start background tasks for storage management
def start_storage_management():
    """Start background tasks for storage management."""
    # Storage tier management thread
    tier_thread = threading.Thread(
        target=manage_storage_tiers,
        daemon=True
    )
    tier_thread.start()
    
    # Deduplication management thread
    dedup_thread = threading.Thread(
        target=manage_deduplication,
        daemon=True
    )
    dedup_thread.start()

def setup_logger():
    """Configure the main system logger with proper formatting and rotation."""
    logger = logging.getLogger('dfs_logger')
    logger.setLevel(logging.DEBUG)

    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s'
    )

    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/system.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Error file handler
    error_handler = logging.handlers.RotatingFileHandler(
        'logs/error.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Performance log handler
    perf_handler = logging.handlers.RotatingFileHandler(
        'logs/performance.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    perf_handler.setLevel(logging.INFO)
    perf_handler.setFormatter(file_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    logger.addHandler(perf_handler)

    return logger

# Create logger instance
logger = setup_logger()

def log_operation(operation_type):
    """Decorator to log system operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            logger.info(f"Starting {operation_type}: {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                logger.info(f"Completed {operation_type}: {func.__name__}")
                return result
            except Exception as e:
                logger.error(
                    f"Failed {operation_type}: {func.__name__}\n"
                    f"Error: {str(e)}\n{traceback.format_exc()}"
                )
                raise
            finally:
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"{operation_type} duration: {duration:.2f} seconds")
                
        return wrapper
    return decorator

def log_performance(func):
    """Decorator to log function performance metrics."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = None
        error = None
        
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            error = e
            raise
        finally:
            end_time = time.time()
            duration = end_time - start_time
            
            log_data = {
                'function': func.__name__,
                'duration': duration,
                'timestamp': datetime.now().isoformat(),
                'success': error is None,
                'error': str(error) if error else None
            }
            
            # Log performance data
            logger.info(f"Performance: {json.dumps(log_data)}")
            
            # Log detailed error if any
            if error:
                logger.error(f"Error in {func.__name__}: {str(error)}\n{traceback.format_exc()}")

    return wrapper

def log_system_state(upload_folders):
    """Log current system state and storage usage."""
    try:
        usage_data = {}
        total_space = 0
        used_space = 0
        
        for node, folder in upload_folders.items():
            node_size = sum(
                os.path.getsize(os.path.join(folder, f))
                for f in os.listdir(folder)
                if os.path.isfile(os.path.join(folder, f))
            )
            usage_data[node] = {
                'used_space': node_size,
                'usage_percentage': (node_size / (500 * 1024 * 1024)) * 100  # 500MB limit
            }
            total_space += 500 * 1024 * 1024  # Add node capacity
            used_space += node_size
        
        system_state = {
            'timestamp': datetime.now().isoformat(),
            'total_space': total_space,
            'used_space': used_space,
            'usage_percentage': (used_space / total_space) * 100,
            'node_status': usage_data
        }
        
        logger.info(f"System State: {json.dumps(system_state)}")
    except Exception as e:
        logger.error(f"Error logging system state: {str(e)}\n{traceback.format_exc()}")

def require_admin(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('X-Admin-Key')
        if not auth_header or auth_header != "kv83nhjd9d9j3mxsidsis9432u3jsdkslos9":
            logger.warning(f"Unauthorized admin access attempt from {request.remote_addr}")
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/admin/nodes/health', methods=['GET'])
@require_admin
@log_operation('admin_node_health_check')
def get_nodes_health():
    """Get health status of all storage nodes."""
    try:
        now = datetime.now()
        node_status = {}
        
        for node, folder in UPLOAD_FOLDERS.items():
            # Check node heartbeat
            last_heartbeat = node_heartbeats.get(node, datetime.min)
            heartbeat_age = (now - last_heartbeat).total_seconds()
            
            # Check storage usage
            try:
                used_space = sum(
                    os.path.getsize(os.path.join(folder, f))
                    for f in os.listdir(folder)
                    if os.path.isfile(os.path.join(folder, f))
                )
                total_space = 500 * 1024 * 1024  # 500MB per node
                usage_percent = (used_space / total_space) * 100
            except Exception as e:
                logger.error(f"Error calculating storage for node {node}: {str(e)}")
                used_space = 0
                usage_percent = 0
            
            node_status[node] = {
                'status': 'active' if heartbeat_age < HEARTBEAT_THRESHOLD else 'inactive',
                'last_heartbeat': last_heartbeat.isoformat() if heartbeat_age < HEARTBEAT_THRESHOLD else None,
                'seconds_since_heartbeat': heartbeat_age,
                'storage': {
                    'used_bytes': used_space,
                    'total_bytes': total_space,
                    'usage_percent': usage_percent
                }
            }
        
        logger.info(f"Admin node health check completed")
        return jsonify({
            'timestamp': now.isoformat(),
            'nodes': node_status
        })
        
    except Exception as e:
        logger.error(f"Error in admin node health check: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/admin/files', methods=['GET'])
@require_admin
@log_operation('admin_list_files')
def list_all_files():
    """List all files in the system with their locations and metadata."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get all files with their metadata
            cursor.execute('''
                SELECT 
                    m.id,
                    m.filename,
                    m.current_version,
                    m.size,
                    m.compressed_size,
                    m.compression_ratio,
                    m.upload_timestamp,
                    m.location,
                    m.replicas,
                    COUNT(DISTINCT c.chunk_location) as chunk_count
                FROM metadata m
                LEFT JOIN chunks c ON m.id = c.file_id AND c.status = 'active'
                GROUP BY m.id
            ''')
            
            files = []
            for row in cursor.fetchall():
                # Get chunk locations for each file
                cursor.execute('''
                    SELECT DISTINCT chunk_location
                    FROM chunks
                    WHERE file_id = ? AND status = 'active'
                ''', (row['id'],))
                chunk_locations = [loc[0] for loc in cursor.fetchall()]
                
                files.append({
                    'id': row['id'],
                    'filename': row['filename'],
                    'current_version': row['current_version'],
                    'size': row['size'],
                    'compressed_size': row['compressed_size'],
                    'compression_ratio': row['compression_ratio'],
                    'upload_timestamp': row['upload_timestamp'],
                    'primary_location': row['location'],
                    'replicas': eval(row['replicas']) if row['replicas'] else [],
                    'chunk_count': row['chunk_count'],
                    'chunk_locations': chunk_locations
                })
        
        logger.info(f"Admin file listing completed. Found {len(files)} files.")
        return jsonify({'files': files})
        
    except Exception as e:
        logger.error(f"Error in admin file listing: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/admin/files/<int:file_id>/reallocate', methods=['POST'])
@require_admin
@log_operation('admin_reallocate_file')
@log_performance
def reallocate_file(file_id):
    """Force reallocation of a file's chunks to different nodes."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Verify file exists
            cursor.execute('SELECT filename FROM metadata WHERE id = ?', (file_id,))
            result = cursor.fetchone()
            if not result:
                return jsonify({'error': 'File not found'}), 404
                
            filename = result['filename']
            logger.info(f"Starting reallocation for file: {filename} (ID: {file_id})")
            
            # Get all chunks for the file
            cursor.execute('''
                SELECT id, chunk_index, chunk_location, chunk_hash
                FROM chunks
                WHERE file_id = ? AND status = 'active'
            ''', (file_id,))
            chunks = cursor.fetchall()
            
            reallocated_chunks = []
            failed_chunks = []
            
            for chunk in chunks:
                try:
                    # Read current chunk
                    with open(chunk['chunk_location'], 'rb') as f:
                        chunk_data = f.read()
                    
                    # Verify chunk integrity
                    if calculate_hash(chunk_data) != chunk['chunk_hash']:
                        raise ValueError(f"Chunk {chunk['chunk_index']} is corrupted")
                    
                    # Select new node
                    new_node = select_storage_node(len(chunk_data), UPLOAD_FOLDERS)
                    new_folder = UPLOAD_FOLDERS[new_node]
                    
                    # Create new chunk location
                    new_chunk_path = os.path.join(
                        new_folder,
                        f"{filename}_chunk_{chunk['chunk_index']}_new"
                    )
                    
                    # Write to new location
                    os.makedirs(os.path.dirname(new_chunk_path), exist_ok=True)
                    with open(new_chunk_path, 'wb') as f:
                        f.write(chunk_data)
                    
                    # Verify new chunk
                    with open(new_chunk_path, 'rb') as f:
                        if calculate_hash(f.read()) != chunk['chunk_hash']:
                            raise ValueError(f"New chunk verification failed")
                    
                    # Update database
                    cursor.execute('''
                        UPDATE chunks
                        SET chunk_location = ?, status = 'active'
                        WHERE id = ?
                    ''', (new_chunk_path, chunk['id']))
                    
                    # Create replicas
                    replica_locations = replicate_chunk(new_chunk_path, UPLOAD_FOLDERS)
                    
                    reallocated_chunks.append({
                        'chunk_index': chunk['chunk_index'],
                        'old_location': chunk['chunk_location'],
                        'new_location': new_chunk_path,
                        'replicas': replica_locations
                    })
                    
                    # Remove old chunk
                    try:
                        os.remove(chunk['chunk_location'])
                    except Exception as e:
                        logger.warning(f"Could not remove old chunk {chunk['chunk_location']}: {e}")
                    
                except Exception as e:
                    logger.error(f"Failed to reallocate chunk {chunk['chunk_index']}: {e}")
                    failed_chunks.append({
                        'chunk_index': chunk['chunk_index'],
                        'error': str(e)
                    })
            
            # Update metadata with new locations
            cursor.execute('''
                UPDATE metadata
                SET location = ?, replicas = ?
                WHERE id = ?
            ''', (new_node, str(list(set(n for c in reallocated_chunks for n in c['replicas']))), file_id))
            
            conn.commit()
            
            success_count = len(reallocated_chunks)
            fail_count = len(failed_chunks)
            logger.info(f"Reallocation completed. Success: {success_count}, Failed: {fail_count}")
            
            return jsonify({
                'message': f'Reallocation completed. {success_count} chunks succeeded, {fail_count} failed.',
                'reallocated_chunks': reallocated_chunks,
                'failed_chunks': failed_chunks
            })
            
    except Exception as e:
        logger.error(f"Error in file reallocation: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/admin/nodes/<node_name>/verify', methods=['POST'])
@require_admin
@log_operation('admin_verify_node')
@log_performance
def verify_node(node_name):
    """Verify integrity of all chunks on a specific node."""
    try:
        if node_name not in UPLOAD_FOLDERS:
            return jsonify({'error': 'Node not found'}), 404
            
        node_folder = UPLOAD_FOLDERS[node_name]
        verification_results = {
            'verified_chunks': [],
            'corrupted_chunks': [],
            'missing_chunks': []
        }
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get all chunks supposed to be on this node
            cursor.execute('''
                SELECT id, file_id, chunk_index, chunk_location, chunk_hash
                FROM chunks
                WHERE chunk_location LIKE ? AND status = 'active'
            ''', (f'%{node_name}%',))
            
            chunks = cursor.fetchall()
            
            for chunk in chunks:
                try:
                    if not os.path.exists(chunk['chunk_location']):
                        verification_results['missing_chunks'].append({
                            'chunk_id': chunk['id'],
                            'file_id': chunk['file_id'],
                            'location': chunk['chunk_location']
                        })
                        continue
                        
                    with open(chunk['chunk_location'], 'rb') as f:
                        current_hash = calculate_hash(f.read())
                        
                    if current_hash != chunk['chunk_hash']:
                        verification_results['corrupted_chunks'].append({
                            'chunk_id': chunk['id'],
                            'file_id': chunk['file_id'],
                            'location': chunk['chunk_location'],
                            'expected_hash': chunk['chunk_hash'],
                            'actual_hash': current_hash
                        })
                    else:
                        verification_results['verified_chunks'].append({
                            'chunk_id': chunk['id'],
                            'file_id': chunk['file_id'],
                            'location': chunk['chunk_location']
                        })
                        
                except Exception as e:
                    logger.error(f"Error verifying chunk {chunk['id']}: {e}")
                    verification_results['corrupted_chunks'].append({
                        'chunk_id': chunk['id'],
                        'file_id': chunk['file_id'],
                        'location': chunk['chunk_location'],
                        'error': str(e)
                    })
        
        total_chunks = len(chunks)
        verified_count = len(verification_results['verified_chunks'])
        corrupted_count = len(verification_results['corrupted_chunks'])
        missing_count = len(verification_results['missing_chunks'])
        
        logger.info(f"Node verification completed for {node_name}. "
                   f"Total: {total_chunks}, Verified: {verified_count}, "
                   f"Corrupted: {corrupted_count}, Missing: {missing_count}")
        
        return jsonify({
            'node_name': node_name,
            'total_chunks': total_chunks,
            'results': verification_results,
            'summary': {
                'verified': verified_count,
                'corrupted': corrupted_count,
                'missing': missing_count
            }
        })
        
    except Exception as e:
        logger.error(f"Error in node verification: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# Node health logging
def log_node_health(node_heartbeats):
    """Log health status of all nodes."""
    try:
        now = datetime.now()
        node_status = {}
        
        for node, last_heartbeat in node_heartbeats.items():
            time_since_heartbeat = (now - last_heartbeat).total_seconds()
            node_status[node] = {
                'last_heartbeat': last_heartbeat.isoformat(),
                'seconds_since_heartbeat': time_since_heartbeat,
                'status': 'active' if time_since_heartbeat < 40 else 'inactive'
            }
        
        logger.info(f"Node Health Status: {json.dumps(node_status)}")
    except Exception as e:
        logger.error(f"Error logging node health: {str(e)}\n{traceback.format_exc()}")

@contextmanager
def get_db_connection():
    """Context manager for database connections with retry logic."""
    conn = None
    try:
        retries = 0
        while retries < MAX_RETRIES:
            try:
                conn = sqlite3.connect('metadata.db', timeout=DB_TIMEOUT)
                conn.row_factory = sqlite3.Row
                yield conn
                conn.commit()  # Commit if no exception occurred
                break
            except sqlite3.OperationalError as e:
                retries += 1
                if retries == MAX_RETRIES:
                    raise
                time.sleep(1)  # Wait before retrying
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback on error
        raise
    finally:
        if conn:
            conn.close()  # Always close the connection

def calculate_hash(data: bytes) -> str:
    """Calculate SHA-256 hash of data using hashlib.
    
    Args:
        data (bytes): The data to hash
        
    Returns:
        str: The hexadecimal representation of the SHA-256 hash
    """
    hash_obj = hashlib.sha256()
    # For large files, update the hash in chunks
    if isinstance(data, bytes):
        hash_obj.update(data)
    else:
        # If data is a file-like object, read and update in chunks
        chunk = data.read(4096)  # 4KB chunks
        while chunk:
            hash_obj.update(chunk)
            chunk = data.read(4096)
            
    return hash_obj.hexdigest()

def ensure_all_nodes_synced(file_id: int, version: int, nodes: List[str]) -> bool:
    """Ensure all nodes have synced a specific version of a file."""
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()
    
    try:
        # Check consistency status for all nodes
        cursor.execute('''
            SELECT node_name, status
            FROM consistency_status
            WHERE file_id = ? AND version_number = ?
        ''', (file_id, version))
        
        statuses = cursor.fetchall()
        all_synced = all(status == 'synced' for _, status in statuses)
        synced_nodes = {node for node, status in statuses if status == 'synced'}
        
        # Verify all required nodes are present and synced
        return all_synced and set(nodes) == synced_nodes
    
    finally:
        conn.close()

def update_consistency_status(file_id: int, version: int, node: str, status: str):
    """Update the consistency status for a node."""
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO consistency_status
            (file_id, version_number, node_name, status, last_update)
            VALUES (?, ?, ?, ?, ?)
        ''', (file_id, version, node, status, datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()

def sync_version_to_node(file_id: int, version: int, node: str, chunk_locations: List[str]) -> bool:
    """Synchronize a specific version of chunks to a node."""
    try:
        # Copy chunks to the node and verify
        for chunk_location in chunk_locations:
            target_path = os.path.join(UPLOAD_FOLDERS[node], os.path.basename(chunk_location))
            
            if not os.path.exists(target_path):
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.copy2(chunk_location, target_path)
            
            # Verify the copy
            if not os.path.exists(target_path):
                return False
                
            # Verify chunk integrity
            with open(chunk_location, 'rb') as f:
                original_hash = calculate_hash(f.read())
            with open(target_path, 'rb') as f:
                copied_hash = calculate_hash(f.read())
                
            if original_hash != copied_hash:
                return False
        
        return True
    except Exception as e:
        print(f"Error syncing to node {node}: {e}")
        return False

def ensure_version_consistency(file_id: int, version: int, nodes: List[str], chunk_locations: List[str]) -> bool:
    """Ensure a specific version is consistent across all nodes."""
    with ThreadPoolExecutor() as executor:
        # Create sync tasks for each node
        future_to_node = {
            executor.submit(sync_version_to_node, file_id, version, node, chunk_locations): node
            for node in nodes
        }
        
        # Wait for all syncs to complete
        for future in as_completed(future_to_node):
            node = future_to_node[future]
            try:
                success = future.result()
                status = 'synced' if success else 'failed'
                update_consistency_status(file_id, version, node, status)
                
                if not success:
                    print(f"Failed to sync version {version} to node {node}")
                    return False
            except Exception as e:
                print(f"Error syncing to node {node}: {e}")
                update_consistency_status(file_id, version, node, 'failed')
                return False
    
    return True

def redistribute_chunks(failed_node):
    print(f"Redistributing chunks for failed node: {failed_node}")
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()

    # Find all chunks stored on the failed node
    cursor.execute('''
        SELECT id, chunk_index, file_id, chunk_location
        FROM chunks
        WHERE chunk_location LIKE ?
    ''', (f'%{failed_node}%',))
    chunks = cursor.fetchall()

    for chunk_id, chunk_index, file_id, chunk_location in chunks:
        # Try to find a healthy node to store the chunk
        for target_node, target_path in UPLOAD_FOLDERS.items():
            if target_node != failed_node:
                try:
                    # Move the chunk to the healthy node
                    new_chunk_path = os.path.join(target_path, os.path.basename(chunk_location))
                    os.makedirs(target_path, exist_ok=True)

                    # Copy the chunk file to the new node
                    if not os.path.exists(new_chunk_path):
                        shutil.copy(chunk_location, new_chunk_path)
                    print(f"Moved chunk {chunk_index} (file ID: {file_id}) from {chunk_location} to {new_chunk_path}")

                    # Update the database with the new chunk location
                    cursor.execute('''
                        UPDATE chunks
                        SET chunk_location = ?
                        WHERE id = ?
                    ''', (new_chunk_path, chunk_id))

                    break  # Move on to the next chunk after successful redistribution
                except Exception as e:
                    print(f"Failed to redistribute chunk {chunk_index} from {chunk_location}: {e}")
                    continue

    conn.commit()
    conn.close()
    print(f"Finished redistributing chunks for failed node: {failed_node}")

def replicate_chunk(chunk_path, upload_folders):
    """Replicate a chunk to different nodes based on load balancing."""
    replica_locations = []
    chunk_size = os.path.getsize(chunk_path)
    
    # Determine original node
    original_node = None
    for node, folder in upload_folders.items():
        if chunk_path.startswith(folder):
            original_node = node
            break
    
    excluded_nodes = {original_node} if original_node else set()
    
    # Create two replicas on different nodes
    for _ in range(2):
        try:
            # Select target node based on load balancing
            replica_node = select_storage_node(
                chunk_size, 
                upload_folders,
                excluded_nodes
            )
            
            replica_folder = upload_folders[replica_node]
            replica_path = os.path.join(
                replica_folder, 
                os.path.basename(chunk_path)
            )
            
            # Skip if replica already exists
            if os.path.exists(replica_path):
                print(f"Replica already exists at {replica_path}")
                continue
                
            # Create replica
            os.makedirs(replica_folder, exist_ok=True)
            shutil.copy(chunk_path, replica_path)
            replica_locations.append(replica_path)
            
            # Exclude this node from future replicas
            excluded_nodes.add(replica_node)
            
        except Exception as e:
            print(f"Failed to create replica: {e}")
            continue
            
    return replica_locations

def compress_data(data: bytes) -> bytes:
    """Compress data using zlib."""
    return zlib.compress(data)

def decompress_data(data: bytes) -> bytes:
    """Decompress data using zlib."""
    return zlib.decompress(data)

def generate_key():
    """Generate a new encryption key and salt."""
    try:
        # Generate a random salt
        salt = secrets.token_bytes(16)
        
        # Generate key using PBKDF2
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(b"your-secret-password"))
        
        # Save the key and salt
        with open(ENCRYPTION_KEY_FILE, "wb") as key_file:
            key_file.write(key)
        with open(SALT_FILE, "wb") as salt_file:
            salt_file.write(salt)
            
        return key
    except Exception as e:
        print(f"Error generating key: {e}")
        raise

def get_encryption_key():
    """Get or generate the encryption key."""
    try:
        with open(ENCRYPTION_KEY_FILE, "rb") as key_file:
            return key_file.read()
    except FileNotFoundError:
        return generate_key()

class EncryptionManager:
    def __init__(self):
        self.key = get_encryption_key()
        self.fernet = Fernet(self.key)
    
    def encrypt_data(self, data: bytes) -> bytes:
        """Encrypt data using Fernet."""
        return self.fernet.encrypt(data)
    
    def decrypt_data(self, encrypted_data: bytes) -> bytes:
        """Decrypt data using Fernet."""
        return self.fernet.decrypt(encrypted_data)

# Create a global encryption manager
encryption_manager = EncryptionManager()

def encrypt_and_compress_data(data: bytes) -> bytes:
    """First compress and then encrypt data."""
    try:
        # First compress
        compressed_data = compress_data(data)
        # Then encrypt
        return encryption_manager.encrypt_data(compressed_data)
    except Exception as e:
        print(f"Error in encrypt_and_compress_data: {e}")
        raise

def decrypt_and_decompress_data(data: bytes) -> bytes:
    """First decrypt and then decompress data."""
    try:
        # First decrypt
        decrypted_data = encryption_manager.decrypt_data(data)
        # Then decompress
        return decompress_data(decrypted_data)
    except Exception as e:
        print(f"Error in decrypt_and_decompress_data: {e}")
        raise

def split_file_into_chunks(filepath, upload_folders):
    """Split a file into chunks, compress, encrypt, and distribute based on load balancing."""
    chunks = []
    
    with open(filepath, 'rb') as f:
        chunk_index = 0
        while chunk := f.read(CHUNK_SIZE):
            try:
                # Process the chunk (compress and encrypt)
                processed_chunk = encrypt_and_compress_data(chunk)
                chunk_size = len(processed_chunk)
                
                # Select target node based on available space and current load
                target_node = select_storage_node(chunk_size, upload_folders)
                target_folder = upload_folders[target_node]
                
                # Construct chunk path and save processed data
                chunk_filename = f"{os.path.basename(filepath)}_chunk_{chunk_index}"
                chunk_path = os.path.join(target_folder, chunk_filename)
                
                os.makedirs(target_folder, exist_ok=True)
                with open(chunk_path, 'wb') as chunk_file:
                    chunk_file.write(processed_chunk)
                
                chunks.append((chunk_index, chunk_path))
                chunk_index += 1
                
            except Exception as e:
                print(f"Error processing chunk {chunk_index}: {e}")
                raise
    
    return chunks

def assemble_chunks(filename, output_folder):
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()
    
    # Get file ID and chunk details
    cursor.execute('''
        SELECT id FROM metadata WHERE filename = ?
    ''', (filename,))
    file_id = cursor.fetchone()[0]

    cursor.execute('''
        SELECT chunk_index, chunk_location FROM chunks WHERE file_id = ?
        ORDER BY chunk_index
    ''', (file_id,))
    chunks = cursor.fetchall()
    conn.close()

    # Reassemble the file with decompression
    output_path = os.path.join(output_folder, filename)
    with open(output_path, 'wb') as output_file:
        for _, chunk_location in chunks:
            with open(chunk_location, 'rb') as chunk_file:
                compressed_data = chunk_file.read()
                decompressed_data = decompress_data(compressed_data)
                output_file.write(decompressed_data)
    return output_path

def get_node_usage(upload_folders, total_capacity=500 * 1024 * 1024):  # 500 MB per node
    """Calculate available space and usage percentage for each node."""
    usage = {}
    for node, folder in upload_folders.items():
        try:
            used_space = sum(
                os.path.getsize(os.path.join(folder, f)) 
                for f in os.listdir(folder) 
                if os.path.isfile(os.path.join(folder, f))
            )
            available_space = total_capacity - used_space
            usage_percentage = (used_space / total_capacity) * 100
            usage[node] = {
                'available_space': available_space,
                'usage_percentage': usage_percentage
            }
        except Exception as e:
            print(f"Error calculating usage for {node}: {e}")
            usage[node] = {
                'available_space': 0,
                'usage_percentage': 100  # Assume full if error
            }
    return usage

def select_storage_node(chunk_size, upload_folders, excluded_nodes=None):
    """Select the best node for storing a chunk based on available space and load."""
    if excluded_nodes is None:
        excluded_nodes = set()
    
    usage = get_node_usage(upload_folders)
    available_nodes = {
        node: stats for node, stats in usage.items() 
        if node not in excluded_nodes and stats['available_space'] >= chunk_size
    }
    
    if not available_nodes:
        raise Exception("No suitable nodes available for storage")
    
    # Select node with lowest usage percentage
    return min(
        available_nodes.items(),
        key=lambda x: x[1]['usage_percentage']
    )[0]

def get_least_used_node(upload_folders):
    """Get the node with the lowest usage percentage."""
    usage = get_node_usage(upload_folders)
    # Return the node with minimum usage percentage
    return min(
        usage.items(),
        key=lambda x: x[1]['usage_percentage']
    )[0]

def assemble_chunks(filename, output_folder):
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()
    
    # Get file ID and chunk details
    cursor.execute('''
        SELECT id FROM metadata WHERE filename = ?
    ''', (filename,))
    file_id = cursor.fetchone()[0]

    cursor.execute('''
        SELECT chunk_index, chunk_location FROM chunks WHERE file_id = ?
        ORDER BY chunk_index
    ''', (file_id,))
    chunks = cursor.fetchall()
    conn.close()

    # Reassemble the file
    output_path = os.path.join(output_folder, filename)
    with open(output_path, 'wb') as output_file:
        for _, chunk_location in chunks:
            with open(chunk_location, 'rb') as chunk_file:
                output_file.write(chunk_file.read())
    return output_path

def save_metadata(filename, size, compressed_size, location, replicas):
    """Save file metadata including compression information."""
    compression_ratio = (size - compressed_size) / size * 100 if size > 0 else 0
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO metadata (
                filename, 
                size, 
                compressed_size,
                compression_ratio,
                upload_timestamp, 
                location, 
                replicas
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            filename, 
            size, 
            compressed_size,
            compression_ratio,
            datetime.now().isoformat(), 
            location, 
            replicas
        ))
        conn.commit()

def get_metadata(filename):
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM metadata WHERE filename = ?', (filename,))
    metadata = cursor.fetchone()
    
    conn.close()
    return metadata

@app.route('/upload', methods=['POST'])
@log_operation('file_upload')
@log_performance
def upload_file():
    """Handle file upload with simplified file handling to avoid generator issues."""
    logger.info(f"Received upload request")
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    uploaded_file = request.files['file']
    if not uploaded_file:
        return jsonify({"error": "Empty file provided"}), 400

    # Create temporary directory if it doesn't exist
    temp_dir = os.path.join(os.getcwd(), 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    
    temp_path = os.path.join(temp_dir, secure_filename(uploaded_file.filename))
    chunks = []

    try:
        # Save the file first
        uploaded_file.save(temp_path)
        original_size = os.path.getsize(temp_path)

        # Calculate file hash
        with open(temp_path, 'rb') as f:
            file_hash = calculate_hash(f.read())

        # Select initial storage node
        initial_node = select_storage_node(original_size, UPLOAD_FOLDERS)
        total_compressed_size = 0
        
        # Process the file in chunks
        with open(temp_path, 'rb') as f:
            chunk_index = 0
            while True:
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    break

                # Process the chunk
                processed_chunk = encrypt_and_compress_data(chunk_data)
                chunk_size = len(processed_chunk)
                total_compressed_size += chunk_size

                # Select storage node for this chunk
                target_node = select_storage_node(chunk_size, UPLOAD_FOLDERS)
                target_folder = UPLOAD_FOLDERS[target_node]

                # Save the chunk
                chunk_filename = f"{secure_filename(uploaded_file.filename)}_chunk_{chunk_index}"
                chunk_path = os.path.join(target_folder, chunk_filename)
                
                os.makedirs(os.path.dirname(chunk_path), exist_ok=True)
                with open(chunk_path, 'wb') as chunk_file:
                    chunk_file.write(processed_chunk)
                
                chunks.append((chunk_index, chunk_path))
                chunk_index += 1

        # Database operations
        with sqlite3.connect('metadata.db', timeout=DB_TIMEOUT) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Check for existing file
            cursor.execute(
                'SELECT id, current_version FROM metadata WHERE filename = ?', 
                (uploaded_file.filename,)
            )
            existing_file = cursor.fetchone()
            
            file_id = None
            current_version = 0
            
            if existing_file:
                file_id = existing_file['id']
                current_version = existing_file['current_version']
                new_version = current_version + 1
            else:
                new_version = 1

            # Calculate compression ratio
            compression_ratio = (
                (original_size - total_compressed_size) / original_size * 100
            ) if original_size > 0 else 0

            # Insert/update metadata
            if not file_id:
                cursor.execute('''
                    INSERT INTO metadata (
                        filename, current_version, size, compressed_size,
                        compression_ratio, upload_timestamp, location, replicas
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    uploaded_file.filename, new_version, original_size,
                    total_compressed_size, compression_ratio,
                    datetime.now().isoformat(), initial_node, ""
                ))
                file_id = cursor.lastrowid
            else:
                cursor.execute('''
                    UPDATE metadata 
                    SET current_version = ?, size = ?, compressed_size = ?,
                        compression_ratio = ?, upload_timestamp = ?, location = ?
                    WHERE id = ?
                ''', (
                    new_version, original_size, total_compressed_size,
                    compression_ratio, datetime.now().isoformat(),
                    initial_node, file_id
                ))

            # Add version information
            cursor.execute('''
                INSERT INTO versions (
                    file_id, version_number, timestamp, size,
                    compressed_size, hash
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                file_id, new_version, datetime.now().isoformat(),
                original_size, total_compressed_size, file_hash
            ))

            # Store chunks and create replicas
            all_locations = set()
            for chunk_index, chunk_path in chunks:
                # Get chunk hash and size
                with open(chunk_path, 'rb') as f:
                    chunk_data = f.read()
                    chunk_hash = calculate_hash(chunk_data)
                    chunk_size = len(chunk_data)

                cursor.execute('''
                    INSERT INTO chunks (
                        file_id, version_number, chunk_index, chunk_location,
                        original_size, compressed_size, chunk_hash, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    file_id, new_version, chunk_index, chunk_path,
                    CHUNK_SIZE, chunk_size, chunk_hash, 'active'
                ))
                
                all_locations.add(os.path.dirname(chunk_path))
                
                # Create replicas
                replica_locations = replicate_chunk(chunk_path, UPLOAD_FOLDERS)
                for replica_path in replica_locations:
                    cursor.execute('''
                        INSERT INTO chunks (
                            file_id, version_number, chunk_index, chunk_location,
                            original_size, compressed_size, chunk_hash, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        file_id, new_version, chunk_index, replica_path,
                        CHUNK_SIZE, chunk_size, chunk_hash, 'active'
                    ))
                    all_locations.add(os.path.dirname(replica_path))

            # Update storage locations
            storage_nodes = [
                node for node in UPLOAD_FOLDERS.keys() 
                if UPLOAD_FOLDERS[node] in all_locations
            ]
            
            cursor.execute('''
                UPDATE metadata 
                SET replicas = ?
                WHERE id = ?
            ''', (str(storage_nodes), file_id))

            # Record version change
            cursor.execute('''
                INSERT INTO version_changes (
                    file_id, old_version, new_version, change_type, timestamp
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                file_id, current_version, new_version,
                'update' if existing_file else 'create',
                datetime.now().isoformat()
            ))

            conn.commit()

            return jsonify({
                "message": "File uploaded and replicated successfully",
                "filename": uploaded_file.filename,
                "version": new_version,
                "original_size": original_size,
                "compressed_size": total_compressed_size,
                "compression_ratio": compression_ratio,
                "storage_nodes": storage_nodes
            }), 200

    except Exception as e:
        # Clean up chunks on error
        for _, path in chunks:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except:
                pass
        return jsonify({"error": str(e)}), 500

    finally:
        # Clean up temporary file
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception as e:
            print(f"Error cleaning up temporary file: {e}")

@app.route('/versions/<filename>', methods=['GET'])
def get_file_versions(filename):
    """Get version history for a file."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    v.version_number,
                    v.timestamp,
                    v.size,
                    v.compressed_size,
                    v.hash,
                    m.current_version
                FROM metadata m
                JOIN versions v ON m.id = v.file_id
                WHERE m.filename = ?
                ORDER BY v.version_number DESC
            ''', (filename,))
            
            versions = cursor.fetchall()
            if not versions:
                return jsonify({"error": "File not found"}), 404
            
            # Convert Row objects to dictionaries
            version_list = []
            for row in versions:
                version_list.append({
                    "version": row['version_number'],
                    "timestamp": row['timestamp'],
                    "size": row['size'],
                    "compressed_size": row['compressed_size'],
                    "hash": row['hash'],
                    "is_current": row['version_number'] == row['current_version']
                })
            
            return jsonify({
                "filename": filename,
                "current_version": versions[0]['current_version'],
                "versions": version_list
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/admin/storage/tiers', methods=['GET'])
@require_admin
def get_storage_tiers():
    """Get current storage tier statistics."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Modify query to handle potential NULL values and ensure proper grouping
            cursor.execute('''
                SELECT 
                    COALESCE(storage_tier, 'hot') as tier_name,
                    COUNT(*) as file_count,
                    COALESCE(SUM(size), 0) as total_size,
                    COALESCE(SUM(compressed_size), 0) as total_compressed_size
                FROM metadata
                WHERE is_archived = 0 OR is_archived IS NULL
                GROUP BY storage_tier
            ''')
            
            rows = cursor.fetchall()
            tiers = []
            for row in rows:
                tiers.append({
                    'name': row['tier_name'],
                    'file_count': row['file_count'],
                    'total_size': row['total_size'],
                    'total_compressed_size': row['total_compressed_size']
                })
            
            return jsonify({'tiers': tiers})
            
    except Exception as e:
        logger.error(f"Error getting storage tier stats: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/admin/storage/deduplication', methods=['GET'])
@require_admin
def get_deduplication_stats():
    """Get deduplication statistics."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) as total_refs,
                       SUM(total_space_saved) as space_saved
                FROM deduplication
            ''')
            
            stats = cursor.fetchone()
            return jsonify({
                'total_references': stats['total_refs'],
                'total_space_saved': stats['space_saved']
            })
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/rollback/<filename>/<int:version>', methods=['POST'])
def rollback_version(filename, version):
    """Roll back a file to a specific version."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check if version exists
            cursor.execute('''
                SELECT m.id, m.current_version, v.version_number
                FROM metadata m
                JOIN versions v ON m.id = v.file_id
                WHERE m.filename = ? AND v.version_number = ?
            ''', (filename, version))
            
            result = cursor.fetchone()
            if not result:
                return jsonify({"error": "Version not found"}), 404
                
            file_id = result['id']
            current_version = result['current_version']
            
            if version == current_version:
                return jsonify({"message": "Already at requested version"}), 200
            
            # Mark current version's chunks as deprecated
            cursor.execute('''
                UPDATE chunks
                SET status = 'deprecated'
                WHERE file_id = ? AND version_number = ? AND status = 'active'
            ''', (file_id, current_version))
            
            # Reactivate old version's chunks
            cursor.execute('''
                UPDATE chunks
                SET status = 'active'
                WHERE file_id = ? AND version_number = ? AND status = 'deprecated'
            ''', (file_id, version))
            
            # Update current version in metadata
            cursor.execute('''
                UPDATE metadata
                SET current_version = ?
                WHERE id = ?
            ''', (version, file_id))
            
            conn.commit()
            
            return jsonify({
                "message": f"Successfully rolled back {filename} to version {version}",
                "previous_version": current_version,
                "current_version": version
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/diff/<filename>', methods=['GET'])
def compare_versions(filename):
    """Compare two versions of a file."""
    version1 = request.args.get('v1', type=int)
    version2 = request.args.get('v2', type=int)
    
    if not version1 or not version2:
        return jsonify({"error": "Must specify two versions to compare"}), 400
        
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get file metadata and verify versions exist
            cursor.execute('''
                SELECT m.id, v.version_number, v.timestamp, v.size, v.hash
                FROM metadata m
                JOIN versions v ON m.id = v.file_id
                WHERE m.filename = ? AND v.version_number IN (?, ?)
                ORDER BY v.version_number
            ''', (filename, version1, version2))
            
            versions = cursor.fetchall()
            if len(versions) != 2:
                return jsonify({"error": "One or both versions not found"}), 404
            
            # Compare version metadata
            v1, v2 = versions[0], versions[1]
            comparison = {
                "filename": filename,
                "version1": {
                    "number": v1['version_number'],
                    "timestamp": v1['timestamp'],
                    "size": v1['size'],
                    "hash": v1['hash']
                },
                "version2": {
                    "number": v2['version_number'],
                    "timestamp": v2['timestamp'],
                    "size": v2['size'],
                    "hash": v2['hash']
                },
                "differences": {
                    "size_change": v2['size'] - v1['size'],
                    "is_identical": v1['hash'] == v2['hash'],
                    "time_between": (
                        datetime.fromisoformat(v2['timestamp']) - 
                        datetime.fromisoformat(v1['timestamp'])
                    ).total_seconds()
                }
            }
            
            return jsonify(comparison)
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Enhance upload route to track version changes
def track_version_change(file_id: int, old_version: int, new_version: int, 
                        change_type: str = "update"):
    """Track changes between versions."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        timestamp = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO version_changes (
                file_id, 
                old_version, 
                new_version, 
                change_type, 
                timestamp
            ) VALUES (?, ?, ?, ?, ?)
        ''', (file_id, old_version, new_version, change_type, timestamp))
        
        conn.commit()

@app.route('/admin/files/<int:file_id>/archive', methods=['POST'])
@require_admin
@log_operation('admin_archive_file')
def admin_archive_file(file_id):
    """Manually trigger archiving for a file."""
    try:
        archive_file(file_id)
        return jsonify({'message': f'File {file_id} archived successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/admin/files/<int:file_id>/restore', methods=['POST'])
@require_admin
@log_operation('admin_restore_file')
def admin_restore_file(file_id):
    """Manually trigger restoration of an archived file."""
    try:
        restore_archived_file(file_id)
        return jsonify({'message': f'File {file_id} restored successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/download/<filename>', methods=['GET'])
@log_operation('file_download')
@log_performance
def download_file(filename):
    """Download a file with optional version specification."""
    logger.info(f"Received download request")
    version = request.args.get('version', type=int)
    temp_path = None
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get file metadata and version info
            if version:
                cursor.execute('''
                    SELECT m.id, v.version_number
                    FROM metadata m
                    JOIN versions v ON m.id = v.file_id
                    WHERE m.filename = ? AND v.version_number = ?
                ''', (filename, version))
            else:
                cursor.execute('''
                    SELECT id, current_version
                    FROM metadata
                    WHERE filename = ?
                ''', (filename,))
                
            result = cursor.fetchone()
            if not result:
                return jsonify({"error": "File not found"}), 404
                
            file_id, version_number = result['id'], result['current_version']
            
            # Get unique chunks
            cursor.execute('''
                SELECT DISTINCT chunk_index, chunk_location, chunk_hash
                FROM chunks
                WHERE file_id = ? AND version_number = ? AND status = 'active'
                GROUP BY chunk_index
                ORDER BY chunk_index
            ''', (file_id, version_number))
            chunks = cursor.fetchall()

            if not chunks:
                return jsonify({"error": "No chunks found for the file"}), 404

        # Create a temporary directory for reassembly
        temp_dir = os.path.join(os.getcwd(), 'temp_downloads')
        os.makedirs(temp_dir, exist_ok=True)
        temp_path = os.path.join(temp_dir, f"tmp_{filename}")
        
        # Reassemble file
        with open(temp_path, 'wb') as output_file:
            for chunk in chunks:
                chunk_index = chunk['chunk_index']
                chunk_location = chunk['chunk_location']
                expected_hash = chunk['chunk_hash']
                
                # Try to get a valid chunk
                chunk_data = None
                valid_chunk_found = False
                
                if os.path.exists(chunk_location):
                    with open(chunk_location, 'rb') as f:
                        chunk_data = f.read()
                        if calculate_hash(chunk_data) == expected_hash:
                            valid_chunk_found = True
                
                if not valid_chunk_found:
                    chunk_filename = os.path.basename(chunk_location)
                    for node_folder in UPLOAD_FOLDERS.values():
                        replica_path = os.path.join(node_folder, chunk_filename)
                        if os.path.exists(replica_path):
                            with open(replica_path, 'rb') as f:
                                chunk_data = f.read()
                                if calculate_hash(chunk_data) == expected_hash:
                                    valid_chunk_found = True
                                    break
                
                if not valid_chunk_found:
                    raise Exception(f"Chunk {chunk_index} is corrupted or missing")
                
                # Decrypt and write chunk
                decrypted_data = decrypt_and_decompress_data(chunk_data)
                output_file.write(decrypted_data)

        # Send file with cleanup in a finally block
        try:
            return send_file(
                temp_path,
                as_attachment=True,
                download_name=filename
            )
        finally:
            # Schedule cleanup using a background thread
            def delayed_cleanup():
                time.sleep(1)  # Give a small delay to ensure file is sent
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception as e:
                    print(f"Error cleaning up temp file {temp_path}: {e}")

            cleanup_thread = threading.Thread(target=delayed_cleanup)
            cleanup_thread.daemon = True
            cleanup_thread.start()

    except Exception as e:
        # Clean up on error
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return jsonify({"error": str(e)}), 500

# HTTPS Configuration
def create_ssl_context():
    """Create SSL context for HTTPS."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        context.load_cert_chain('cert.pem', 'key.pem')
        return context
    except FileNotFoundError:
        print("SSL certificates not found. Generating self-signed certificates...")
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from datetime import datetime, timedelta, timezone
        
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        
        # Generate certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u"localhost")
        ])
        
        # Use timezone-aware datetime objects
        now = datetime.now(timezone.utc)
        
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            now
        ).not_valid_after(
            now + timedelta(days=365)
        ).sign(private_key, hashes.SHA256())
        
        # Save private key and certificate
        with open("key.pem", "wb") as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ))
            
        with open("cert.pem", "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
            
        context.load_cert_chain('cert.pem', 'key.pem')
        return context

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.json
    node_name = data.get('node_name')

    if not node_name:
        return jsonify({"error": "Node name is required"}), 400

    # Check if the node was previously inactive
    if node_name not in node_heartbeats:
        print(f"Recovered node: {node_name}")
    
    # Update the last heartbeat timestamp for the node
    node_heartbeats[node_name] = datetime.now()
    print(f"Heartbeat received from {node_name} at {node_heartbeats[node_name]}")

    return jsonify({"message": f"Heartbeat received from {node_name}"}), 200

@log_operation('node_monitoring')
def monitor_nodes():
    """Periodically check for inactive nodes."""
    logger.info("Starting node monitoring service")
    
    while True:
        try:
            now = datetime.now()
            inactive_nodes = []
            active_count = 0
            total_nodes = len(node_heartbeats)

            # Log current system state
            log_system_state(UPLOAD_FOLDERS)
            
            for node, last_heartbeat in node_heartbeats.items():
                time_since_last_heartbeat = (now - last_heartbeat).total_seconds()
                
                if time_since_last_heartbeat > HEARTBEAT_THRESHOLD:
                    inactive_nodes.append(node)
                    logger.warning(f"Node {node} exceeded heartbeat threshold: {time_since_last_heartbeat:.2f} seconds since last heartbeat")
                else:
                    active_count += 1
                    logger.debug(f"Node {node} active: {time_since_last_heartbeat:.2f} seconds since last heartbeat")

            # Log overall system health
            logger.info(f"System health check - Active nodes: {active_count}/{total_nodes}")
            
            # Handle inactive nodes
            for node in inactive_nodes:
                logger.error(f"Node {node} is inactive - initiating failure handling")
                try:
                    # Handle node failure
                    handle_node_failure(node)
                    # Remove inactive node from tracking
                    del node_heartbeats[node]
                    logger.info(f"Successfully processed failure for node {node}")
                except Exception as e:
                    logger.error(f"Failed to handle node failure for {node}: {str(e)}", exc_info=True)

            # Log node health metrics
            log_node_health(node_heartbeats)
            
        except Exception as e:
            logger.error(f"Error in node monitoring loop: {str(e)}", exc_info=True)
            
        finally:
            time.sleep(10)  # Check every 10 seconds

@log_operation('node_failure')
@log_performance
def handle_node_failure(node_name):
    """Handle failure of a specific node."""
    try:
        logger.warning(f"Initiating failure handling for node: {node_name}")
        
        # Log the current state before redistribution
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) as chunk_count 
                FROM chunks 
                WHERE chunk_location LIKE ?
            ''', (f'%{node_name}%',))
            affected_chunks = cursor.fetchone()[0]
            
        logger.info(f"Found {affected_chunks} chunks to redistribute from node {node_name}")
        
        # Start redistribution
        start_time = time.time()
        redistribute_chunks(node_name)
        duration = time.time() - start_time
        
        # Log successful completion
        logger.info(f"Successfully handled failure for node {node_name}. Duration: {duration:.2f} seconds")
        
        # Verify redistribution
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) as remaining_chunks 
                FROM chunks 
                WHERE chunk_location LIKE ? AND status = 'active'
            ''', (f'%{node_name}%',))
            remaining_chunks = cursor.fetchone()[0]
            
        if remaining_chunks > 0:
            logger.warning(f"Found {remaining_chunks} chunks still referencing failed node {node_name}")
        else:
            logger.info(f"All chunks successfully redistributed from node {node_name}")
            
        # Log updated system state
        log_system_state(UPLOAD_FOLDERS)
        
    except Exception as e:
        logger.error(f"Failed to handle node failure for {node_name}: {str(e)}", exc_info=True)
        raise

@app.route('/files', methods=['GET'])
@log_operation('list_files')
def list_files():
    """List all files for non-admin users."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get all files with their metadata
            cursor.execute('''
                SELECT 
                    m.id,
                    m.filename,
                    m.size,
                    m.compressed_size,
                    m.upload_timestamp,
                    m.location,
                    m.replicas
                FROM metadata m
                WHERE m.is_archived = 0
                ORDER BY m.upload_timestamp DESC
            ''')
            
            files = []
            for row in cursor.fetchall():
                files.append({
                    'id': row['id'],
                    'filename': row['filename'],
                    'size': row['size'],
                    'compressed_size': row['compressed_size'],
                    'upload_timestamp': row['upload_timestamp'],
                    'primary_location': row['location']
                })
        
        return jsonify({'files': files})
        
    except Exception as e:
        logger.error(f"Error in file listing: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/login', methods=['POST'])
def login():
    """Handle admin login."""
    data = request.json
    if not data or 'adminKey' not in data:
        return jsonify({'error': 'Admin key is required'}), 400
        
    admin_key = data['adminKey']
    
    # Check against the hardcoded admin key
    if admin_key == "kv83nhjd9d9j3mxsidsis9432u3jsdkslos9":
        return jsonify({
            'success': True,
            'message': 'Login successful'
        })
    
    return jsonify({
        'success': False,
        'message': 'Invalid admin key'
    }), 401

# Add this function to initialize the database with some default values
def initialize_system_stats():
    """Initialize system statistics in the database."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Initialize storage tiers if they don't exist
            cursor.execute('''
                INSERT OR IGNORE INTO storage_tiers 
                (tier_name, max_size, retention_days, auto_archive_days, 
                 compression_level, created_at, last_modified)
                VALUES 
                ('hot', 500000000, 30, NULL, 1, datetime('now'), datetime('now')),
                ('warm', 500000000, 90, 60, 6, datetime('now'), datetime('now')),
                ('cold', 500000000, 365, 180, 9, datetime('now'), datetime('now'))
            ''')
            
            conn.commit()
            logger.info("System stats initialized successfully")
            
    except Exception as e:
        logger.error(f"Error initializing system stats: {str(e)}")
        raise

if __name__ == '__main__':
    print("Starting Heartbeat.")
    # Start the heartbeat subprocess
    heartbeat_process = subprocess.Popen(['python', 'node_heartbeat.py'])

    # Register a cleanup function to terminate the heartbeat subprocess
    def cleanup():
        print("Shutting down Heartbeat.")
        heartbeat_process.terminate()
        try:
            heartbeat_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            heartbeat_process.kill()
            print("Heartbeat process forcefully killed.")

    # Register the cleanup function to be called at exit
    atexit.register(cleanup)

    def signal_handler(signum, frame):
        print(f"Signal {signum} received. Cleaning up...")
        cleanup()
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start the node monitoring thread
    monitoring_thread = threading.Thread(target=monitor_nodes, daemon=True)
    monitoring_thread.start()

    start_storage_management()

    try:
        # Initialize encryption
        encryption_manager = EncryptionManager()
        print("Encryption initialized.")

        # Start the Flask app with HTTPS
        app.run(host='0.0.0.0', port=5000)

    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Shutting down...")