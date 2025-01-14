import sqlite3
import os
from datetime import datetime

def setup_database():
    # Backup existing database if it exists
    if os.path.exists('metadata.db'):
        backup_path = 'metadata.db.backup'
        counter = 1
        while os.path.exists(backup_path):
            backup_path = f'metadata.db.backup.{counter}'
            counter += 1
        try:
            os.rename('metadata.db', backup_path)
            print(f"Created backup of existing database at: {backup_path}")
        except Exception as e:
            print(f"Warning: Could not create backup: {e}")

    # Create new database connection
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()

    # Create table for storage tiers configuration
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS storage_tiers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tier_name TEXT NOT NULL UNIQUE CHECK(tier_name IN ('hot', 'warm', 'cold')),
            max_size INTEGER NOT NULL,
            retention_days INTEGER NOT NULL,
            auto_archive_days INTEGER,
            compression_level INTEGER CHECK(compression_level BETWEEN 0 AND 9),
            created_at TEXT NOT NULL,
            last_modified TEXT NOT NULL
        )
    ''')

    # Create table for retention policies
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS retention_policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            policy_name TEXT NOT NULL UNIQUE,
            min_versions INTEGER NOT NULL,
            max_versions INTEGER NOT NULL,
            retention_period_days INTEGER NOT NULL,
            auto_archive_enabled INTEGER DEFAULT 0,
            archive_after_days INTEGER,
            created_at TEXT NOT NULL,
            last_modified TEXT NOT NULL
        )
    ''')

    # Create enhanced metadata table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            current_version INTEGER NOT NULL DEFAULT 1,
            size INTEGER NOT NULL,
            compressed_size INTEGER NOT NULL,
            compression_ratio REAL NOT NULL,
            upload_timestamp TEXT NOT NULL,
            location TEXT NOT NULL,
            replicas TEXT,
            storage_tier TEXT CHECK(storage_tier IN ('hot', 'warm', 'cold')) DEFAULT 'hot',
            last_accessed TEXT,
            access_count INTEGER DEFAULT 0,
            retention_policy TEXT,
            is_archived INTEGER DEFAULT 0,
            archive_date TEXT,
            content_hash TEXT,
            deduplication_ref INTEGER,
            FOREIGN KEY(deduplication_ref) REFERENCES metadata(id),
            FOREIGN KEY(retention_policy) REFERENCES retention_policies(policy_name)
        )
    ''')

    # Create versions table with storage tier support
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            version_number INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            size INTEGER NOT NULL,
            compressed_size INTEGER NOT NULL,
            hash TEXT NOT NULL,
            storage_tier TEXT DEFAULT 'hot',
            is_archived INTEGER DEFAULT 0,
            FOREIGN KEY(file_id) REFERENCES metadata(id),
            UNIQUE(file_id, version_number)
        )
    ''')

    # Create enhanced chunks table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            version_number INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_location TEXT NOT NULL,
            original_size INTEGER NOT NULL,
            compressed_size INTEGER NOT NULL,
            chunk_hash TEXT NOT NULL,
            storage_tier TEXT DEFAULT 'hot',
            deduplication_ref TEXT,
            status TEXT CHECK(status IN ('pending', 'active', 'deprecated', 'archived')) NOT NULL DEFAULT 'pending',
            FOREIGN KEY(file_id) REFERENCES metadata(id)
        )
    ''')

    # Create storage nodes table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_name TEXT NOT NULL UNIQUE,
            last_heartbeat TEXT NOT NULL
        )
    ''')

    # Create consistency tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS consistency_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            version_number INTEGER NOT NULL,
            node_name TEXT NOT NULL,
            status TEXT CHECK(status IN ('pending', 'synced', 'failed')) NOT NULL,
            last_update TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES metadata(id),
            UNIQUE(file_id, version_number, node_name)
        )
    ''')

    # Create version changes table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS version_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            old_version INTEGER NOT NULL,
            new_version INTEGER NOT NULL,
            change_type TEXT NOT NULL CHECK(change_type IN ('create', 'update', 'rollback', 'revert')),
            change_description TEXT,
            user_id TEXT,
            timestamp TEXT NOT NULL,
            parent_version INTEGER,
            FOREIGN KEY(file_id) REFERENCES metadata(id),
            FOREIGN KEY(parent_version) REFERENCES version_changes(id)
        )
    ''')

    # Create version tags table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS version_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            version_number INTEGER NOT NULL,
            tag_name TEXT NOT NULL,
            tag_description TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT,
            FOREIGN KEY(file_id) REFERENCES metadata(id),
            UNIQUE(file_id, version_number, tag_name)
        )
    ''')

    # Create file access history table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS access_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            access_type TEXT NOT NULL,
            access_timestamp TEXT NOT NULL,
            user_id TEXT,
            FOREIGN KEY(file_id) REFERENCES metadata(id)
        )
    ''')

    # Create deduplication tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deduplication (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_hash TEXT NOT NULL UNIQUE,
            reference_count INTEGER DEFAULT 1,
            total_space_saved INTEGER DEFAULT 0,
            first_seen TEXT NOT NULL,
            last_reference TEXT NOT NULL
        )
    ''')

    # Create archives table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS archives (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            archive_date TEXT NOT NULL,
            archive_location TEXT NOT NULL,
            archive_size INTEGER NOT NULL,
            restore_count INTEGER DEFAULT 0,
            last_restore_date TEXT,
            archive_tier TEXT DEFAULT 'cold',
            FOREIGN KEY(file_id) REFERENCES metadata(id)
        )
    ''')

    # Create all necessary indices
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chunks_file_version ON chunks(file_id, version_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_versions_file ON versions(file_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_consistency_file_version ON consistency_status(file_id, version_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_version_changes_file ON version_changes(file_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_version_tags_file ON version_tags(file_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_metadata_storage_tier ON metadata(storage_tier)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_metadata_content_hash ON metadata(content_hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_deduplication_hash ON deduplication(content_hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_access_history_file ON access_history(file_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_archives_file ON archives(file_id)')

    # Insert default storage tier configurations
    default_tiers = [
        ('hot', 1000000000, 30, None, 1, datetime.now().isoformat(), datetime.now().isoformat()),
        ('warm', 5000000000, 90, 60, 6, datetime.now().isoformat(), datetime.now().isoformat()),
        ('cold', 10000000000, 365, 180, 9, datetime.now().isoformat(), datetime.now().isoformat())
    ]
    
    # Insert default storage tier configurations if they don't exist
    cursor.execute('''
        INSERT OR IGNORE INTO storage_tiers 
        (tier_name, max_size, retention_days, auto_archive_days, compression_level, created_at, last_modified)
        VALUES 
        ('hot', 1000000000, 30, NULL, 1, datetime('now'), datetime('now')),
        ('warm', 5000000000, 90, 60, 6, datetime('now'), datetime('now')),
        ('cold', 10000000000, 365, 180, 9, datetime('now'), datetime('now'))
    ''')

    # Insert default retention policy
    cursor.execute('''
        INSERT OR IGNORE INTO retention_policies 
        (policy_name, min_versions, max_versions, retention_period_days, 
         auto_archive_enabled, archive_after_days, created_at, last_modified)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', ('default', 1, 10, 365, 1, 180, datetime.now().isoformat(), datetime.now().isoformat()))

    # Commit changes and close connection
    conn.commit()
    conn.close()

def verify_database():
    """Verify that all tables and configurations were created correctly."""
    conn = sqlite3.connect('metadata.db')
    cursor = conn.cursor()
    
    # Get list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    print("\nDatabase verification:")
    print("Tables found:", len(tables))
    for table in tables:
        print(f"- {table[0]}")
    
    # Verify storage tiers
    cursor.execute("SELECT * FROM storage_tiers")
    tiers = cursor.fetchall()
    print("\nStorage tiers configured:", len(tiers))
    
    # Verify retention policies
    cursor.execute("SELECT * FROM retention_policies")
    policies = cursor.fetchall()
    print("Retention policies configured:", len(policies))
    
    # Verify indices
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indices = cursor.fetchall()
    print("\nIndices created:", len(indices))
    for index in indices:
        print(f"- {index[0]}")
    
    conn.close()

if __name__ == "__main__":
    print("Setting up enhanced database with storage management features...")
    setup_database()
    print("Database setup complete.")
    verify_database()
    print("\nDatabase has been updated with storage tiers, retention policies, archiving support, and performance optimizations.")