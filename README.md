# Distributed File System (DFS)

## 🚀 Overview

A modern, scalable Distributed File System implementation featuring automatic file replication, storage tiering, and intelligent data deduplication. Built with Python (Flask) and React/Next.js, this system demonstrates enterprise-level architectural patterns and advanced data management capabilities.

![System Architecture](https://via.placeholder.com/800x400?text=DFS+Architecture)

## ✨ Key Features

### Storage Management
- **Multi-tiered Storage Architecture** (Hot, Warm, Cold)
- **Intelligent Data Migration** based on access patterns
- **Automatic Compression** with configurable levels per tier
- **Data Deduplication** to optimize storage usage
- **File Chunking** for efficient large file handling

### High Availability
- **Automatic Node Health Monitoring**
- **Real-time Node Failure Detection**
- **Automatic Data Replication** across nodes
- **Self-healing Capabilities** when nodes fail
- **Configurable Replication Factors**

### Security
- **Encryption at Rest** using PBKDF2 and Fernet
- **Secure Admin Interface** with role-based access
- **HTTPS Support** with automatic certificate generation
- **Robust Authentication System**

### Modern Web Interface
- **React-based Admin Dashboard**
- **Real-time System Monitoring**
- **Interactive File Management**
- **Progress Tracking** for all operations
- **Responsive Design** using Tailwind CSS

## 🛠 Technical Stack

### Backend
- **Python 3.x**
- **Flask** - Web framework
- **SQLite** - Metadata storage
- **cryptography** - Data encryption
- **zlib** - Data compression

### Frontend
- **React 18**
- **Next.js 13**
- **TypeScript**
- **Tailwind CSS**
- **shadcn/ui** - UI components
- **Lucide Icons**

### System Components
- **Node Management System**
- **Storage Tier Manager**
- **File Chunking Service**
- **Replication Manager**
- **Health Monitoring System**

## 🏗 Architecture

The system is built on a distributed architecture with the following key components:

### Storage Nodes
- Multiple independent storage nodes
- Local data management
- Health reporting
- Chunk storage and retrieval

### Metadata Management
- Centralized metadata database
- File location tracking
- Version control
- Deduplication records

### File Processing Pipeline
1. File upload initiated
2. File chunked into manageable pieces
3. Chunks compressed and encrypted
4. Distributed across storage nodes
5. Metadata updated
6. Replicas created for redundancy

## 💻 Getting Started

### Prerequisites
```bash
# Clone the repository
git clone https://github.com/APinchofPepper/DistributedFileSystem.git

# Install Python dependencies
pip install -r requirements.txt

# Install Node.js dependencies
cd dfs-frontend
npm install
```

### Configuration
```python
# config.py
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunks
VERSION_SYNC_TIMEOUT = 30     # seconds
DB_TIMEOUT = 20.0
MAX_RETRIES = 3
```

### Running the System
```bash
# Start the backend server
python server.py

# In a new terminal, start the frontend
cd dfs-frontend
npm run dev
```

## 🔧 Advanced Features

### Storage Tiering
The system automatically manages data across three storage tiers:
- **Hot Storage**: Frequently accessed data
- **Warm Storage**: Intermediately accessed data
- **Cold Storage**: Rarely accessed data

### Data Deduplication
```python
def manage_deduplication():
    """Find and manage duplicate files."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT content_hash, COUNT(*) as count
            FROM metadata
            GROUP BY content_hash
            HAVING count > 1
        ''')
```

### Node Health Monitoring
```python
def monitor_nodes():
    """Periodically check for inactive nodes."""
    while True:
        for node, last_heartbeat in node_heartbeats.items():
            time_since_heartbeat = (now - last_heartbeat).total_seconds()
            if time_since_heartbeat > HEARTBEAT_THRESHOLD:
                handle_node_failure(node)
```

## 🔐 Security Features

### Encryption
- PBKDF2 key derivation
- Fernet symmetric encryption
- Automatic key rotation
- Secure key storage

### Authentication
- Role-based access control
- Admin authentication
- Secure session management

## 📊 Admin Dashboard

The admin dashboard provides comprehensive system monitoring and management capabilities:
- Real-time node status monitoring
- Storage usage visualization
- File operation management
- System health metrics
- Storage tier management

## 🚀 Performance

The system is designed for optimal performance:
- Chunked file transfers
- Parallel processing
- Efficient data compression
- Smart caching
- Load balancing

## 🛟 Error Handling

Robust error handling throughout the system:
- Automatic retry mechanisms
- Graceful degradation
- Comprehensive logging
- Error reporting
- Recovery procedures

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## 🎯 Future Enhancements

- [ ] Distributed metadata storage
- [ ] Advanced caching mechanisms
- [ ] Machine learning for access pattern prediction
- [ ] Geographic data distribution
- [ ] Enhanced monitoring and analytics

## 👨‍💻 About the Author

This project was developed to demonstrate advanced distributed systems concepts and modern web development practices. It showcases expertise in:
- Distributed Systems Architecture
- Full-Stack Development
- System Design
- Security Implementation
- Performance Optimization

## 📞 Contact

For questions and feedback:
- Email: mustonej@oregonstate.edu
- Website: jackmustonen.com
- GitHub: [Your GitHub Profile](https://github.com/APinchofPepper)

---
