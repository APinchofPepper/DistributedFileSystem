import time
import requests
import threading
import urllib3
from datetime import datetime

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# List of nodes and their names
NODES = [
    {"name": "Node1", "url": "http://localhost:5000/heartbeat"},
    {"name": "Node2", "url": "http://localhost:5000/heartbeat"},
    {"name": "Node3", "url": "http://localhost:5000/heartbeat"}
]

# Interval for sending heartbeats (in seconds)
HEARTBEAT_INTERVAL = 10

def send_heartbeat(node_name, url):
    """Send heartbeats for a specific node."""
    session = requests.Session()
    while True:
        try:
            response = session.post(
                url, 
                json={"node_name": node_name},
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 200:
                print(f"Heartbeat sent for {node_name}: {response.json()}")
            else:
                print(f"Failed to send heartbeat for {node_name}: {response.status_code}")
        except Exception as e:
            print(f"Error sending heartbeat for {node_name}: {e}")
        time.sleep(HEARTBEAT_INTERVAL)

if __name__ == "__main__":
    print(f"Starting heartbeat simulation for {len(NODES)} nodes...")
    
    # Start a thread for each node
    threads = []
    for node in NODES:
        thread = threading.Thread(
            target=send_heartbeat,
            args=(node["name"], node["url"]),
            daemon=True
        )
        thread.start()
        threads.append(thread)
        print(f"Started heartbeat thread for {node['name']}")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down heartbeat simulation...")