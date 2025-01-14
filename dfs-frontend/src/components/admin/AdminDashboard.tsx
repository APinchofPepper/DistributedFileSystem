import React, { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Input } from '@/components/ui/input';
import { useAuth } from '@/contexts/AuthContext';
import { 
  Settings, 
  Database, 
  Activity, 
  RefreshCw,
  Shield,
  HardDrive,
  Archive,
  AlertCircle,
  FileUp,
  Download,
  RotateCw,
  Search,
  History,
  Wrench,
  ArrowLeft
} from 'lucide-react';

// Interfaces
interface NodeStatus {
  status: string;
  storage: {
    used_bytes: number;
    usage_percent: number;
  };
}

interface StorageTier {
  name: string;
  file_count: number;
  total_size: number;
  total_compressed_size: number;
}

interface SystemStats {
  total_files: number;
  total_storage: number;
  used_storage: number;
  node_count: number;
}

// Utility function
function formatBytes(bytes: number) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

// System Configuration Component
function SystemConfig() {
  const { adminKey } = useAuth();
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState<SystemStats>({
    total_files: 0,
    total_storage: 0,
    used_storage: 0,
    node_count: 0
  });

  React.useEffect(() => {
    fetchSystemStats();
  }, []);

  const fetchSystemStats = async () => {
    try {
      const response = await fetch('http://localhost:5000/admin/storage/tiers', {
        headers: { 'X-Admin-Key': adminKey! }
      });
      const data = await response.json();
      
      const totalStats = data.tiers.reduce((acc: Partial<SystemStats>, tier: StorageTier) => ({
        total_files: (acc.total_files || 0) + tier.file_count,
        total_storage: (acc.total_storage || 0) + tier.total_size,
        used_storage: (acc.used_storage || 0) + tier.total_compressed_size
      }), { total_files: 0, total_storage: 0, used_storage: 0 });

      setStats({
        ...totalStats as SystemStats,
        node_count: 3
      });
      setLoading(false);
    } catch (err) {
      console.error('Failed to fetch system stats:', err);
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center p-8">
        <RefreshCw className="h-8 w-8 animate-spin text-blue-500" />
      </div>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>System Overview</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-6 bg-white rounded-lg shadow">
            <h3 className="text-lg font-semibold mb-4">Storage Statistics</h3>
            <div className="space-y-2">
              <p>Total Files: {stats.total_files}</p>
              <p>Storage Used: {formatBytes(stats.used_storage)}</p>
              <p>Total Storage: {formatBytes(stats.total_storage)}</p>
              <p>Usage: {((stats.used_storage / stats.total_storage) * 100).toFixed(1)}%</p>
            </div>
          </div>
          <div className="p-6 bg-white rounded-lg shadow">
            <h3 className="text-lg font-semibold mb-4">Node Information</h3>
            <div className="space-y-2">
              <p>Active Nodes: {stats.node_count}</p>
              <p>Storage Per Node: {formatBytes(stats.total_storage / stats.node_count)}</p>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

interface VerificationResult {
  verified: boolean;
  timestamp: string;
  results?: {
    verified_chunks: number;
    corrupted_chunks: number;
    missing_chunks: number;
  };
}

export function NodeMonitor() {
  const { adminKey } = useAuth();
  const [nodeStatus, setNodeStatus] = useState<Record<string, NodeStatus>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [verificationResults, setVerificationResults] = useState<Record<string, VerificationResult>>({});

  React.useEffect(() => {
    fetchNodeStatus();
    const interval = setInterval(fetchNodeStatus, 10000);
    return () => clearInterval(interval);
  }, []);

  const fetchNodeStatus = async () => {
    try {
      const response = await fetch('http://localhost:5000/admin/nodes/health', {
        headers: { 'X-Admin-Key': adminKey! }
      });
      const data = await response.json();
      setNodeStatus(data.nodes || {});
      setError(null);
      setLoading(false);
    } catch (err) {
      setError('Failed to fetch node status');
      console.error('Failed to fetch node status:', err);
    }
  };

  const handleVerifyNode = async (nodeName: string) => {
    try {
      // Set verification in progress
      setVerificationResults(prev => ({
        ...prev,
        [nodeName]: { verified: false, timestamp: new Date().toISOString() }
      }));

      const response = await fetch(`http://localhost:5000/admin/nodes/${nodeName}/verify`, {
        method: 'POST',
        headers: { 'X-Admin-Key': adminKey! }
      });
      
      if (!response.ok) throw new Error('Failed to verify node');
      
      const data = await response.json();
      
      // Update verification results with the response data
      setVerificationResults(prev => ({
        ...prev,
        [nodeName]: {
          verified: true,
          timestamp: new Date().toISOString(),
          results: {
            verified_chunks: data.results.verified_chunks.length,
            corrupted_chunks: data.results.corrupted_chunks.length,
            missing_chunks: data.results.missing_chunks.length
          }
        }
      }));
      
      setError(null);
      // Refresh node status
      fetchNodeStatus();
    } catch (err) {
      setError('Failed to verify node');
      setVerificationResults(prev => ({
        ...prev,
        [nodeName]: {
          verified: false,
          timestamp: new Date().toISOString()
        }
      }));
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center p-8">
        <RefreshCw className="h-8 w-8 animate-spin text-blue-500" />
      </div>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Node Status</CardTitle>
      </CardHeader>
      <CardContent>
        {error && (
          <Alert variant="destructive" className="mb-4">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {Object.entries(nodeStatus).map(([nodeName, status]) => (
            <Card key={nodeName}>
              <CardContent className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-lg font-semibold">{nodeName}</h3>
                  <span className={`px-2 py-1 rounded-full text-sm ${
                    status.status === 'active' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                  }`}>
                    {status.status}
                  </span>
                </div>
                <div className="space-y-2">
                  <p>Storage Used: {formatBytes(status.storage.used_bytes)}</p>
                  <p>Usage: {status.storage.usage_percent.toFixed(1)}%</p>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div
                      className="bg-blue-500 rounded-full h-2"
                      style={{ width: `${status.storage.usage_percent}%` }}
                    />
                  </div>

                  {/* Verification Results Display */}
                  {verificationResults[nodeName] && (
                    <div className="mt-4 p-3 bg-gray-50 rounded-lg">
                      <p className="text-sm font-medium mb-2">
                        Verification Status:
                      </p>
                      {verificationResults[nodeName].results ? (
                        <div className="space-y-1 text-sm">
                          <p className="text-green-600">
                            ✓ {verificationResults[nodeName].results.verified_chunks} chunks verified
                          </p>
                          {verificationResults[nodeName].results.corrupted_chunks > 0 && (
                            <p className="text-red-600">
                              ⚠ {verificationResults[nodeName].results.corrupted_chunks} corrupted chunks
                            </p>
                          )}
                          {verificationResults[nodeName].results.missing_chunks > 0 && (
                            <p className="text-red-600">
                              ⚠ {verificationResults[nodeName].results.missing_chunks} missing chunks
                            </p>
                          )}
                          <p className="text-gray-500 text-xs mt-2">
                            Last verified: {new Date(verificationResults[nodeName].timestamp).toLocaleTimeString()}
                          </p>
                        </div>
                      ) : (
                        <p className="text-sm text-gray-500">Verification in progress...</p>
                      )}
                    </div>
                  )}

                  <button
                    onClick={() => handleVerifyNode(nodeName)}
                    className="mt-4 w-full bg-blue-500 text-white px-4 py-2 rounded-lg hover:bg-blue-600 disabled:opacity-50"
                    disabled={verificationResults[nodeName]?.verified === false}
                  >
                    {verificationResults[nodeName]?.verified === false ? (
                      <div className="flex items-center justify-center gap-2">
                        <RefreshCw className="h-4 w-4 animate-spin" />
                        <span>Verifying...</span>
                      </div>
                    ) : (
                      'Verify Node'
                    )}
                  </button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

// Storage Manager Component
function StorageManager() {
  const { adminKey } = useAuth();
  const [storageStats, setStorageStats] = useState<StorageTier[]>([]);
  const [loading, setLoading] = useState(true);

  React.useEffect(() => {
    fetchStorageStats();
  }, []);

  const fetchStorageStats = async () => {
    try {
      const response = await fetch('http://localhost:5000/admin/storage/tiers', {
        headers: { 'X-Admin-Key': adminKey! }
      });
      const data = await response.json();
      setStorageStats(data.tiers || []);
      setLoading(false);
    } catch (err) {
      console.error('Failed to fetch storage stats:', err);
    }
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center p-8">
        <RefreshCw className="h-8 w-8 animate-spin text-blue-500" />
      </div>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Storage Tiers</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {storageStats.map((tier) => (
            <Card key={tier.name}>
              <CardContent className="p-6">
                <h3 className="text-lg font-semibold mb-4 capitalize">{tier.name} Storage</h3>
                <div className="space-y-2">
                  <p>Files: {tier.file_count}</p>
                  <p>Total Size: {formatBytes(tier.total_size)}</p>
                  <p>Compressed: {formatBytes(tier.total_compressed_size)}</p>
                  <p>Savings: {((1 - tier.total_compressed_size / tier.total_size) * 100).toFixed(1)}%</p>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

// File Operations Component
function FileOperations() {
  const { adminKey } = useAuth();
  const [selectedFile, setSelectedFile] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [files, setFiles] = useState<any[]>([]);

  React.useEffect(() => {
    fetchFiles();
  }, []);

  const fetchFiles = async () => {
    try {
      const response = await fetch('http://localhost:5000/admin/files', {
        headers: { 'X-Admin-Key': adminKey! }
      });
      const data = await response.json();
      setFiles(data.files || []);
      setError(null);
    } catch (err) {
      setError('Failed to fetch files');
    }
  };

  const handleArchiveFile = async (fileId: number) => {
    setLoading(true);
    try {
      const response = await fetch(`http://localhost:5000/admin/files/${fileId}/archive`, {
        method: 'POST',
        headers: { 'X-Admin-Key': adminKey! }
      });
      if (!response.ok) throw new Error('Failed to archive file');
      await fetchFiles();
      setError(null);
    } catch (err) {
      setError('Failed to archive file');
    } finally {
      setLoading(false);
    }
  };

  const handleRestoreFile = async (fileId: number) => {
    setLoading(true);
    try {
      const response = await fetch(`http://localhost:5000/admin/files/${fileId}/restore`, {
        method: 'POST',
        headers: { 'X-Admin-Key': adminKey! }
      });
      if (!response.ok) throw new Error('Failed to restore file');
      await fetchFiles();
      setError(null);
    } catch (err) {
      setError('Failed to restore file');
    } finally {
      setLoading(false);
    }
  };

  const handleReallocateFile = async (fileId: number) => {
    setLoading(true);
    try {
      const response = await fetch(`http://localhost:5000/admin/files/${fileId}/reallocate`, {
        method: 'POST',
        headers: { 'X-Admin-Key': adminKey! }
      });
      if (!response.ok) throw new Error('Failed to reallocate file');
      await fetchFiles();
      setError(null);
    } catch (err) {
      setError('Failed to reallocate file');
    } finally {
      setLoading(false);
    }
  };

  const filteredFiles = files.filter(file => 
    file.filename.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>File Operations</CardTitle>
      </CardHeader>
      <CardContent>
        {error && (
          <Alert variant="destructive" className="mb-4">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <div className="mb-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <Input
              type="text"
              placeholder="Search files..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b">
                <th className="px-4 py-2 text-left">Filename</th>
                <th className="px-4 py-2 text-left">Size</th>
                <th className="px-4 py-2 text-left">Location</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredFiles.map((file) => (
                <tr key={file.id} className="border-b hover:bg-gray-50">
                <td className="px-4 py-2">{file.filename}</td>
                <td className="px-4 py-2">{formatBytes(file.size)}</td>
                <td className="px-4 py-2">{file.primary_location}</td>
                <td className="px-4 py-2">
                  <div className="flex space-x-2">
                    <button
                      onClick={() => handleArchiveFile(file.id)}
                      disabled={loading}
                      className="text-blue-500 hover:text-blue-600 disabled:opacity-50"
                      title="Archive"
                    >
                      <Archive className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => handleRestoreFile(file.id)}
                      disabled={loading}
                      className="text-green-500 hover:text-green-600 disabled:opacity-50"
                      title="Restore"
                    >
                      <FileUp className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => handleReallocateFile(file.id)}
                      disabled={loading}
                      className="text-purple-500 hover:text-purple-600 disabled:opacity-50"
                      title="Reallocate"
                    >
                      <RotateCw className="h-4 w-4" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {loading && (
        <div className="flex justify-center items-center mt-4">
          <RefreshCw className="h-6 w-6 animate-spin text-blue-500" />
        </div>
      )}
    </CardContent>
  </Card>
);
}

// src/components/admin/AdminDashboard.tsx
interface AdminDashboardProps {
  onReturn: () => void;
}

export function AdminDashboard({ onReturn }: AdminDashboardProps) {
  const [activeTab, setActiveTab] = useState('system');
  const { logout } = useAuth();

  const renderContent = () => {
    switch (activeTab) {
      case 'system':
        return <SystemConfig />;
      case 'nodes':
        return <NodeMonitor />;
      case 'storage':
        return <StorageManager />;
      case 'files':
        return <FileOperations />;
      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-7xl mx-auto">
        <div className="flex justify-between items-center mb-8">
          <div className="flex items-center gap-4">
            <button
              onClick={onReturn}
              className="bg-gray-100 text-gray-600 px-4 py-2 rounded-lg hover:bg-gray-200 flex items-center gap-2"
            >
              <ArrowLeft className="h-4 w-4" />
              Back to Files
            </button>
            <h1 className="text-3xl font-bold text-gray-900">Admin Dashboard</h1>
          </div>
          <button
            onClick={logout}
            className="bg-red-500 text-white px-4 py-2 rounded-lg hover:bg-red-600"
          >
            Logout
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <button
            onClick={() => setActiveTab('system')}
            className={`p-6 rounded-lg flex items-center gap-3 ${
              activeTab === 'system' ? 'bg-blue-500 text-white' : 'bg-white'
            }`}
          >
            <Settings className="h-6 w-6" />
            <span>System Status</span>
          </button>

          <button
            onClick={() => setActiveTab('nodes')}
            className={`p-6 rounded-lg flex items-center gap-3 ${
              activeTab === 'nodes' ? 'bg-blue-500 text-white' : 'bg-white'
            }`}
          >
            <Activity className="h-6 w-6" />
            <span>Node Monitor</span>
          </button>

          <button
            onClick={() => setActiveTab('storage')}
            className={`p-6 rounded-lg flex items-center gap-3 ${
              activeTab === 'storage' ? 'bg-blue-500 text-white' : 'bg-white'
            }`}
          >
            <Database className="h-6 w-6" />
            <span>Storage Management</span>
          </button>

          <button
            onClick={() => setActiveTab('files')}
            className={`p-6 rounded-lg flex items-center gap-3 ${
              activeTab === 'files' ? 'bg-blue-500 text-white' : 'bg-white'
            }`}
          >
            <Wrench className="h-6 w-6" />
            <span>File Operations</span>
          </button>
        </div>

        <div className="mt-6">
          {renderContent()}
        </div>
      </div>
    </div>
  );
}