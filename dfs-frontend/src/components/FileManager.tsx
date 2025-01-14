// src/components/FileManager.tsx
'use client';

import React, { useState, useEffect, useCallback } from 'react';
import { 
  AlertCircle, 
  Upload, 
  Download, 
  Server, 
  RefreshCw, 
  Clock,
  Search,
  Trash2,
  FileText,
  Shield
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Input } from '@/components/ui/input';
import { Progress } from '@/components/ui/progress';
import VersionHistory from './VersionHistory';
import { useAuth } from '@/contexts/AuthContext';
import { useDropzone } from 'react-dropzone';

interface FileManagerProps {
  onShowAdmin: () => void;
}

interface FileInfo {
  id: number;
  filename: string;
  size: number;
  compressed_size: number;
  primary_location: string;
}

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

interface UploadProgress {
  [key: string]: number;
}

export function FileManager({ onShowAdmin }: FileManagerProps) {
  const { isAdmin, adminKey } = useAuth();
  const [files, setFiles] = useState<FileInfo[]>([]);
  const [nodeStatus, setNodeStatus] = useState<Record<string, NodeStatus>>({});
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [storageStats, setStorageStats] = useState<StorageTier[]>([]);
  const [selectedTab, setSelectedTab] = useState('files');
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [uploadProgress, setUploadProgress] = useState<UploadProgress>({});
  const [selectedFiles, setSelectedFiles] = useState<Set<number>>(new Set());
  const [totalStorage, setTotalStorage] = useState({
    used: 0,
    total: 0,
    fileCount: 0
  });

  // Filter files based on search query
  const filteredFiles = files.filter(file => 
    file.filename.toLowerCase().includes(searchQuery.toLowerCase())
  );

  // Initialize data and set up polling
  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      await Promise.all([
        fetchFiles(),
        fetchNodeStatus(),
        fetchStorageStats()
      ]);
    } catch (err) {
      console.error('Error fetching data:', err);
    }
  };

  const fetchFiles = async () => {
    try {
      const headers: HeadersInit = {};
      if (isAdmin && adminKey) {
        headers['X-Admin-Key'] = adminKey;
      }

      const response = await fetch('http://localhost:5000/admin/files', {
        headers
      });
      const data = await response.json();
      setFiles(data.files || []);
      setError(null);
    } catch (err) {
      setError('Failed to fetch files');
    }
  };

  const fetchNodeStatus = async () => {
    if (!isAdmin || !adminKey) return;
    
    try {
      const response = await fetch('http://localhost:5000/admin/nodes/health', {
        headers: { 'X-Admin-Key': adminKey }
      });
      const data = await response.json();
      setNodeStatus(data.nodes || {});
      setError(null);
    } catch (err) {
      setError('Failed to fetch node status');
    }
  };

  const fetchStorageStats = async () => {
    if (!isAdmin || !adminKey) return;
    
    try {
      const response = await fetch('http://localhost:5000/admin/storage/tiers', {
        headers: { 'X-Admin-Key': adminKey }
      });
      const data = await response.json();
      setStorageStats(data.tiers || []);
      updateTotalStorage(data.tiers || []);
      setError(null);
    } catch (err) {
      setError('Failed to fetch storage stats');
    }
  };

  const updateTotalStorage = (tiers: StorageTier[]) => {
    const total = tiers.reduce((acc, tier) => ({
      used: acc.used + tier.total_size,
      fileCount: acc.fileCount + tier.file_count
    }), { used: 0, fileCount: 0 });
    
    setTotalStorage({
      used: total.used,
      total: 500 * 1024 * 1024 * 3, // 500MB * 3 nodes
      fileCount: total.fileCount
    });
  };

  // Download functionality
  const downloadFile = async (filename: string) => {
    try {
      const response = await fetch(`http://localhost:5000/download/${filename}`);
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      setError(null);
    } catch (err) {
      setError('Failed to download file');
    }
  };

  // Delete functionality
  const handleDelete = async (filename: string) => {
    try {
      const response = await fetch(`http://localhost:5000/delete/${filename}`, {
        method: 'DELETE',
        headers: {
          'X-Admin-Key': adminKey || ''
        }
      });

      if (!response.ok) throw new Error('Delete failed');
      await fetchFiles();
    } catch (err) {
      setError('Failed to delete file');
    }
  };

  // Batch operations
  const handleDeleteSelected = async () => {
    const deletePromises = Array.from(selectedFiles).map(fileId => {
      const file = files.find(f => f.id === fileId);
      if (!file) return Promise.resolve();
      return handleDelete(file.filename);
    });

    try {
      await Promise.all(deletePromises);
      setSelectedFiles(new Set());
    } catch (err) {
      setError('Failed to delete some files');
    }
  };

  const handleDownloadSelected = async () => {
    const downloadPromises = Array.from(selectedFiles).map(fileId => {
      const file = files.find(f => f.id === fileId);
      if (!file) return Promise.resolve();
      return downloadFile(file.filename);
    });

    try {
      await Promise.all(downloadPromises);
    } catch (err) {
      setError('Failed to download some files');
    }
  };

  // File selection
  const toggleFileSelection = (fileId: number) => {
    const newSelection = new Set(selectedFiles);
    if (newSelection.has(fileId)) {
      newSelection.delete(fileId);
    } else {
      newSelection.add(fileId);
    }
    setSelectedFiles(newSelection);
  };

  // File upload
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop: useCallback(async (acceptedFiles: Array<File>) => {
      setIsUploading(true);
      setError(null);
      
      for (const file of acceptedFiles) {
        const formData = new FormData();
        formData.append('file', file);
    
        try {
          const xhr = new XMLHttpRequest();
          xhr.upload.addEventListener('progress', (event) => {
            if (event.lengthComputable) {
              const progress = (event.loaded / event.total) * 100;
              setUploadProgress(prev => ({
                ...prev,
                [file.name]: progress
              }));
            }
          });
    
          xhr.onload = async () => {
            if (xhr.status === 200) {
              setUploadProgress(prev => {
                const newProgress = { ...prev };
                delete newProgress[file.name];
                return newProgress;
              });
            } else {
              setError(`Failed to upload ${file.name}`);
            }
          };
    
          xhr.open('POST', 'http://localhost:5000/upload');
          xhr.send(formData);
        } catch (err) {
          setError(`Failed to upload ${file.name}`);
        }
      }
    
      await fetchFiles();
      setIsUploading(false);
    }, [])
  });

  // Utility functions
  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  };

  // Render functions
  const renderHeader = () => (
    <header className="mb-8">
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-bold text-gray-900">Distributed File System</h1>
        {isAdmin && (
          <button
            onClick={onShowAdmin}
            className="flex items-center gap-2 bg-purple-500 text-white px-4 py-2 rounded-lg hover:bg-purple-600"
          >
            <Shield className="h-4 w-4" />
            Admin Dashboard
          </button>
        )}
      </div>
      <div className="mt-4 flex space-x-4">
        <button
          onClick={() => {
            setSelectedTab('files');
            setSelectedFile(null);
          }}
          className={`px-4 py-2 rounded-lg ${
            selectedTab === 'files' ? 'bg-blue-500 text-white' : 'bg-white text-gray-600'
          }`}
        >
          Files
        </button>
        {isAdmin && (
          <>
            <button
              onClick={() => {
                setSelectedTab('nodes');
                setSelectedFile(null);
              }}
              className={`px-4 py-2 rounded-lg ${
                selectedTab === 'nodes' ? 'bg-blue-500 text-white' : 'bg-white text-gray-600'
              }`}
            >
              Node Status
            </button>
            <button
              onClick={() => {
                setSelectedTab('storage');
                setSelectedFile(null);
              }}
              className={`px-4 py-2 rounded-lg ${
                selectedTab === 'storage' ? 'bg-blue-500 text-white' : 'bg-white text-gray-600'
              }`}
            >
              Storage
            </button>
          </>
        )}
      </div>
    </header>
  );

  const renderStatsDashboard = () => (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>System Overview</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="p-4 bg-blue-50 rounded-lg">
            <h4 className="text-sm font-medium text-blue-600">Total Files</h4>
            <p className="text-2xl font-bold">{totalStorage.fileCount}</p>
          </div>
          <div className="p-4 bg-green-50 rounded-lg">
            <h4 className="text-sm font-medium text-green-600">Storage Used</h4>
            <p className="text-2xl font-bold">{formatBytes(totalStorage.used)}</p>
          </div>
          <div className="p-4 bg-purple-50 rounded-lg">
            <h4 className="text-sm font-medium text-purple-600">Storage Capacity</h4>
            <p className="text-2xl font-bold">{formatBytes(totalStorage.total)}</p>
          </div>
          <div className="p-4 bg-orange-50 rounded-lg">
            <h4 className="text-sm font-medium text-orange-600">Usage</h4>
            <p className="text-2xl font-bold">
              {((totalStorage.used / totalStorage.total) * 100).toFixed(1)}%
            </p>
          </div>
        </div>
        <div className="mt-4">
          <div className="flex justify-between mb-1">
            <span className="text-sm text-gray-600">Storage Usage</span>
            <span className="text-sm text-gray-600">
              {formatBytes(totalStorage.used)} of {formatBytes(totalStorage.total)}
            </span>
          </div>
          <Progress
            value={(totalStorage.used / totalStorage.total) * 100}
            className="h-2"
          />
        </div>
      </CardContent>
    </Card>
  );

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-7xl mx-auto">
        {renderHeader()}
        
        {error && (
          <Alert variant="destructive" className="mb-6">
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {selectedTab === 'files' && (
          <div className="space-y-6">
            {renderStatsDashboard()}

            <Card {...getRootProps()} className={`cursor-pointer ${isDragActive ? 'border-blue-500 border-2' : ''}`}>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>File Upload</span>
                  <div className="flex items-center gap-4">
                    {selectedFiles.size > 0 && (
                      <>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDownloadSelected();
                          }}
                          className="bg-blue-500 text-white px-4 py-2 rounded-lg hover:bg-blue-600"
                        >
                          <Download className="h-4 w-4" />
                        </button>
                      </>
                    )}
                  </div>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-center py-8 text-gray-500">
                  {isDragActive ? (
                    <p>Drop the files here ...</p>
                  ) : (
                    <p>Drag and drop files here, or click to select files</p>
                  )}
                  <input {...getInputProps()} />
                </div>
                {Object.entries(uploadProgress).map(([filename, progress]) => (
                  <div key={filename} className="mt-4">
                    <div className="flex justify-between mb-1">
                      <span className="text-sm">{filename}</span>
                      <span className="text-sm">{Math.round(progress)}%</span>
                    </div>
                    <Progress value={progress} className="h-2" />
                  </div>
                ))}
              </CardContent>
            </Card>

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

            <Card>
              <CardHeader>
                <CardTitle>Files</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b">
                        <th className="px-4 py-2 text-left">
                          <input
                            type="checkbox"
                            onChange={(e) => {
                              const newSelection = e.target.checked
                                ? new Set(files.map(f => f.id))
                                : new Set<number>();
                              setSelectedFiles(newSelection);
                            }}
                            checked={selectedFiles.size === files.length}
                          />
                        </th>
                        <th className="px-4 py-2 text-left">Filename</th>
                        <th className="px-4 py-2 text-left">Size</th>
                        <th className="px-4 py-2 text-left">Compressed Size</th>
                        <th className="px-4 py-2 text-left">Location</th>
                        <th className="px-4 py-2 text-left">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredFiles.map((file) => (
                        <tr 
                          key={file.id} 
                          className={`border-b hover:bg-gray-50 ${
                            selectedFile === file.filename ? 'bg-blue-50' : ''
                          }`}
                        >
                          <td className="px-4 py-2">
                            <input
                              type="checkbox"
                              checked={selectedFiles.has(file.id)}
                              onChange={() => toggleFileSelection(file.id)}
                              onClick={(e) => e.stopPropagation()}
                            />
                          </td>
                          <td 
                            className="px-4 py-2 cursor-pointer"
                            onClick={() => setSelectedFile(
                              selectedFile === file.filename ? null : file.filename
                            )}
                          >
                            <div className="flex items-center">
                              <FileText className="h-4 w-4 mr-2 text-gray-400" />
                              {file.filename}
                              {selectedFile === file.filename && (
                                <Clock className="ml-2 h-4 w-4 text-blue-500" />
                              )}
                            </div>
                          </td>
                          <td className="px-4 py-2">{formatBytes(file.size)}</td>
                          <td className="px-4 py-2">{formatBytes(file.compressed_size)}</td>
                          <td className="px-4 py-2">{file.primary_location}</td>
                          <td className="px-4 py-2">
                            <div className="flex space-x-2">
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  downloadFile(file.filename);
                                }}
                                className="text-blue-500 hover:text-blue-600"
                              >
                                <Download className="h-4 w-4" />
                              </button>
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleDelete(file.filename);
                                }}
                                className="text-red-500 hover:text-red-600"
                              >
                                <Trash2 className="h-4 w-4" />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>

            {selectedFile && (
              <VersionHistory
                filename={selectedFile}
                onVersionChange={fetchFiles}
                onClose={() => setSelectedFile(null)}
                className="mt-6"
              />
            )}
          </div>
        )}

        {selectedTab === 'nodes' && (
          <Card>
            <CardHeader>
              <CardTitle>Node Status</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {Object.entries(nodeStatus).map(([nodeName, status]) => (
                  <Card key={nodeName}>
                    <CardContent className="p-6">
                      <div className="flex items-center justify-between mb-4">
                        <Server className="h-6 w-6" />
                        <span className={`px-2 py-1 rounded-full text-sm ${
                          status.status === 'active' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                        }`}>
                          {status.status}
                        </span>
                      </div>
                      <h3 className="text-lg font-semibold mb-2">{nodeName}</h3>
                      <div className="space-y-2 text-sm text-gray-600">
                        <p>Storage Used: {formatBytes(status.storage.used_bytes)}</p>
                        <p>Usage: {status.storage.usage_percent.toFixed(1)}%</p>
                        <div className="w-full bg-gray-200 rounded-full h-2">
                          <div
                            className="bg-blue-500 rounded-full h-2"
                            style={{ width: `${status.storage.usage_percent}%` }}
                          />
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        {selectedTab === 'storage' && (
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
        )}
      </div>
    </div>
  );
}