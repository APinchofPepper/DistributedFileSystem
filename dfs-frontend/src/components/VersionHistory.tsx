import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Download, RotateCcw, Clock } from 'lucide-react';
import { Alert, AlertDescription } from '@/components/ui/alert';

interface Version {
  version: number;
  timestamp: string;
  size: number;
  compressed_size: number;
  hash: string;
  is_current: boolean;
}

interface VersionHistoryProps {
  filename: string;
  onVersionChange: () => void;
  onClose?: () => void;
  className?: string;
}

const VersionHistory: React.FC<VersionHistoryProps> = ({ 
  filename, 
  onVersionChange,
  onClose,
  className = ''
}) => {
  const [versions, setVersions] = useState<Version[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [rollingBack, setRollingBack] = useState(false);

  useEffect(() => {
    fetchVersions();
  }, [filename]);

  const fetchVersions = async () => {
    try {
      setLoading(true);
      const response = await fetch(`http://localhost:5000/versions/${filename}`);
      const data = await response.json();
      
      if (!response.ok) throw new Error(data.error || 'Failed to fetch versions');
      
      setVersions(data.versions);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch versions');
    } finally {
      setLoading(false);
    }
  };

  const downloadVersion = async (version: number) => {
    try {
      const response = await fetch(
        `http://localhost:5000/download/${filename}?version=${version}`
      );
      
      if (!response.ok) throw new Error('Download failed');
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      setError('Failed to download version');
    }
  };

  const rollbackVersion = async (version: number) => {
    try {
      setRollingBack(true);
      const response = await fetch(
        `http://localhost:5000/rollback/${filename}/${version}`,
        { method: 'POST' }
      );
      
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Rollback failed');
      
      await fetchVersions();
      onVersionChange();
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to rollback version');
    } finally {
      setRollingBack(false);
    }
  };

  const formatDate = (timestamp: string) => {
    return new Date(timestamp).toLocaleString();
  };

  const formatSize = (size: number) => {
    const units = ['B', 'KB', 'MB', 'GB'];
    let value = size;
    let unitIndex = 0;
    
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }
    
    return `${value.toFixed(2)} ${units[unitIndex]}`;
  };

  if (loading) {
    return (
      <Card className={className}>
        <CardContent className="p-6">
          <div className="flex items-center justify-center">
            <Clock className="animate-spin h-6 w-6 text-blue-500 mr-2" />
            <span>Loading versions...</span>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <div className="flex items-center">
            <Clock className="mr-2 h-5 w-5" />
            Version History
          </div>
          {onClose && (
            <button
              onClick={onClose}
              className="text-gray-500 hover:text-gray-700"
            >
              ×
            </button>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent>
        {error && (
          <Alert variant="destructive" className="mb-4">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b">
                <th className="px-4 py-2 text-left">Version</th>
                <th className="px-4 py-2 text-left">Timestamp</th>
                <th className="px-4 py-2 text-left">Size</th>
                <th className="px-4 py-2 text-left">Compressed</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody>
              {versions.map((version) => (
                <tr
                  key={version.version}
                  className={`border-b hover:bg-gray-50 ${
                    version.is_current ? 'bg-blue-50' : ''
                  }`}
                >
                  <td className="px-4 py-2">
                    {version.version}
                    {version.is_current && (
                      <span className="ml-2 text-xs bg-blue-100 text-blue-800 px-2 py-1 rounded">
                        Current
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-2">{formatDate(version.timestamp)}</td>
                  <td className="px-4 py-2">{formatSize(version.size)}</td>
                  <td className="px-4 py-2">{formatSize(version.compressed_size)}</td>
                  <td className="px-4 py-2">
                    <div className="flex space-x-2">
                      <button
                        onClick={() => downloadVersion(version.version)}
                        className="text-blue-500 hover:text-blue-600"
                        title="Download this version"
                      >
                        <Download className="h-4 w-4" />
                      </button>
                      {!version.is_current && (
                        <button
                          onClick={() => rollbackVersion(version.version)}
                          className="text-orange-500 hover:text-orange-600"
                          disabled={rollingBack}
                          title="Rollback to this version"
                        >
                          <RotateCcw className={`h-4 w-4 ${rollingBack ? 'animate-spin' : ''}`} />
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
};

export default VersionHistory;