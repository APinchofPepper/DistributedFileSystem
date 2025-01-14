// src/components/admin/BackupRestore.tsx
import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { 
  Database,
  RefreshCw,
  Download,
  UploadCloud,
  Calendar,
  CheckCircle,
  XCircle,
  Archive,
  AlertCircle
} from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';
import { fetchWithAuth } from '@/utils/adminApi';

interface Backup {
  id: string;
  timestamp: string;
  size: number;
  type: 'full' | 'incremental';
  status: 'completed' | 'failed';
  retention_days: number;
  metadata: {
    files_count: number;
    nodes_included: string[];
    storage_tiers: string[];
  };
}

export function BackupRestore() {
  const { adminKey } = useAuth();
  const [backups, setBackups] = useState<Backup[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [isCreatingBackup, setIsCreatingBackup] = useState(false);
  const [isRestoring, setIsRestoring] = useState(false);
  const [selectedBackup, setSelectedBackup] = useState<string | null>(null);

  useEffect(() => {
    fetchBackups();
  }, []);

  const fetchBackups = async () => {
    try {
      setIsLoading(true);
      const data = await fetchWithAuth('/admin/backups', adminKey!);
      setBackups(data.backups);
      setError('');
    } catch (err) {
      setError('Failed to fetch backups');
    } finally {
      setIsLoading(false);
    }
  };

  const createBackup = async (type: 'full' | 'incremental') => {
    try {
      setIsCreatingBackup(true);
      setError('');
      setSuccess('');

      const data = await fetchWithAuth('/admin/backups', adminKey!, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type })
      });

      setSuccess(`Successfully initiated ${type} backup`);
      await fetchBackups();
    } catch (err) {
      setError('Failed to create backup');
    } finally {
      setIsCreatingBackup(false);
    }
  };

  const restoreBackup = async (backupId: string) => {
    if (!confirm('Are you sure you want to restore this backup? This will affect the current system state.')) {
      return;
    }

    try {
      setIsRestoring(true);
      setError('');
      setSuccess('');
      setSelectedBackup(backupId);

      await fetchWithAuth(`/admin/backups/${backupId}/restore`, adminKey!, {
        method: 'POST'
      });

      setSuccess('Successfully restored backup');
      await fetchBackups();
    } catch (err) {
      setError('Failed to restore backup');
    } finally {
      setIsRestoring(false);
      setSelectedBackup(null);
    }
  };

  const downloadBackup = async (backupId: string) => {
    try {
      const response = await fetch(`http://localhost:5000/admin/backups/${backupId}/download`, {
        headers: { 'X-Admin-Key': adminKey! }
      });

      if (!response.ok) throw new Error('Download failed');

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `backup_${backupId}.zip`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      setError('Failed to download backup');
    }
  };

  const formatSize = (bytes: number) => {
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unitIndex = 0;

    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }

    return `${size.toFixed(2)} ${units[unitIndex]}`;
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center p-8">
        <RefreshCw className="h-8 w-8 animate-spin text-blue-500" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {success && (
        <Alert>
          <CheckCircle className="h-4 w-4" />
          <AlertDescription>{success}</AlertDescription>
        </Alert>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              Backup & Restore
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => createBackup('incremental')}
                disabled={isCreatingBackup}
                className="flex items-center gap-2 bg-blue-500 text-white px-4 py-2 rounded-lg hover:bg-blue-600 disabled:opacity-50"
              >
                <Archive className="h-4 w-4" />
                Incremental Backup
              </button>
              <button
                onClick={() => createBackup('full')}
                disabled={isCreatingBackup}
                className="flex items-center gap-2 bg-green-500 text-white px-4 py-2 rounded-lg hover:bg-green-600 disabled:opacity-50"
              >
                <Database className="h-4 w-4" />
                Full Backup
              </button>
            </div>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {backups.map((backup) => (
              <div
                key={backup.id}
                className="border rounded-lg p-4 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    {backup.status === 'completed' ? (
                      <CheckCircle className="h-4 w-4 text-green-500" />
                    ) : (
                      <XCircle className="h-4 w-4 text-red-500" />
                    )}
                    <span className="font-medium">
                      {backup.type.charAt(0).toUpperCase() + backup.type.slice(1)} Backup
                    </span>
                    <span className="text-gray-500">•</span>
                    <span className="text-sm text-gray-500">
                      {new Date(backup.timestamp).toLocaleString()}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => downloadBackup(backup.id)}
                      className="text-blue-500 hover:text-blue-600"
                      title="Download backup"
                    >
                      <Download className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => restoreBackup(backup.id)}
                      disabled={isRestoring}
                      className={`text-green-500 hover:text-green-600 ${
                        isRestoring && selectedBackup === backup.id ? 'animate-spin' : ''
                      }`}
                      title="Restore from backup"
                    >
                      <RefreshCw className="h-4 w-4" />
                    </button>
                  </div>
                </div>

                <div className="mt-2 grid grid-cols-1 md:grid-cols-3 gap-4 text-sm text-gray-600">
                  <div>
                    <span className="font-medium">Size:</span> {formatSize(backup.size)}
                  </div>
                  <div>
                    <span className="font-medium">Files:</span> {backup.metadata.files_count}
                  </div>
                  <div>
                    <span className="font-medium">Retention:</span> {backup.retention_days} days
                  </div>
                </div>

                <div className="mt-2 text-sm text-gray-600">
                  <span className="font-medium">Nodes:</span>{' '}
                  {backup.metadata.nodes_included.join(', ')}
                </div>
              </div>
            ))}

            {backups.length === 0 && (
              <div className="text-center py-8 text-gray-500">
                No backups found
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}