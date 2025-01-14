// src/components/admin/AuditLogs.tsx
import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { 
  Activity, 
  RefreshCw, 
  FileText, 
  Settings, 
  User,
  Calendar,
  Filter,
  Download
} from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';
import { fetchWithAuth } from '@/utils/adminApi';

interface LogEntry {
  id: number;
  timestamp: string;
  type: string;
  description: string;
  user_id?: string;
  details: any;
}

export function AuditLogs() {
  const { adminKey } = useAuth();
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [filters, setFilters] = useState({
    startDate: '',
    endDate: '',
    type: '',
    userId: ''
  });

  useEffect(() => {
    fetchLogs();
  }, [filters]);

  const fetchLogs = async () => {
    try {
      setIsLoading(true);
      const params = new URLSearchParams();
      if (filters.startDate) params.append('startDate', filters.startDate);
      if (filters.endDate) params.append('endDate', filters.endDate);
      if (filters.type) params.append('type', filters.type);
      if (filters.userId) params.append('userId', filters.userId);

      const data = await fetchWithAuth(`/admin/logs?${params}`, adminKey!);
      setLogs(data.logs);
      setError('');
    } catch (err) {
      setError('Failed to fetch audit logs');
    } finally {
      setIsLoading(false);
    }
  };

  const getLogIcon = (type: string) => {
    switch (type) {
      case 'file':
        return <FileText className="h-4 w-4" />;
      case 'system':
        return <Settings className="h-4 w-4" />;
      case 'user':
        return <User className="h-4 w-4" />;
      default:
        return <Activity className="h-4 w-4" />;
    }
  };

  const exportLogs = async () => {
    try {
      const data = await fetchWithAuth('/admin/logs/export', adminKey!);
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `audit_logs_${new Date().toISOString()}.json`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
    } catch (err) {
      setError('Failed to export logs');
    }
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
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Activity className="h-5 w-5" />
              Audit Logs
            </div>
            <button
              onClick={exportLogs}
              className="flex items-center gap-2 bg-blue-500 text-white px-4 py-2 rounded-lg hover:bg-blue-600"
            >
              <Download className="h-4 w-4" />
              Export Logs
            </button>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-6 grid grid-cols-1 md:grid-cols-4 gap-4">
            <div>
              <label className="text-sm font-medium">Start Date</label>
              <Input
                type="date"
                value={filters.startDate}
                onChange={(e) => setFilters(f => ({ ...f, startDate: e.target.value }))}
              />
            </div>
            <div>
              <label className="text-sm font-medium">End Date</label>
              <Input
                type="date"
                value={filters.endDate}
                onChange={(e) => setFilters(f => ({ ...f, endDate: e.target.value }))}
              />
            </div>
            <div>
              <label className="text-sm font-medium">Log Type</label>
              <select
                className="w-full rounded-md border border-input bg-background px-3 py-2"
                value={filters.type}
                onChange={(e) => setFilters(f => ({ ...f, type: e.target.value }))}
              >
                <option value="">All Types</option>
                <option value="file">File Operations</option>
                <option value="system">System Changes</option>
                <option value="user">User Activity</option>
                <option value="security">Security Events</option>
              </select>
            </div>
            <div>
              <label className="text-sm font-medium">User ID</label>
              <Input
                type="text"
                value={filters.userId}
                onChange={(e) => setFilters(f => ({ ...f, userId: e.target.value }))}
                placeholder="Filter by user ID"
              />
            </div>
          </div>

          <div className="space-y-4">
            {logs.map((log) => (
              <div
                key={log.id}
                className="border rounded-lg p-4 hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center gap-2 mb-2">
                  {getLogIcon(log.type)}
                  <span className="font-medium capitalize">{log.type}</span>
                  <span className="text-gray-500">•</span>
                  <span className="text-sm text-gray-500">
                    {new Date(log.timestamp).toLocaleString()}
                  </span>
                  {log.user_id && (
                    <>
                      <span className="text-gray-500">•</span>
                      <span className="text-sm text-gray-500">User: {log.user_id}</span>
                    </>
                  )}
                </div>
                <p className="text-gray-700">{log.description}</p>
                {log.details && (
                  <pre className="mt-2 p-2 bg-gray-100 rounded text-sm overflow-x-auto">
                    {JSON.stringify(log.details, null, 2)}
                  </pre>
                )}
              </div>
            ))}

            {logs.length === 0 && (
              <div className="text-center py-8 text-gray-500">
                No logs found matching the current filters
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )}