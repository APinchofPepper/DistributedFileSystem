// src/components/admin/SystemConfig.tsx
import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { HardDrive, RefreshCw, Server } from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';

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

export function SystemConfig() {
  const { adminKey } = useAuth();
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [storageTiers, setStorageTiers] = useState<StorageTier[]>([]);
  const [systemStats, setSystemStats] = useState<SystemStats>({
    total_files: 0,
    total_storage: 0,
    used_storage: 0,
    node_count: 3
  });

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 10000); // Refresh every 10 seconds
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      setIsLoading(true);
      const response = await fetch('http://localhost:5000/admin/storage/tiers', {
        headers: {
          'X-Admin-Key': adminKey!
        }
      });

      if (!response.ok) throw new Error('Failed to fetch storage tiers');
      
      const data = await response.json();
      setStorageTiers(data.tiers || []);

      // Calculate system stats from tiers
      const stats = data.tiers.reduce((acc: Partial<SystemStats>, tier: StorageTier) => ({
        total_files: (acc.total_files || 0) + tier.file_count,
        total_storage: (acc.total_storage || 0) + tier.total_size,
        used_storage: (acc.used_storage || 0) + tier.total_compressed_size
      }), {});

      setSystemStats({
        ...stats,
        node_count: 3,
      } as SystemStats);

      setError('');
    } catch (err) {
      setError('Failed to fetch system configuration');
    } finally {
      setIsLoading(false);
    }
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
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
          <CardTitle className="flex items-center gap-2">
            <Server className="h-5 w-5" />
            System Overview
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="p-4 bg-blue-50 rounded-lg">
              <h3 className="text-lg font-semibold mb-4">Storage Statistics</h3>
              <div className="space-y-2">
                <p className="flex justify-between">
                  <span className="text-gray-600">Total Files:</span>
                  <span className="font-medium">{systemStats.total_files}</span>
                </p>
                <p className="flex justify-between">
                  <span className="text-gray-600">Storage Used:</span>
                  <span className="font-medium">{formatBytes(systemStats.used_storage)}</span>
                </p>
                <p className="flex justify-between">
                  <span className="text-gray-600">Storage Capacity:</span>
                  <span className="font-medium">{formatBytes(systemStats.total_storage)}</span>
                </p>
                <p className="flex justify-between">
                  <span className="text-gray-600">Usage:</span>
                  <span className="font-medium">
                    {((systemStats.used_storage / systemStats.total_storage) * 100).toFixed(1)}%
                  </span>
                </p>
              </div>
            </div>
            <div className="p-4 bg-green-50 rounded-lg">
              <h3 className="text-lg font-semibold mb-4">Node Information</h3>
              <div className="space-y-2">
                <p className="flex justify-between">
                  <span className="text-gray-600">Total Nodes:</span>
                  <span className="font-medium">{systemStats.node_count}</span>
                </p>
                <p className="flex justify-between">
                  <span className="text-gray-600">Storage Per Node:</span>
                  <span className="font-medium">
                    {formatBytes(systemStats.total_storage / systemStats.node_count)}
                  </span>
                </p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <HardDrive className="h-5 w-5" />
            Storage Tiers
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {storageTiers.map((tier) => (
              <div key={tier.name} className="p-4 bg-white rounded-lg shadow">
                <h3 className="text-lg font-semibold mb-4 capitalize">{tier.name} Storage</h3>
                <div className="space-y-2">
                  <p className="flex justify-between">
                    <span className="text-gray-600">Files:</span>
                    <span className="font-medium">{tier.file_count}</span>
                  </p>
                  <p className="flex justify-between">
                    <span className="text-gray-600">Total Size:</span>
                    <span className="font-medium">{formatBytes(tier.total_size)}</span>
                  </p>
                  <p className="flex justify-between">
                    <span className="text-gray-600">Compressed:</span>
                    <span className="font-medium">{formatBytes(tier.total_compressed_size)}</span>
                  </p>
                  <p className="flex justify-between">
                    <span className="text-gray-600">Savings:</span>
                    <span className="font-medium">
                      {((1 - tier.total_compressed_size / tier.total_size) * 100).toFixed(1)}%
                    </span>
                  </p>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}