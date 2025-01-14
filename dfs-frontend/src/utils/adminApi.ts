// src/utils/adminApi.ts
const BASE_URL = 'http://localhost:5000';

export async function fetchWithAuth(endpoint: string, adminKey: string, options: RequestInit = {}) {
  const headers = {
    ...options.headers,
    'X-Admin-Key': adminKey
  };

  const response = await fetch(`${BASE_URL}${endpoint}`, {
    ...options,
    headers
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Unknown error' }));
    throw new Error(error.error || 'Request failed');
  }

  return response.json();
}

export async function verifyAdminKey(key: string): Promise<boolean> {
  try {
    const response = await fetch(`${BASE_URL}/admin/verify`, {
      headers: {
        'X-Admin-Key': key
      }
    });
    return response.ok;
  } catch {
    return false;
  }
}

export async function fetchAuditLogs(adminKey: string, options: {
  startDate?: string;
  endDate?: string;
  type?: string;
  limit?: number;
} = {}) {
  const params = new URLSearchParams();
  if (options.startDate) params.append('startDate', options.startDate);
  if (options.endDate) params.append('endDate', options.endDate);
  if (options.type) params.append('type', options.type);
  if (options.limit) params.append('limit', options.limit.toString());

  return fetchWithAuth(`/admin/logs?${params}`, adminKey);
}

export async function createBackup(adminKey: string) {
  return fetchWithAuth('/admin/backup', adminKey, {
    method: 'POST'
  });
}

export async function restoreBackup(adminKey: string, backupId: string) {
  return fetchWithAuth(`/admin/backup/${backupId}/restore`, adminKey, {
    method: 'POST'
  });
}

export async function updateSystemConfig(adminKey: string, config: any) {
  return fetchWithAuth('/admin/config', adminKey, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(config)
  });
}

export async function fetchNodeHealth(adminKey: string) {
  return fetchWithAuth('/admin/nodes/health', adminKey);
}

export async function fetchStorageTiers(adminKey: string) {
  return fetchWithAuth('/admin/storage/tiers', adminKey);
}

export async function updateStorageTier(adminKey: string, tierName: string, config: any) {
  return fetchWithAuth(`/admin/storage/tiers/${tierName}`, adminKey, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(config)
  });
}

export async function fetchUserActivity(adminKey: string, userId?: string) {
  const params = new URLSearchParams();
  if (userId) params.append('userId', userId);
  
  return fetchWithAuth(`/admin/users/activity?${params}`, adminKey);
}

export async function archiveFile(adminKey: string, fileId: number) {
  return fetchWithAuth(`/admin/files/${fileId}/archive`, adminKey, {
    method: 'POST'
  });
}

export async function restoreFile(adminKey: string, fileId: number) {
  return fetchWithAuth(`/admin/files/${fileId}/restore`, adminKey, {
    method: 'POST'
  });
}