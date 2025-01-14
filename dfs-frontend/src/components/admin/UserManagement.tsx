// src/components/admin/UserManagement.tsx
import React, { useState, useEffect } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Alert, AlertDescription } from '@/components/ui/alert';
import {
  UserPlus,
  RefreshCw,
  Shield,
  User,
  Key,
  Edit,
  Trash2,
  CheckCircle,
  XCircle,
  Search
} from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';
import { fetchWithAuth } from '@/utils/adminApi';

interface UserData {
  id: string;
  username: string;
  role: 'admin' | 'user';
  status: 'active' | 'inactive';
  last_login?: string;
  created_at: string;
  permissions: string[];
}

export function UserManagement() {
  const { adminKey } = useAuth();
  const [users, setUsers] = useState<UserData[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [editingUser, setEditingUser] = useState<UserData | null>(null);
  interface NewUser {
    username: string;
    role: 'admin' | 'user';
    permissions: string[];
  }

  const [newUser, setNewUser] = useState<NewUser>({
    username: '',
    role: 'user',
    permissions: []
  });
  const [showNewUserForm, setShowNewUserForm] = useState(false);

  useEffect(() => {
    fetchUsers();
  }, []);

  const fetchUsers = async () => {
    try {
      setIsLoading(true);
      const data = await fetchWithAuth('/admin/users', adminKey!);
      setUsers(data.users);
      setError('');
    } catch (err) {
      setError('Failed to fetch users');
    } finally {
      setIsLoading(false);
    }
  };

  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setError('');
      setSuccess('');
      
      await fetchWithAuth('/admin/users', adminKey!, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newUser)
      });

      setSuccess('User created successfully');
      setShowNewUserForm(false);
      setNewUser({
        username: '',
        role: 'user',
        permissions: []
      });
      await fetchUsers();
    } catch (err) {
      setError('Failed to create user');
    }
  };

  const handleUpdateUser = async (userId: string, updates: Partial<UserData>) => {
    try {
      setError('');
      setSuccess('');

      await fetchWithAuth(`/admin/users/${userId}`, adminKey!, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
      });

      setSuccess('User updated successfully');
      setEditingUser(null);
      await fetchUsers();
    } catch (err) {
      setError('Failed to update user');
    }
  };

  const handleDeleteUser = async (userId: string) => {
    if (!confirm('Are you sure you want to delete this user?')) {
      return;
    }

    try {
      setError('');
      setSuccess('');

      await fetchWithAuth(`/admin/users/${userId}`, adminKey!, {
        method: 'DELETE'
      });

      setSuccess('User deleted successfully');
      await fetchUsers();
    } catch (err) {
      setError('Failed to delete user');
    }
  };

  const filteredUsers = users.filter(user =>
    user.username.toLowerCase().includes(searchQuery.toLowerCase())
  );

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
              <User className="h-5 w-5" />
              User Management
            </div>
            <button
              onClick={() => setShowNewUserForm(!showNewUserForm)}
              className="flex items-center gap-2 bg-blue-500 text-white px-4 py-2 rounded-lg hover:bg-blue-600"
            >
              <UserPlus className="h-4 w-4" />
              Add User
            </button>
          </CardTitle>
        </CardHeader>
        <CardContent>
          {showNewUserForm && (
            <form onSubmit={handleCreateUser} className="mb-6 p-4 border rounded-lg">
              <h3 className="text-lg font-semibold mb-4">Create New User</h3>
              <div className="space-y-4">
                <div>
                  <label className="text-sm font-medium">Username</label>
                  <Input
                    value={newUser.username}
                    onChange={(e) => setNewUser(u => ({ ...u, username: e.target.value }))}
                    required
                  />
                </div>
                <div>
                  <label className="text-sm font-medium">Role</label>
                  <select
                    value={newUser.role}
                    onChange={(e) => setNewUser((prevUser) => ({
                      ...prevUser,
                      role: e.target.value === 'admin' ? 'admin' : 'user'
                    }))}
                    className="w-full rounded-md border border-input bg-background px-3 py-2"
                  >
                    <option value="user">User</option>
                    <option value="admin">Admin</option>
                  </select>
                </div>
                <div className="flex gap-2">
                  <button
                    type="submit"
                    className="bg-green-500 text-white px-4 py-2 rounded-lg hover:bg-green-600"
                  >
                    Create User
                  </button>
                  <button
                    type="button"
                    onClick={() => setShowNewUserForm(false)}
                    className="bg-gray-500 text-white px-4 py-2 rounded-lg hover:bg-gray-600"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            </form>
          )}

          <div className="mb-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <Input
                type="text"
                placeholder="Search users..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10"
              />
            </div>
          </div>

          <div className="space-y-4">
            {filteredUsers.map((user) => (
              <div
                key={user.id}
                className="border rounded-lg p-4 hover:bg-gray-50 transition-colors"
              >
                {editingUser?.id === user.id ? (
                  <div className="space-y-4">
                    <Input
                      value={editingUser.username}
                      onChange={(e) => setEditingUser(u => u ? { ...u, username: e.target.value } : null)}
                    />
                    <select
                      value={editingUser.role}
                      onChange={(e) => setEditingUser(u => u ? { ...u, role: e.target.value as 'admin' | 'user' } : null)}
                      className="w-full rounded-md border border-input bg-background px-3 py-2"
                    >
                      <option value="user">User</option>
                      <option value="admin">Admin</option>
                    </select>
                    <div className="flex gap-2">
                      <button
                        onClick={() => handleUpdateUser(user.id, editingUser)}
                        className="bg-green-500 text-white px-4 py-2 rounded-lg hover:bg-green-600"
                      >
                        Save
                      </button>
                      <button
                        onClick={() => setEditingUser(null)}
                        className="bg-gray-500 text-white px-4 py-2 rounded-lg hover:bg-gray-600"
                      >
                        Cancel
                      </button>
                    </div>
                  </div>
                ) : (
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{user.username}</span>
                        {user.role === 'admin' && (
                          <Shield className="h-4 w-4 text-purple-500" />
                        )}
                        <span className={`px-2 py-1 rounded-full text-xs ${
                          user.status === 'active' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                        }`}>
                          {user.status}
                        </span>
                      </div>
                      <div className="text-sm text-gray-500 mt-1">
                        Created: {new Date(user.created_at).toLocaleDateString()}
                        {user.last_login && ` • Last login: ${new Date(user.last_login).toLocaleDateString()}`}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => setEditingUser(user)}
                        className="text-blue-500 hover:text-blue-600"
                      >
                        <Edit className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => handleDeleteUser(user.id)}
                        className="text-red-500 hover:text-red-600"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </div>
                )}
              </div>
            ))}

            {filteredUsers.length === 0 && (
              <div className="text-center py-8 text-gray-500">
                No users found
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}