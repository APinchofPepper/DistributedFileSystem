// src/components/dfs-frontend.tsx
'use client';

import React from 'react';
import { AuthProvider, useAuth } from '@/contexts/AuthContext';
import { Login } from '@/components/Login';
import { AdminDashboard } from '@/components/admin/AdminDashboard';
import { FileManager } from '@/components/FileManager';

// Wrapper component that provides auth context
export function DFSFrontend() {
  return (
    <AuthProvider>
      <DFSFrontendContent />
    </AuthProvider>
  );
}

// Inner component that uses auth context
function DFSFrontendContent() {
  const { isAuthenticated, isAdmin } = useAuth();
  const [showAdmin, setShowAdmin] = React.useState(false);

  // Show login if not authenticated
  if (!isAuthenticated) {
    return <Login />;
  }

  // Show admin dashboard if in admin mode
  if (showAdmin && isAdmin) {
    return <AdminDashboard onReturn={() => setShowAdmin(false)} />;
  }

  // Show file manager by default
  return <FileManager onShowAdmin={() => setShowAdmin(true)} />;
}