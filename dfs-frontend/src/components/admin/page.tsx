// src/app/admin/page.tsx
'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '@/contexts/AuthContext';
import { AdminDashboard } from '@/components/admin/AdminDashboard';

export default function AdminPage() {
  const { isAuthenticated, isAdmin } = useAuth();
  const router = useRouter();

  useEffect(() => {
    if (!isAuthenticated || !isAdmin) {
      router.push('/');
    }
  }, [isAuthenticated, isAdmin, router]);

  if (!isAuthenticated || !isAdmin) {
    return null;
  }

  // Add handler for returning to the main file manager
  const handleReturn = () => {
    router.push('/');
  };

  return <AdminDashboard onReturn={handleReturn} />;
}