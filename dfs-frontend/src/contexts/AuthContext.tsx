// src/contexts/AuthContext.tsx
import React, { createContext, useContext, useState, useEffect } from 'react';

interface AuthContextType {
  isAuthenticated: boolean;
  isAdmin: boolean;
  adminKey: string | null;
  login: (key: string) => Promise<boolean>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const [adminKey, setAdminKey] = useState<string | null>(null);

  useEffect(() => {
    // Check for saved admin key in localStorage
    const savedKey = localStorage.getItem('adminKey');
    if (savedKey) {
      setAdminKey(savedKey);
      setIsAuthenticated(true);
      setIsAdmin(true);
    }
  }, []);

  const login = async (key: string) => {
    try {
      // Verify admin key with backend using an existing endpoint
      const response = await fetch('http://localhost:5000/admin/nodes/health', {
        headers: {
          'X-Admin-Key': key
        }
      });

      if (response.ok) {
        setAdminKey(key);
        setIsAuthenticated(true);
        setIsAdmin(true);
        localStorage.setItem('adminKey', key);
        return true;
      }
      return false;
    } catch (error) {
      console.error('Authentication error:', error);
      return false;
    }
  };

  const logout = () => {
    setAdminKey(null);
    setIsAuthenticated(false);
    setIsAdmin(false);
    localStorage.removeItem('adminKey');
  };

  return (
    <AuthContext.Provider value={{ isAuthenticated, isAdmin, adminKey, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}