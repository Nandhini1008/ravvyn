'use client';

import React, { useState } from 'react';
import { exportToSheet, exportToDoc, exportChat, ExportToSheetRequest, ExportToDocRequest, ExportChatRequest } from '../lib/api';

interface ExportOptionsProps {
  conversationId?: string;
  data?: any;
  content?: string;
}

export default function ExportOptions({ conversationId, data, content }: ExportOptionsProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const handleExportToSheet = async () => {
    if (!data && !conversationId) {
      setError('No data or conversation to export');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      if (conversationId) {
        const request: ExportChatRequest = {
          conversation_id: conversationId,
          format: 'sheet',
        };
        const result = await exportChat(request);
        setSuccess(`Exported to sheet: ${result.sheet_id}`);
      } else {
        const request: ExportToSheetRequest = {
          data: data || content,
          sheet_name: `Export ${new Date().toISOString().split('T')[0]}`,
        };
        const result = await exportToSheet(request);
        setSuccess(`Exported to sheet: ${result.sheet_id}`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to export to sheet');
    } finally {
      setLoading(false);
    }
  };

  const handleExportToDoc = async () => {
    if (!content && !conversationId) {
      setError('No content or conversation to export');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      if (conversationId) {
        const request: ExportChatRequest = {
          conversation_id: conversationId,
          format: 'doc',
        };
        const result = await exportChat(request);
        setSuccess(`Exported to doc: ${result.doc_id}`);
      } else {
        const request: ExportToDocRequest = {
          content: content || JSON.stringify(data, null, 2),
          doc_name: `Export ${new Date().toISOString().split('T')[0]}`,
        };
        const result = await exportToDoc(request);
        setSuccess(`Exported to doc: ${result.doc_id}`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to export to doc');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-4 bg-gray-50 rounded-lg">
      <h3 className="text-lg font-semibold mb-4">Export Options</h3>
      
      {error && (
        <div className="mb-4 p-3 bg-red-100 text-red-800 rounded-lg text-sm">{error}</div>
      )}
      
      {success && (
        <div className="mb-4 p-3 bg-green-100 text-green-800 rounded-lg text-sm">{success}</div>
      )}

      <div className="flex gap-4">
        <button
          onClick={handleExportToSheet}
          disabled={loading}
          className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
        >
          {loading ? 'Exporting...' : 'Export to Sheet'}
        </button>
        <button
          onClick={handleExportToDoc}
          disabled={loading}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? 'Exporting...' : 'Export to Doc'}
        </button>
      </div>
    </div>
  );
}

