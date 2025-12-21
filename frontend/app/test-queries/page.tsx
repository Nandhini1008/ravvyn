"use client";

import { useState } from "react";
import {
  processNaturalLanguageQuery,
  getSheetData,
  getTabsSummary,
} from "@/lib/api";
import { motion } from "framer-motion";
import { Search, Database, BarChart3, Clock, ArrowRight } from "lucide-react";

export default function TestQueriesPage() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sheetData, setSheetData] = useState<any>(null);

  const exampleQueries = [
    "What is the amount on December 12th?",
    "Show me data from RO DETAILS",
    "What is the latest amount?",
    "Total amount for today",
    "Find data at 11:00",
    "Show me data with 473",
    "What is the amount in costing tab?",
    "Latest data from tank dipping",
  ];

  const handleQuery = async () => {
    if (!query.trim()) return;

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await processNaturalLanguageQuery(query);
      setResult(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query failed");
    } finally {
      setLoading(false);
    }
  };

  const loadSheetData = async () => {
    setLoading(true);
    try {
      const data = await getSheetData();
      setSheetData(data);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load sheet data"
      );
    } finally {
      setLoading(false);
    }
  };

  const loadTabsSummary = async () => {
    setLoading(true);
    try {
      const data = await getTabsSummary();
      setSheetData(data);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load tabs summary"
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 p-6">
      <div className="max-w-4xl mx-auto space-y-8">
        {/* Header */}
        <motion.div
          className="text-center space-y-4"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
        >
          <h1 className="text-4xl font-bold text-slate-800">
            Natural Language Query Test
          </h1>
          <p className="text-slate-600">
            Test the natural language query system with your sheet data
          </p>
        </motion.div>

        {/* Query Input */}
        <motion.div
          className="bg-white rounded-xl shadow-lg p-6 space-y-4"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <div className="flex items-center gap-3">
            <Search className="w-5 h-5 text-blue-500" />
            <h2 className="text-xl font-semibold text-slate-800">
              Ask a Question
            </h2>
          </div>

          <div className="space-y-4">
            <div className="flex gap-3">
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleQuery()}
                placeholder="Ask about your data..."
                className="flex-1 px-4 py-3 border border-slate-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              <button
                onClick={handleQuery}
                disabled={loading || !query.trim()}
                className="px-6 py-3 bg-blue-500 text-white rounded-lg hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
              >
                {loading ? (
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                ) : (
                  <ArrowRight className="w-4 h-4" />
                )}
                Query
              </button>
            </div>

            {/* Example Queries */}
            <div className="space-y-2">
              <p className="text-sm text-slate-600">Try these examples:</p>
              <div className="flex flex-wrap gap-2">
                {exampleQueries.map((example, index) => (
                  <button
                    key={index}
                    onClick={() => setQuery(example)}
                    className="px-3 py-1.5 text-xs bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg transition-colors"
                  >
                    {example}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </motion.div>

        {/* Data Actions */}
        <motion.div
          className="bg-white rounded-xl shadow-lg p-6 space-y-4"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
        >
          <div className="flex items-center gap-3">
            <Database className="w-5 h-5 text-green-500" />
            <h2 className="text-xl font-semibold text-slate-800">
              Data Actions
            </h2>
          </div>

          <div className="flex gap-3">
            <button
              onClick={loadSheetData}
              disabled={loading}
              className="px-4 py-2 bg-green-500 text-white rounded-lg hover:bg-green-600 disabled:opacity-50"
            >
              Load Sheet Data
            </button>
            <button
              onClick={loadTabsSummary}
              disabled={loading}
              className="px-4 py-2 bg-purple-500 text-white rounded-lg hover:bg-purple-600 disabled:opacity-50"
            >
              Load Tabs Summary
            </button>
          </div>
        </motion.div>

        {/* Error Display */}
        {error && (
          <motion.div
            className="bg-red-50 border border-red-200 rounded-xl p-4"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <div className="flex items-center gap-3">
              <div className="w-2 h-2 bg-red-500 rounded-full" />
              <p className="text-red-700 font-medium">Error</p>
            </div>
            <p className="text-red-600 mt-2">{error}</p>
          </motion.div>
        )}

        {/* Query Result */}
        {result && (
          <motion.div
            className="bg-white rounded-xl shadow-lg p-6 space-y-4"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <div className="flex items-center gap-3">
              <BarChart3 className="w-5 h-5 text-blue-500" />
              <h2 className="text-xl font-semibold text-slate-800">
                Query Result
              </h2>
              <span
                className={`px-2 py-1 text-xs rounded-full ${
                  result.success
                    ? "bg-green-100 text-green-700"
                    : "bg-red-100 text-red-700"
                }`}
              >
                {result.success ? "Success" : "Failed"}
              </span>
            </div>

            <div className="space-y-4">
              {/* Query Info */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 p-4 bg-slate-50 rounded-lg">
                <div>
                  <p className="text-sm text-slate-600">Query Type</p>
                  <p className="font-medium text-slate-800">
                    {result.query_type}
                  </p>
                </div>
                <div>
                  <p className="text-sm text-slate-600">Confidence</p>
                  <p className="font-medium text-slate-800">
                    {Math.round(result.confidence * 100)}%
                  </p>
                </div>
                <div>
                  <p className="text-sm text-slate-600">Data Found</p>
                  <p className="font-medium text-slate-800">
                    {result.data_found} points
                  </p>
                </div>
              </div>

              {/* Answer */}
              <div className="space-y-2">
                <p className="text-sm text-slate-600">Answer:</p>
                <div className="p-4 bg-blue-50 rounded-lg">
                  <p className="text-slate-800 whitespace-pre-wrap">
                    {result.answer}
                  </p>
                </div>
              </div>

              {/* Supporting Data */}
              {result.supporting_data && result.supporting_data.length > 0 && (
                <div className="space-y-2">
                  <p className="text-sm text-slate-600">Supporting Data:</p>
                  <div className="space-y-2">
                    {result.supporting_data.map((item: any, index: number) => (
                      <div key={index} className="p-3 bg-slate-50 rounded-lg">
                        <div className="flex items-center gap-2 mb-2">
                          <span className="text-sm font-medium text-slate-700">
                            {item.tab_name}
                          </span>
                          <span className="text-xs text-slate-500">
                            Row {item.row_index}
                          </span>
                        </div>
                        <div className="text-sm text-slate-600">
                          {Array.isArray(item.data)
                            ? item.data.filter(Boolean).join(", ")
                            : JSON.stringify(item.data)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Suggestions */}
              {result.suggestions && result.suggestions.length > 0 && (
                <div className="space-y-2">
                  <p className="text-sm text-slate-600">Suggestions:</p>
                  <ul className="space-y-1">
                    {result.suggestions.map(
                      (suggestion: string, index: number) => (
                        <li
                          key={index}
                          className="text-sm text-slate-700 flex items-center gap-2"
                        >
                          <div className="w-1 h-1 bg-slate-400 rounded-full" />
                          {suggestion}
                        </li>
                      )
                    )}
                  </ul>
                </div>
              )}
            </div>
          </motion.div>
        )}

        {/* Sheet Data Display */}
        {sheetData && (
          <motion.div
            className="bg-white rounded-xl shadow-lg p-6 space-y-4"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
          >
            <div className="flex items-center gap-3">
              <Clock className="w-5 h-5 text-purple-500" />
              <h2 className="text-xl font-semibold text-slate-800">
                Sheet Data
              </h2>
            </div>

            <div className="space-y-4">
              {/* Sheet Info */}
              {sheetData.sheet_info && (
                <div className="p-4 bg-slate-50 rounded-lg">
                  <h3 className="font-medium text-slate-800 mb-2">
                    Sheet Information
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                    <div>
                      <span className="text-slate-600">Name: </span>
                      <span className="text-slate-800">
                        {sheetData.sheet_info.sheet_name}
                      </span>
                    </div>
                    <div>
                      <span className="text-slate-600">Last Synced: </span>
                      <span className="text-slate-800">
                        {sheetData.sheet_info.last_synced
                          ? new Date(
                              sheetData.sheet_info.last_synced
                            ).toLocaleString()
                          : "Never"}
                      </span>
                    </div>
                  </div>
                </div>
              )}

              {/* Data Summary */}
              {sheetData.data_summary && (
                <div className="p-4 bg-slate-50 rounded-lg">
                  <h3 className="font-medium text-slate-800 mb-2">
                    Data Summary
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                    <div>
                      <span className="text-slate-600">Total Rows: </span>
                      <span className="text-slate-800">
                        {sheetData.data_summary.total_rows_retrieved || 0}
                      </span>
                    </div>
                    <div>
                      <span className="text-slate-600">Tabs Found: </span>
                      <span className="text-slate-800">
                        {sheetData.data_summary.tabs_found?.length || 0}
                      </span>
                    </div>
                    <div>
                      <span className="text-slate-600">Tabs: </span>
                      <span className="text-slate-800">
                        {sheetData.data_summary.tabs_found?.join(", ") ||
                          "None"}
                      </span>
                    </div>
                  </div>
                </div>
              )}

              {/* Raw Data Preview */}
              {sheetData.data && sheetData.data.length > 0 && (
                <div className="space-y-2">
                  <h3 className="font-medium text-slate-800">
                    Data Preview (First 5 rows)
                  </h3>
                  <div className="space-y-2 max-h-96 overflow-y-auto">
                    {sheetData.data
                      .slice(0, 5)
                      .map((row: any, index: number) => (
                        <div key={index} className="p-3 bg-slate-50 rounded-lg">
                          <div className="flex items-center gap-2 mb-2">
                            <span className="text-sm font-medium text-slate-700">
                              {row.tab_name}
                            </span>
                            <span className="text-xs text-slate-500">
                              Row {row.row_index}
                            </span>
                          </div>
                          <div className="text-sm text-slate-600">
                            {row.non_empty_values?.join(", ") || "No data"}
                          </div>
                        </div>
                      ))}
                  </div>
                </div>
              )}
            </div>
          </motion.div>
        )}
      </div>
    </div>
  );
}
