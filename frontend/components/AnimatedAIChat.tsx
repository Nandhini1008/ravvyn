"use client";

import { useEffect, useRef, useCallback, useTransition } from "react";
import { useState } from "react";
import { cn } from "@/lib/utils";
import {
  ImageIcon,
  Figma,
  MonitorIcon,
  Paperclip,
  SendIcon,
  XIcon,
  LoaderIcon,
  Sparkles,
  Command,
  RefreshCw,
} from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import * as React from "react";
import { MessageFormatter } from "./MessageFormatter";
import {
  sendChatMessage,
  syncSheets,
  getSyncStatus,
  processNaturalLanguageQuery,
  getETPTankCapacity,
  exportQueryResults,
  type ETPTankCapacityResponse,
} from "@/lib/api";

interface UseAutoResizeTextareaProps {
  minHeight: number;
  maxHeight?: number;
}

function useAutoResizeTextarea({
  minHeight,
  maxHeight,
}: UseAutoResizeTextareaProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const adjustHeight = useCallback(
    (reset?: boolean) => {
      const textarea = textareaRef.current;
      if (!textarea) return;

      if (reset) {
        textarea.style.height = `${minHeight}px`;
        return;
      }

      textarea.style.height = `${minHeight}px`;
      const newHeight = Math.max(
        minHeight,
        Math.min(textarea.scrollHeight, maxHeight ?? Number.POSITIVE_INFINITY)
      );

      textarea.style.height = `${newHeight}px`;
    },
    [minHeight, maxHeight]
  );

  useEffect(() => {
    const textarea = textareaRef.current;
    if (textarea) {
      textarea.style.height = `${minHeight}px`;
    }
  }, [minHeight]);

  useEffect(() => {
    const handleResize = () => adjustHeight();

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, [adjustHeight]);

  return { textareaRef, adjustHeight };
}

interface CommandSuggestion {
  icon: React.ReactNode;
  label: string;
  description: string;
  prefix: string;
}

interface TextareaProps
  extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  containerClassName?: string;
  showRing?: boolean;
}

const Textarea = React.forwardRef<HTMLTextAreaElement, TextareaProps>(
  ({ className, containerClassName, showRing = true, ...props }, ref) => {
    const [isFocused, setIsFocused] = React.useState(false);

    return (
      <div className={cn("relative", containerClassName)}>
        <textarea
          className={cn(
            "border-input bg-background flex min-h-[80px] w-full rounded-md border px-3 py-2 text-sm",
            "transition-all duration-200 ease-in-out",
            "placeholder:text-muted-foreground",
            "disabled:cursor-not-allowed disabled:opacity-50",
            showRing
              ? "focus-visible:ring-0 focus-visible:ring-offset-0 focus-visible:outline-none"
              : "",
            className
          )}
          ref={ref}
          onFocus={() => setIsFocused(true)}
          onBlur={() => setIsFocused(false)}
          {...props}
        />
        {showRing && isFocused && (
          <motion.span
            className="ring-primary/30 pointer-events-none absolute inset-0 rounded-md ring-2 ring-offset-0"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
          />
        )}
        {props.onChange && (
          <div
            className="bg-primary absolute right-2 bottom-2 h-2 w-2 rounded-full opacity-0"
            style={{
              animation: "none",
            }}
            id="textarea-ripple"
          />
        )}
      </div>
    );
  }
);

Textarea.displayName = "Textarea";

export default function AnimatedAIChat() {
  const [value, setValue] = useState("");
  const [attachments, setAttachments] = useState<string[]>([]);
  const [isTyping, setIsTyping] = useState(false);
  const [isPending, startTransition] = useTransition();
  const [activeSuggestion, setActiveSuggestion] = useState<number>(-1);
  const [showCommandPalette, setShowCommandPalette] = useState(false);
  const [recentCommand, setRecentCommand] = useState<string | null>(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });
  const [messages, setMessages] = useState<
    Array<{ 
      role: "user" | "assistant"; 
      content: string;
      queryData?: {
        query: string;
        raw_data?: any;
        formatted_response?: string;
      };
    }>
  >([]);
  const [error, setError] = useState<string | null>(null);
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncSuccess, setSyncSuccess] = useState<string | null>(null);
  const [showETPModal, setShowETPModal] = useState(false);
  const [etpDate, setEtpDate] = useState("");
  const [etpInletValue, setEtpInletValue] = useState("");
  const [etpData, setEtpData] = useState<ETPTankCapacityResponse | null>(null);
  const [isLoadingETP, setIsLoadingETP] = useState(false);
  const [exportingToSheets, setExportingToSheets] = useState<number | null>(null);
  const [exportSuccess, setExportSuccess] = useState<{index: number; url: string} | null>(null);
  const isMountedRef = useRef(true);
  const abortControllerRef = useRef<AbortController | null>(null);

  const { textareaRef, adjustHeight } = useAutoResizeTextarea({
    minHeight: 60,
    maxHeight: 200,
  });

  const [inputFocused, setInputFocused] = useState(false);
  const commandPaletteRef = useRef<HTMLDivElement>(null);

  // Cleanup on unmount
  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, []);

  const commandSuggestions: CommandSuggestion[] = React.useMemo(
    () => [
      {
        icon: <ImageIcon className="h-4 w-4" />,
        label: "Clone UI",
        description: "Generate a UI from a screenshot",
        prefix: "/clone",
      },
      {
        icon: <Figma className="h-4 w-4" />,
        label: "Import Figma",
        description: "Import a design from Figma",
        prefix: "/figma",
      },
      {
        icon: <MonitorIcon className="h-4 w-4" />,
        label: "Create Page",
        description: "Generate a new web page",
        prefix: "/page",
      },
      {
        icon: <Sparkles className="h-4 w-4" />,
        label: "Improve",
        description: "Improve existing UI design",
        prefix: "/improve",
      },
    ],
    []
  );

  useEffect(() => {
    if (value.startsWith("/") && !value.includes(" ")) {
      setShowCommandPalette(true);
      const matchingSuggestionIndex = commandSuggestions.findIndex((cmd) =>
        cmd.prefix.startsWith(value)
      );

      if (matchingSuggestionIndex >= 0) {
        setActiveSuggestion(matchingSuggestionIndex);
      } else {
        setActiveSuggestion(-1);
      }
    } else {
      setShowCommandPalette(false);
    }
  }, [value, commandSuggestions]);

  useEffect(() => {
    let isMounted = true;

    const handleMouseMove = (e: MouseEvent) => {
      if (isMounted) {
        setMousePosition({ x: e.clientX, y: e.clientY });
      }
    };

    window.addEventListener("mousemove", handleMouseMove);
    return () => {
      isMounted = false;
      window.removeEventListener("mousemove", handleMouseMove);
    };
  }, []);

  useEffect(() => {
    let isMounted = true;

    const handleClickOutside = (event: MouseEvent) => {
      if (!isMounted) return;

      const target = event.target as Node;
      const commandButton = document.querySelector("[data-command-button]");

      if (
        commandPaletteRef.current &&
        !commandPaletteRef.current.contains(target) &&
        !commandButton?.contains(target)
      ) {
        setShowCommandPalette(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      isMounted = false;
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (showCommandPalette) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveSuggestion((prev) =>
          prev < commandSuggestions.length - 1 ? prev + 1 : 0
        );
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveSuggestion((prev) =>
          prev > 0 ? prev - 1 : commandSuggestions.length - 1
        );
      } else if (e.key === "Tab" || e.key === "Enter") {
        e.preventDefault();
        if (activeSuggestion >= 0) {
          const selectedCommand = commandSuggestions[activeSuggestion];
          setValue(selectedCommand.prefix + " ");
          setShowCommandPalette(false);
          setRecentCommand(selectedCommand.label);
          setTimeout(() => setRecentCommand(null), 3500);
        }
      } else if (e.key === "Escape") {
        e.preventDefault();
        setShowCommandPalette(false);
      }
    } else if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (value.trim()) {
        handleSendMessage();
      }
    }
  };

  const handleSendMessage = async () => {
    if (!value.trim() || isTyping || !isMountedRef.current) return;

    const userMessage = value.trim();
    setValue("");
    adjustHeight(true);
    setError(null);

    // Cancel any pending request
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    // Create new abort controller for this request
    abortControllerRef.current = new AbortController();

    // Add user message to chat
    if (isMountedRef.current) {
      setMessages((prev) => [...prev, { role: "user", content: userMessage }]);
      setIsTyping(true);
    }

    startTransition(async () => {
      try {
        // Detect if this is a data query using simple heuristics
        const isDataQuery =
          /\b(what|show|find|amount|data|value|total|how much|when|where|latest|recent|december|january|time|date|ro details|costing|tank|running)\b/i.test(
            userMessage
          ) ||
          /\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/.test(userMessage) || // Date patterns
          /\d{1,2}:\d{2}/.test(userMessage) || // Time patterns
          /\b\d+\b/.test(userMessage); // Numbers

        let response;

        if (isDataQuery) {
          // Use natural language query processor for data queries
          try {
            const queryResult = await processNaturalLanguageQuery(userMessage);

            if (queryResult.success && queryResult.answer) {
              let responseText = queryResult.answer;

              // Add confidence indicator if low
              if (queryResult.confidence < 0.7) {
                responseText += `\n\n💡 *Confidence: ${Math.round(
                  queryResult.confidence * 100
                )}%*`;
              }

              // Add data source info
              if (queryResult.data_found > 0) {
                responseText += `\n\n📊 *Found ${queryResult.data_found} relevant data points*`;
              }

              // Add suggestions if available
              if (
                queryResult.suggestions &&
                queryResult.suggestions.length > 0
              ) {
                responseText += `\n\n💡 **You might also try:**\n${queryResult.suggestions
                  .slice(0, 3)
                  .map((s) => `• ${s}`)
                  .join("\n")}`;
              }

              response = { 
                response: responseText, 
                type: "data_query",
                queryData: {
                  query: userMessage,
                  raw_data: queryResult.raw_data,
                  formatted_response: responseText
                }
              };
            } else {
              // Fallback to regular chat if query processing failed
              response = await sendChatMessage(userMessage);
            }
          } catch (queryError) {
            console.warn(
              "Natural language query failed, falling back to regular chat:",
              queryError
            );
            // Fallback to regular chat
            response = await sendChatMessage(userMessage);
          }
        } else {
          // Use regular chat for non-data queries
          response = await sendChatMessage(userMessage);
        }

        // Only update state if component is still mounted
        if (
          isMountedRef.current &&
          !abortControllerRef.current?.signal.aborted
        ) {
          setMessages((prev) => [
            ...prev,
            { 
              role: "assistant", 
              content: response.response,
              queryData: (response as any).queryData
            },
          ]);
        }
      } catch (err) {
        // Only update state if component is still mounted and not aborted
        if (
          isMountedRef.current &&
          !abortControllerRef.current?.signal.aborted
        ) {
          const errorMessage =
            err instanceof Error ? err.message : "Failed to send message";
          setError(errorMessage);
          setMessages((prev) => [
            ...prev,
            {
              role: "assistant",
              content: `Sorry, I encountered an error: ${errorMessage}`,
            },
          ]);
        }
      } finally {
        if (isMountedRef.current) {
          setIsTyping(false);
        }
      }
    });
  };

  const handleAttachFile = () => {
    const mockFileName = `file-${Math.floor(Math.random() * 1000)}.pdf`;
    setAttachments((prev) => [...prev, mockFileName]);
  };

  const removeAttachment = (index: number) => {
    setAttachments((prev) => prev.filter((_, i) => i !== index));
  };

  const selectCommandSuggestion = (index: number) => {
    const selectedCommand = commandSuggestions[index];
    setValue(selectedCommand.prefix + " ");
    setShowCommandPalette(false);
    setRecentCommand(selectedCommand.label);
    setTimeout(() => setRecentCommand(null), 2000);
  };

  const handleSync = async () => {
    setIsSyncing(true);
    setError(null);
    setSyncSuccess(null);

    try {
      await syncSheets(true);
      setSyncSuccess("✓ Synced to latest data");
      setTimeout(() => setSyncSuccess(null), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Sync failed");
      setTimeout(() => setError(null), 5000);
    } finally {
      setIsSyncing(false);
    }
  };

  const handleExportToSheets = async (messageIndex: number, queryData: { query: string; raw_data?: any; formatted_response?: string }) => {
    if (!queryData.raw_data) {
      setError("No data available to export");
      setTimeout(() => setError(null), 5000);
      return;
    }

    setExportingToSheets(messageIndex);
    setError(null);

    try {
      const result = await exportQueryResults({
        query: queryData.query,
        raw_data: queryData.raw_data,
        formatted_response: queryData.formatted_response || ""
      });

      if (result.success && result.sheet_url) {
        setExportSuccess({ index: messageIndex, url: result.sheet_url });
        // Open the sheet in a new tab
        window.open(result.sheet_url, '_blank');
        setTimeout(() => setExportSuccess(null), 5000);
      } else {
        setError(result.error || "Failed to export to Google Sheets");
        setTimeout(() => setError(null), 5000);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to export to Google Sheets");
      setTimeout(() => setError(null), 5000);
    } finally {
      setExportingToSheets(null);
    }
  };

  const handleETPSubmit = async () => {
    if (!etpDate || !etpInletValue) {
      setError("Please enter both date and ETP Inlet tank value");
      setTimeout(() => setError(null), 5000);
      return;
    }

    setIsLoadingETP(true);
    setError(null);

    try {
      const result = await getETPTankCapacity({
        date: etpDate,
        etp_inlet_tank_value: parseFloat(etpInletValue),
      });

      if (result.success) {
        setEtpData(result);
        setShowETPModal(false);
      } else {
        setError(result.message || "Failed to get ETP tank capacity");
        setTimeout(() => setError(null), 5000);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to get ETP tank capacity");
      setTimeout(() => setError(null), 5000);
    } finally {
      setIsLoadingETP(false);
    }
  };

  return (
    <div className="flex w-screen overflow-x-hidden">
      <div className="text-foreground relative flex min-h-screen w-full flex-col items-center justify-center overflow-hidden bg-transparent p-3 sm:p-4 md:p-6">
        <div className="absolute inset-0 h-full w-full overflow-hidden">
          <div className="bg-primary/10 absolute top-0 left-1/4 h-96 w-96 animate-pulse rounded-full mix-blend-normal blur-[128px] filter" />
          <div className="bg-secondary/10 absolute right-1/4 bottom-0 h-96 w-96 animate-pulse rounded-full mix-blend-normal blur-[128px] filter delay-700" />
          <div className="bg-primary/10 absolute top-1/4 right-1/3 h-64 w-64 animate-pulse rounded-full mix-blend-normal blur-[96px] filter delay-1000" />
        </div>

        <div className="relative mx-auto w-full max-w-2xl px-2 sm:px-4">
          <motion.div
            className="relative z-10 space-y-6 sm:space-y-8 md:space-y-12"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, ease: "easeOut" }}
          >
            <div className="space-y-3 sm:space-y-4 text-center relative">
              <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2, duration: 0.5 }}
                className="inline-block"
              >
                <h1 className="pb-1 text-xl sm:text-2xl md:text-3xl font-medium tracking-tight text-center">
                  How can I help today?
                </h1>
                <motion.div
                  className="via-primary/50 h-px bg-gradient-to-r from-transparent to-transparent"
                  initial={{ width: 0, opacity: 0 }}
                  animate={{ width: "100%", opacity: 1 }}
                  transition={{ delay: 0.5, duration: 0.8 }}
                />
              </motion.div>
              <motion.p
                className="text-muted-foreground text-xs sm:text-sm text-center px-2"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.3 }}
              >
                Type a command or ask a question about your data
              </motion.p>

              {/* Action Buttons - Moved below the text */}
              <motion.div
                className="flex flex-wrap justify-center gap-2 sm:gap-3 pt-2 px-2"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.4 }}
              >
                <a
                  href="/test-queries"
                  className={cn(
                    "flex items-center gap-1.5 sm:gap-2 px-3 sm:px-4 py-2 sm:py-2.5 rounded-lg transition-all",
                    "bg-blue-500/10 hover:bg-blue-500/20 text-blue-600",
                    "border border-blue-500/20 hover:border-blue-500/30",
                    "text-xs sm:text-sm font-medium whitespace-nowrap"
                  )}
                  title="Test natural language queries"
                >
                  <Command className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
                  <span className="hidden sm:inline">Test Queries</span>
                  <span className="sm:hidden">Test</span>
                </a>
                <button
                  onClick={handleSync}
                  disabled={isSyncing}
                  className={cn(
                    "flex items-center gap-1.5 sm:gap-2 px-3 sm:px-4 py-2 sm:py-2.5 rounded-lg transition-all",
                    "bg-primary/10 hover:bg-primary/20 text-primary",
                    "border border-primary/20 hover:border-primary/30",
                    "text-xs sm:text-sm font-medium whitespace-nowrap",
                    isSyncing && "opacity-50 cursor-not-allowed"
                  )}
                  title="Sync to latest data from Google Sheets"
                >
                  <RefreshCw
                    className={cn("w-3.5 h-3.5 sm:w-4 sm:h-4", isSyncing && "animate-spin")}
                  />
                  <span className="hidden sm:inline">{isSyncing ? "Syncing..." : "Sync Data"}</span>
                  <span className="sm:hidden">{isSyncing ? "Sync..." : "Sync"}</span>
                </button>
                <button
                  onClick={() => setShowETPModal(true)}
                  className={cn(
                    "flex items-center gap-1.5 sm:gap-2 px-3 sm:px-4 py-2 sm:py-2.5 rounded-lg transition-all",
                    "bg-green-500/10 hover:bg-green-500/20 text-green-600",
                    "border border-green-500/20 hover:border-green-500/30",
                    "text-xs sm:text-sm font-medium whitespace-nowrap"
                  )}
                  title="ETP Tank Capacity and Storage Details"
                >
                  <MonitorIcon className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
                  <span className="hidden md:inline">ETP TANK CAPACITY</span>
                  <span className="md:hidden">ETP</span>
                </button>
              </motion.div>

              {/* Sync Success Message */}
              <AnimatePresence>
                {syncSuccess && (
                  <motion.div
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                    className="bg-green-500/10 border border-green-500/20 text-green-600 px-3 py-1.5 sm:px-4 sm:py-2 rounded-lg text-xs sm:text-sm mx-2"
                  >
                    {syncSuccess}
                  </motion.div>
                )}
              </AnimatePresence>

              {/* Error Message */}
              <AnimatePresence>
                {error && (
                  <motion.div
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                    className="bg-red-500/10 border border-red-500/20 text-red-600 px-3 py-1.5 sm:px-4 sm:py-2 rounded-lg text-xs sm:text-sm mx-2"
                  >
                    {error}
                  </motion.div>
                )}
              </AnimatePresence>
            </div>

            {/* Messages Display */}
            {messages.length > 0 && (
              <motion.div
                className="border-border bg-card/80 space-y-3 sm:space-y-4 rounded-xl sm:rounded-2xl border p-3 sm:p-4 md:p-6 shadow-2xl backdrop-blur-2xl max-h-[300px] sm:max-h-[350px] md:max-h-[400px] overflow-y-auto"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
              >
                {messages.map((msg, idx) => (
                  <motion.div
                    key={idx}
                    className={cn(
                      "flex flex-col",
                      msg.role === "user" ? "items-end" : "items-start"
                    )}
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: idx * 0.1 }}
                  >
                    <div
                      className={cn(
                        "max-w-[90%] sm:max-w-[85%] rounded-lg text-xs sm:text-sm",
                        msg.role === "user"
                          ? "bg-primary text-primary-foreground px-3 py-2 sm:px-4 sm:py-3"
                          : "bg-muted/50 text-muted-foreground px-3 py-3 sm:px-6 sm:py-5 border border-border/50"
                      )}
                    >
                      {msg.role === "assistant" ? (
                        <MessageFormatter content={msg.content} />
                      ) : (
                        <div className="whitespace-pre-wrap leading-relaxed">
                          {msg.content}
                        </div>
                      )}
                    </div>
                    {msg.role === "assistant" && msg.queryData && msg.queryData.raw_data && (
                      <motion.button
                        onClick={() => handleExportToSheets(idx, msg.queryData!)}
                        disabled={exportingToSheets === idx}
                        className={cn(
                          "mt-2 flex items-center gap-1.5 sm:gap-2 px-2.5 sm:px-3 py-1 sm:py-1.5 rounded-md text-[10px] sm:text-xs transition-all",
                          "bg-blue-500/10 hover:bg-blue-500/20 text-blue-600",
                          "border border-blue-500/20 hover:border-blue-500/30",
                          exportingToSheets === idx && "opacity-50 cursor-not-allowed"
                        )}
                        initial={{ opacity: 0, y: -5 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: idx * 0.1 + 0.2 }}
                      >
                        {exportingToSheets === idx ? (
                          <>
                            <LoaderIcon className="h-2.5 w-2.5 sm:h-3 sm:w-3 animate-spin" />
                            <span className="hidden sm:inline">Exporting...</span>
                            <span className="sm:hidden">Export...</span>
                          </>
                        ) : (
                          <>
                            <MonitorIcon className="h-2.5 w-2.5 sm:h-3 sm:w-3" />
                            <span className="hidden sm:inline">View in Sheets</span>
                            <span className="sm:hidden">Sheets</span>
                          </>
                        )}
                      </motion.button>
                    )}
                    {exportSuccess?.index === idx && (
                      <motion.div
                        initial={{ opacity: 0, y: -5 }}
                        animate={{ opacity: 1, y: 0 }}
                        exit={{ opacity: 0, y: -5 }}
                        className="mt-1 text-xs text-green-600"
                      >
                        ✓ Exported! Opening sheet...
                      </motion.div>
                    )}
                  </motion.div>
                ))}
                {isTyping && (
                  <div className="flex justify-start">
                    <div className="bg-muted text-muted-foreground flex items-center gap-1.5 sm:gap-2 rounded-lg px-3 py-1.5 sm:px-4 sm:py-2 text-xs sm:text-sm">
                      <TypingDots />
                    </div>
                  </div>
                )}
              </motion.div>
            )}

            {error && (
              <motion.div
                className="bg-destructive/10 text-destructive rounded-lg border border-destructive/20 p-2.5 sm:p-3 text-xs sm:text-sm mx-2"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
              >
                {error}
              </motion.div>
            )}

            <motion.div
              className="border-border bg-card/80 relative rounded-xl sm:rounded-2xl border shadow-2xl backdrop-blur-2xl"
              initial={{ scale: 0.98 }}
              animate={{ scale: 1 }}
              transition={{ delay: 0.1 }}
            >
              <AnimatePresence>
                {showCommandPalette && (
                  <motion.div
                    ref={commandPaletteRef}
                    className="border-border bg-background/90 absolute right-2 sm:right-4 bottom-full left-2 sm:left-4 z-50 mb-2 overflow-hidden rounded-lg border shadow-lg backdrop-blur-xl"
                    initial={{ opacity: 0, y: 5 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: 5 }}
                    transition={{ duration: 0.15 }}
                  >
                    <div className="bg-background py-1">
                      {commandSuggestions.map((suggestion, index) => (
                        <motion.div
                          key={suggestion.prefix}
                          className={cn(
                            "flex cursor-pointer items-center gap-2 px-3 py-2 text-xs transition-colors",
                            activeSuggestion === index
                              ? "bg-primary/20 text-foreground"
                              : "text-muted-foreground hover:bg-primary/10"
                          )}
                          onClick={() => selectCommandSuggestion(index)}
                          initial={{ opacity: 0 }}
                          animate={{ opacity: 1 }}
                          transition={{ delay: index * 0.03 }}
                        >
                          <div className="text-primary flex h-5 w-5 items-center justify-center">
                            {suggestion.icon}
                          </div>
                          <div className="font-medium">{suggestion.label}</div>
                          <div className="text-muted-foreground ml-1 text-xs">
                            {suggestion.prefix}
                          </div>
                        </motion.div>
                      ))}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>

              <div className="p-3 sm:p-4">
                <Textarea
                  ref={textareaRef}
                  value={value}
                  onChange={(e) => {
                    setValue(e.target.value);
                    adjustHeight();
                  }}
                  onKeyDown={handleKeyDown}
                  onFocus={() => setInputFocused(true)}
                  onBlur={() => setInputFocused(false)}
                  placeholder="Ask mvp.ai a question..."
                  containerClassName="w-full"
                  className={cn(
                    "w-full px-3 py-2 sm:px-4 sm:py-3",
                    "resize-none",
                    "bg-transparent",
                    "border-none",
                    "text-foreground text-xs sm:text-sm",
                    "focus:outline-none",
                    "placeholder:text-muted-foreground",
                    "min-h-[50px] sm:min-h-[60px]"
                  )}
                  style={{
                    overflow: "hidden",
                  }}
                  showRing={false}
                />
              </div>

              <AnimatePresence>
                {attachments.length > 0 && (
                  <motion.div
                    className="flex flex-wrap gap-2 px-4 pb-3"
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: "auto" }}
                    exit={{ opacity: 0, height: 0 }}
                  >
                    {attachments.map((file, index) => (
                      <motion.div
                        key={index}
                        className="bg-primary/5 text-muted-foreground flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs"
                        initial={{ opacity: 0, scale: 0.9 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, scale: 0.9 }}
                      >
                        <span>{file}</span>
                        <button
                          onClick={() => removeAttachment(index)}
                          className="text-muted-foreground hover:text-foreground transition-colors"
                        >
                          <XIcon className="h-3 w-3" />
                        </button>
                      </motion.div>
                    ))}
                  </motion.div>
                )}
              </AnimatePresence>

              <div className="border-border flex items-center justify-between gap-2 sm:gap-4 border-t p-3 sm:p-4">
                <div className="flex items-center gap-2 sm:gap-3">
                  <motion.button
                    type="button"
                    onClick={handleAttachFile}
                    whileTap={{ scale: 0.94 }}
                    className="group text-muted-foreground hover:text-foreground relative rounded-lg p-1.5 sm:p-2 transition-colors"
                  >
                    <Paperclip className="h-3.5 w-3.5 sm:h-4 sm:w-4" />
                    <motion.span
                      className="bg-primary/10 absolute inset-0 rounded-lg opacity-0 transition-opacity group-hover:opacity-100"
                      layoutId="button-highlight"
                    />
                  </motion.button>

                  <motion.button
                    type="button"
                    data-command-button
                    onClick={(e) => {
                      e.stopPropagation();
                      setShowCommandPalette((prev) => !prev);
                    }}
                    whileTap={{ scale: 0.94 }}
                    className={cn(
                      "group text-muted-foreground hover:text-foreground relative rounded-lg p-1.5 sm:p-2 transition-colors",
                      showCommandPalette && "bg-primary/20 text-foreground"
                    )}
                  >
                    <Command className="h-3.5 w-3.5 sm:h-4 sm:w-4" />
                    <motion.span
                      className="bg-primary/10 absolute inset-0 rounded-lg opacity-0 transition-opacity group-hover:opacity-100"
                      layoutId="button-highlight"
                    />
                  </motion.button>
                </div>

                <motion.button
                  type="button"
                  onClick={handleSendMessage}
                  whileHover={{ scale: 1.01 }}
                  whileTap={{ scale: 0.98 }}
                  disabled={isTyping || !value.trim()}
                  className={cn(
                    "rounded-lg px-3 py-1.5 sm:px-4 sm:py-2 text-xs sm:text-sm font-medium transition-all",
                    "flex items-center gap-1.5 sm:gap-2",
                    value.trim()
                      ? "bg-primary text-primary-foreground shadow-primary/10 shadow-lg"
                      : "bg-muted/50 text-muted-foreground"
                  )}
                >
                  {isTyping ? (
                    <LoaderIcon className="h-3.5 w-3.5 sm:h-4 sm:w-4 animate-[spin_2s_linear_infinite]" />
                  ) : (
                    <SendIcon className="h-3.5 w-3.5 sm:h-4 sm:w-4" />
                  )}
                  <span className="hidden sm:inline">Send</span>
                </motion.button>
              </div>
            </motion.div>

            <div className="space-y-3 sm:space-y-4">
              {/* Command Suggestions */}
              <div className="flex flex-wrap items-center justify-center gap-1.5 sm:gap-2 px-2">
                {commandSuggestions.map((suggestion, index) => (
                  <motion.button
                    key={suggestion.prefix}
                    onClick={() => selectCommandSuggestion(index)}
                    className="group bg-primary/5 text-muted-foreground hover:bg-primary/10 hover:text-foreground relative flex items-center gap-1.5 sm:gap-2 rounded-lg px-2.5 py-1.5 sm:px-3 sm:py-2 text-xs sm:text-sm transition-all"
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: index * 0.1 }}
                  >
                    {suggestion.icon}
                    <span>{suggestion.label}</span>
                    <motion.div
                      className="border-border/50 absolute inset-0 rounded-lg border"
                      initial={false}
                      animate={{
                        opacity: [0, 1],
                        scale: [0.98, 1],
                      }}
                      transition={{
                        duration: 0.3,
                        ease: "easeOut",
                      }}
                    />
                  </motion.button>
                ))}
              </div>

              {/* Data Query Examples */}
              <div className="space-y-2">
                <p className="text-center text-[10px] sm:text-xs text-muted-foreground px-2">
                  Try asking about your data:
                </p>
                <div className="flex flex-wrap items-center justify-center gap-1.5 sm:gap-2 px-2">
                  {[
                    "What is the amount on December 12th?",
                    "Show me data from RO DETAILS",
                    "What is the latest amount?",
                    "Total amount for today",
                  ].map((query, index) => (
                    <motion.button
                      key={query}
                      onClick={() => {
                        setValue(query);
                        adjustHeight();
                      }}
                      className="bg-blue-500/5 text-blue-600 hover:bg-blue-500/10 border border-blue-500/20 hover:border-blue-500/30 rounded-lg px-2 py-1 sm:px-3 sm:py-1.5 text-[10px] sm:text-xs transition-all"
                      initial={{ opacity: 0, y: 5 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: 0.5 + index * 0.1 }}
                    >
                      {query}
                    </motion.button>
                  ))}
                </div>
              </div>
            </div>
          </motion.div>
        </div>

        <AnimatePresence>
          {isTyping && (
            <motion.div
              className="border-border bg-background/80 fixed bottom-4 sm:bottom-8 left-1/2 -translate-x-1/2 transform rounded-full border px-3 py-1.5 sm:px-4 sm:py-2 shadow-lg backdrop-blur-2xl"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 20 }}
            >
              <div className="flex items-center gap-2 sm:gap-3">
                <div className="bg-primary/10 flex h-6 w-7 sm:h-7 sm:w-8 items-center justify-center rounded-full text-center">
                  <Sparkles className="text-primary h-3 w-3 sm:h-4 sm:w-4" />
                </div>
                <div className="text-muted-foreground flex items-center gap-1.5 sm:gap-2 text-xs sm:text-sm">
                  <span>Thinking</span>
                  <TypingDots />
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {inputFocused && (
          <motion.div
            className="from-primary via-primary/80 to-secondary pointer-events-none fixed z-0 h-[50rem] w-[50rem] rounded-full bg-gradient-to-r opacity-[0.02] blur-[96px]"
            animate={{
              x: mousePosition.x - 400,
              y: mousePosition.y - 400,
            }}
            transition={{
              type: "spring",
              damping: 25,
              stiffness: 150,
              mass: 0.5,
            }}
          />
        )}

        {/* ETP Tank Capacity Modal */}
        <AnimatePresence>
          {showETPModal && (
            <motion.div
              className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setShowETPModal(false)}
            >
              <motion.div
                className="bg-card border-border relative max-w-md w-full mx-4 rounded-xl sm:rounded-2xl border p-4 sm:p-6 shadow-2xl"
                initial={{ scale: 0.95, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                exit={{ scale: 0.95, opacity: 0 }}
                onClick={(e) => e.stopPropagation()}
              >
                <button
                  onClick={() => setShowETPModal(false)}
                  className="text-muted-foreground hover:text-foreground absolute right-3 top-3 sm:right-4 sm:top-4"
                >
                  <XIcon className="h-4 w-4 sm:h-5 sm:w-5" />
                </button>
                <h2 className="text-foreground mb-3 sm:mb-4 text-lg sm:text-xl font-semibold">
                  ETP Tank Capacity
                </h2>
                <div className="space-y-3 sm:space-y-4">
                  <div>
                    <label className="text-muted-foreground mb-1.5 sm:mb-2 block text-xs sm:text-sm font-medium">
                      Date (DD.MM.YYYY)
                    </label>
                    <input
                      type="text"
                      value={etpDate}
                      onChange={(e) => setEtpDate(e.target.value)}
                      placeholder="25.10.2025"
                      className="border-input bg-background w-full rounded-md border px-2.5 py-1.5 sm:px-3 sm:py-2 text-xs sm:text-sm"
                    />
                  </div>
                  <div>
                    <label className="text-muted-foreground mb-1.5 sm:mb-2 block text-xs sm:text-sm font-medium">
                      ETP Inlet Tank Value (KL)
                    </label>
                    <input
                      type="number"
                      value={etpInletValue}
                      onChange={(e) => setEtpInletValue(e.target.value)}
                      placeholder="40"
                      className="border-input bg-background w-full rounded-md border px-2.5 py-1.5 sm:px-3 sm:py-2 text-xs sm:text-sm"
                    />
                  </div>
                  <button
                    onClick={handleETPSubmit}
                    disabled={isLoadingETP}
                    className={cn(
                      "w-full rounded-lg px-3 py-2 sm:px-4 sm:py-2 text-xs sm:text-sm font-medium transition-all",
                      "bg-primary text-primary-foreground",
                      isLoadingETP && "opacity-50 cursor-not-allowed"
                    )}
                  >
                    {isLoadingETP ? "Loading..." : "Get Tank Capacity"}
                  </button>
                </div>
              </motion.div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* ETP Tank Capacity Table Display */}
        <AnimatePresence>
          {etpData && etpData.success && (
            <motion.div
              className="border-border bg-card/80 relative mx-auto mt-4 sm:mt-6 w-full max-w-4xl rounded-xl sm:rounded-2xl border p-3 sm:p-4 md:p-6 shadow-2xl backdrop-blur-2xl"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 20 }}
            >
              <div className="mb-3 sm:mb-4 flex items-center justify-between">
                <h3 className="text-foreground text-base sm:text-lg font-semibold pr-2">
                  ETP Tank Capacity and Storage Details
                </h3>
                <button
                  onClick={() => setEtpData(null)}
                  className="text-muted-foreground hover:text-foreground flex-shrink-0"
                >
                  <XIcon className="h-4 w-4 sm:h-5 sm:w-5" />
                </button>
              </div>
              <div className="mb-2 text-xs sm:text-sm text-muted-foreground">
                Date: {etpData.date}
              </div>
              <div className="overflow-x-auto -mx-3 sm:-mx-4 md:-mx-6 px-3 sm:px-4 md:px-6">
                <table className="w-full border-collapse min-w-[600px]">
                  <thead>
                    <tr className="border-b border-border">
                      <th className="border-r border-border bg-muted/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-left text-xs sm:text-sm font-medium">
                        Tank Particulars
                      </th>
                      <th className="border-r border-border bg-yellow-100/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm font-medium">
                        Actual Capacity
                        <div className="text-[10px] sm:text-xs font-normal text-muted-foreground">
                          Qty. in KL
                        </div>
                      </th>
                      <th className="border-r border-border bg-pink-100/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm font-medium">
                        Storage @ 5.00pm
                        <div className="text-[10px] sm:text-xs font-normal text-muted-foreground">
                          Qty. in KL
                        </div>
                      </th>
                      <th className="bg-green-100/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm font-medium">
                        Balance
                        <div className="text-[10px] sm:text-xs font-normal text-muted-foreground">
                          Qty. in KL
                        </div>
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(etpData.tanks).map(([tankName, tankData]) => (
                      <tr key={tankName} className="border-b border-border">
                        <td className="border-r border-border px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-xs sm:text-sm">
                          {tankName}
                        </td>
                        <td className="border-r border-border bg-yellow-50/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm">
                          {tankData.actual_capacity.toFixed(0)}
                        </td>
                        <td className="border-r border-border bg-pink-50/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm">
                          {tankData.storage.toFixed(0)}
                        </td>
                        <td className="bg-green-50/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm">
                          {tankData.balance.toFixed(0)}
                        </td>
                      </tr>
                    ))}
                    <tr className="border-t-2 border-green-500 bg-green-50/30 font-semibold">
                      <td className="border-r border-border px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-xs sm:text-sm">
                        Total
                      </td>
                      <td className="border-r border-border bg-yellow-50/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm text-red-600">
                        {etpData.totals.total_capacity.toFixed(0)}
                      </td>
                      <td className="border-r border-border bg-pink-50/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm text-red-600">
                        {etpData.totals.total_storage.toFixed(0)}
                      </td>
                      <td className="bg-green-50/50 px-2 sm:px-3 md:px-4 py-1.5 sm:py-2 text-center text-xs sm:text-sm text-red-600">
                        {etpData.totals.total_balance.toFixed(0)}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

function TypingDots() {
  return (
    <div className="ml-1 flex items-center">
      {[1, 2, 3].map((dot) => (
        <motion.div
          key={dot}
          className="bg-primary mx-0.5 h-1.5 w-1.5 rounded-full"
          initial={{ opacity: 0.3 }}
          animate={{
            opacity: [0.3, 0.9, 0.3],
            scale: [0.85, 1.1, 0.85],
          }}
          transition={{
            duration: 1.2,
            repeat: Infinity,
            delay: dot * 0.15,
            ease: "easeInOut",
          }}
          style={{
            boxShadow: "0 0 4px rgba(255, 255, 255, 0.3)",
          }}
        />
      ))}
    </div>
  );
}
