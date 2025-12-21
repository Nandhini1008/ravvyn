import React from "react";
import { cn } from "@/lib/utils";

interface MessageFormatterProps {
  content: string;
  className?: string;
}

export function MessageFormatter({
  content,
  className,
}: MessageFormatterProps) {
  // Simple formatter for enhanced responses
  const formatMessage = (text: string) => {
    const lines = text.split("\n");
    const elements: React.ReactNode[] = [];

    lines.forEach((line, index) => {
      const trimmedLine = line.trim();

      if (!trimmedLine) {
        // Empty line - add spacing
        elements.push(<div key={index} className="h-2" />);
        return;
      }

      // Headers (lines starting with **)
      if (
        trimmedLine.startsWith("**") &&
        trimmedLine.endsWith("**") &&
        trimmedLine.length > 4
      ) {
        const headerText = trimmedLine.slice(2, -2);

        // Different styles for different header types
        let headerClass = "font-semibold text-foreground mb-2 mt-3 first:mt-0";

        if (headerText.includes("Answer") || headerText.includes("Found")) {
          headerClass =
            "font-bold text-base sm:text-lg text-foreground mb-2 sm:mb-3 mt-3 sm:mt-4 first:mt-0 border-b border-border/30 pb-1.5 sm:pb-2";
        } else if (
          headerText.includes("Details") ||
          headerText.includes("Context")
        ) {
          headerClass =
            "font-medium text-foreground mb-1.5 sm:mb-2 mt-2 sm:mt-3 first:mt-0 text-xs sm:text-sm uppercase tracking-wide text-muted-foreground";
        }

        elements.push(
          <div key={index} className={headerClass}>
            {headerText}
          </div>
        );
        return;
      }

      // Bullet points
      if (trimmedLine.startsWith("•")) {
        const bulletText = trimmedLine.slice(1).trim();
        const formattedText = formatInlineText(bulletText);

        // Different styles for different bullet types
        let bulletClass = "flex items-start gap-1.5 sm:gap-2 mb-1 sm:mb-1.5 ml-1 sm:ml-2";
        let textClass = "text-xs sm:text-sm text-muted-foreground flex-1";

        if (bulletText.includes("Source:") || bulletText.includes("Row:")) {
          textClass = "text-[10px] sm:text-xs text-muted-foreground flex-1 font-mono break-all";
        } else if (bulletText.includes(":")) {
          textClass = "text-xs sm:text-sm text-foreground flex-1";
        }

        elements.push(
          <div key={index} className={bulletClass}>
            <span className="text-primary mt-1 text-xs font-bold">•</span>
            <span className={textClass}>{formattedText}</span>
          </div>
        );
        return;
      }

      // Table rows (markdown tables)
      if (
        trimmedLine.includes("|") &&
        (trimmedLine.startsWith("|") || trimmedLine.match(/^\s*\|/))
      ) {
        // Check if this is a separator row
        if (trimmedLine.match(/^\|[\s\-\|]+\|$/)) {
          elements.push(
            <div key={index} className="border-t border-border my-1" />
          );
          return;
        }

        // Parse table row
        const cells = trimmedLine
          .split("|")
          .map((cell) => cell.trim())
          .filter((cell) => cell);

        // Determine if this is a header row (check if next line is separator)
        const nextLine = lines[index + 1]?.trim();
        const isHeader = nextLine && nextLine.match(/^\|[\s\-\|]+\|$/);

        elements.push(
          <div
            key={index}
            className={cn(
              "grid gap-1 sm:gap-2 py-1.5 sm:py-2 px-2 sm:px-3 rounded-md mb-1 overflow-x-auto",
              isHeader
                ? "bg-primary/5 font-medium text-foreground border border-primary/20"
                : "bg-muted/30 text-xs sm:text-sm",
              cells.length <= 2
                ? "grid-cols-2"
                : cells.length <= 3
                ? "grid-cols-2 sm:grid-cols-3"
                : cells.length <= 4
                ? "grid-cols-2 sm:grid-cols-4"
                : cells.length <= 5
                ? "grid-cols-2 sm:grid-cols-3 md:grid-cols-5"
                : "grid-cols-2 sm:grid-cols-3 md:grid-cols-6"
            )}
          >
            {cells.slice(0, 6).map((cell, cellIndex) => (
              <div
                key={cellIndex}
                className={cn(
                  "truncate break-words",
                  isHeader ? "font-semibold text-center text-xs sm:text-sm" : "text-left"
                )}
              >
                {formatInlineText(cell)}
              </div>
            ))}
          </div>
        );
        return;
      }

      // Separator lines
      if (trimmedLine === "---" || trimmedLine.startsWith("===")) {
        elements.push(
          <div key={index} className="border-t border-border my-4" />
        );
        return;
      }

      // Indented lines (context info)
      if (line.startsWith("   ") || line.startsWith("     ")) {
        const indentedText = line.trim();
        const formattedText = formatInlineText(indentedText);
        elements.push(
          <div key={index} className="text-[10px] sm:text-xs text-muted-foreground ml-3 sm:ml-6 mb-1">
            {formattedText}
          </div>
        );
        return;
      }

      // Italic text (lines starting with *)
      if (
        trimmedLine.startsWith("*") &&
        trimmedLine.endsWith("*") &&
        !trimmedLine.startsWith("**")
      ) {
        const italicText = trimmedLine.slice(1, -1);
        elements.push(
          <div
            key={index}
            className="text-xs sm:text-sm text-muted-foreground italic mb-1.5 sm:mb-2"
          >
            {italicText}
          </div>
        );
        return;
      }

      // Regular text
      const formattedText = formatInlineText(trimmedLine);
      elements.push(
        <div
          key={index}
          className="text-xs sm:text-sm text-muted-foreground mb-1.5 sm:mb-2 leading-relaxed"
        >
          {formattedText}
        </div>
      );
    });

    return elements;
  };

  const formatInlineText = (text: string): React.ReactNode => {
    // Handle inline bold text (**text**)
    const parts = text.split(/(\*\*[^*]+\*\*)/g);

    return parts.map((part, index) => {
      if (part.startsWith("**") && part.endsWith("**") && part.length > 4) {
        const boldText = part.slice(2, -2);
        return (
          <span key={index} className="font-semibold text-foreground">
            {boldText}
          </span>
        );
      }

      // Handle emojis and icons
      if (
        part.includes("📊") ||
        part.includes("📅") ||
        part.includes("📋") ||
        part.includes("🔍") ||
        part.includes("📍") ||
        part.includes("📈")
      ) {
        return (
          <span key={index} className="inline-flex items-center gap-1">
            {part}
          </span>
        );
      }

      return <span key={index}>{part}</span>;
    });
  };

  return (
    <div className={cn("space-y-1", className)}>{formatMessage(content)}</div>
  );
}
