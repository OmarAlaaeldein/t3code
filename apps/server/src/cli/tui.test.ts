import { describe, expect, it } from "vitest";

import {
  NoAvailableProvidersTuiError,
  NoRunningServerTuiError,
  tuiCommand,
  tuiFlags,
  TuiProviderNotFoundError,
  TuiThreadNotFoundError,
  formatThreadAsMarkdown,
} from "./tui.ts";

describe("tui cli", () => {
  it("defines tuiCommand", () => {
    expect(tuiCommand).toBeDefined();
    expect(tuiFlags).toBeDefined();
  });

  it("formats NoRunningServerTuiError", () => {
    const error = new NoRunningServerTuiError({
      checkedStatePaths: ["/tmp/server-state.json"],
    });
    expect(error.message).toContain("No running T3 Code server found.");
    expect(error.message).toContain("/tmp/server-state.json");
    expect(error.message).toContain("t3 serve");
  });

  it("formats TuiProviderNotFoundError", () => {
    const error = new TuiProviderNotFoundError({
      requested: "nonexistent",
      available: ["codex", "claudeAgent"],
    });
    expect(error.message).toContain("Provider 'nonexistent' not found.");
    expect(error.message).toContain("codex, claudeAgent");
  });

  it("formats TuiThreadNotFoundError", () => {
    const error = new TuiThreadNotFoundError({ threadId: "thread-xyz" });
    expect(error.message).toContain("Thread 'thread-xyz' not found on the T3 server.");
  });

  it("formats NoAvailableProvidersTuiError", () => {
    const error = new NoAvailableProvidersTuiError({});
    expect(error.message).toContain("No ready or enabled LLM providers found");
  });
  it("formats conversation thread to markdown", () => {
    const md = formatThreadAsMarkdown(
      {
        id: "thread-123",
        title: "Test Thread",
        createdAt: "2026-09-14T00:00:00.000Z",
        modelSelection: { instanceId: "antigravity", model: "gemini-3.8-flash" },
        messages: [
          { role: "user", text: "Hello AI" },
          { role: "assistant", text: "Hello user! How can I help?" },
        ],
      },
      "Test Project",
    );
    expect(md).toContain("# Test Thread");
    expect(md).toContain("- **Project:** Test Project");
    expect(md).toContain("- **Thread ID:** `thread-123`");
    expect(md).toContain("### 👤 User");
    expect(md).toContain("Hello AI");
    expect(md).toContain("### 🤖 Assistant");
    expect(md).toContain("Hello user! How can I help?");
  });
});
