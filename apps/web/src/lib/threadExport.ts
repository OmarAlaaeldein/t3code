export function formatThreadToMarkdown(
  thread: {
    readonly id: string;
    readonly title?: string;
    readonly createdAt?: string;
    readonly modelSelection?: { readonly instanceId?: string; readonly model?: string } | null;
    readonly messages?: ReadonlyArray<{
      readonly role: string;
      readonly text: string;
      readonly createdAt?: string;
    }>;
  },
  projectTitle?: string,
): string {
  const lines: string[] = [];
  lines.push(`# ${thread.title || "T3 Code Conversation"}\n`);
  if (projectTitle) lines.push(`- **Project:** ${projectTitle}`);
  lines.push(`- **Thread ID:** \`${thread.id}\``);
  if (thread.createdAt) lines.push(`- **Date:** ${new Date(thread.createdAt).toISOString()}`);
  if (thread.modelSelection) {
    lines.push(
      `- **Model:** \`${thread.modelSelection.instanceId ?? "default"}/${thread.modelSelection.model ?? "default"}\``,
    );
  }
  lines.push(`\n---\n`);

  for (const msg of thread.messages ?? []) {
    const roleTitle = msg.role === "user" ? "### 👤 User" : "### 🤖 Assistant";
    lines.push(`${roleTitle}\n`);
    lines.push(msg.text.trim());
    lines.push(`\n\n---\n`);
  }

  return lines.join("\n");
}
