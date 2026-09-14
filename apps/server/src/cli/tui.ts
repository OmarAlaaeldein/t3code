// @effect-diagnostics nodeBuiltinImport:off
/**
 * `t3` — interactive Terminal User Interface (TUI) harness for T3 Code.
 *
 * Connects directly to a running local T3 Code server instance via WebSocket RPC
 * and HTTP APIs. Provides streaming tokens, tool activity notifications, interactive
 * approval prompts, numbered model/session selection, and session management.
 */
import * as NodeFs from "node:fs/promises";
import * as NodeReadlinePromises from "node:readline/promises";

import {
  AuthAdministrativeScopes,
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EnvironmentHttpApi,
  isProviderAvailable,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  ProjectId,
  ProviderApprovalDecision,
  ProviderInstanceId,
  ProviderInteractionMode,
  RuntimeMode,
  ThreadId,
  TurnId,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Argument, Command, Flag, GlobalFlag } from "effect/unstable/cli";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import * as Socket from "effect/unstable/socket/Socket";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../config.ts";
import { isProcessAlive, readPersistedServerRuntimeState } from "../serverRuntimeState.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { baseDirFlag, resolveCliAuthConfig } from "./config.ts";

export class NoRunningServerTuiError extends Schema.TaggedError<NoRunningServerTuiError>()(
  "NoRunningServerTuiError",
  { checkedStatePaths: Schema.Array(Schema.String) },
) {
  override get message(): string {
    return [
      "No running T3 Code server found.",
      ...this.checkedStatePaths.map((statePath) => `  checked ${statePath}`),
      "Start a server first with: `t3 serve` or launch the T3 Code desktop app.",
    ].join("\n");
  }
}

export class TuiProviderNotFoundError extends Schema.TaggedError<TuiProviderNotFoundError>()(
  "TuiProviderNotFoundError",
  { requested: Schema.String, available: Schema.Array(Schema.String) },
) {
  override get message(): string {
    return `Provider '${this.requested}' not found. Available providers: ${this.available.join(", ") || "(none)"}`;
  }
}

export class TuiThreadNotFoundError extends Schema.TaggedError<TuiThreadNotFoundError>()(
  "TuiThreadNotFoundError",
  { threadId: Schema.String },
) {
  override get message(): string {
    return `Thread '${this.threadId}' not found on the T3 server.`;
  }
}

export class NoAvailableProvidersTuiError extends Schema.TaggedError<NoAvailableProvidersTuiError>()(
  "NoAvailableProvidersTuiError",
  {},
) {
  override get message(): string {
    return "No ready or enabled LLM providers found on the T3 server. Configure one in the T3 desktop app or settings.";
  }
}

const randomUuid = Crypto.Crypto.pipe(Effect.flatMap((crypto) => crypto.randomUUIDv4));

const wsRpcProtocolLayer = (wsUrl: string) => {
  const webSocketConstructorLayer = Layer.succeed(
    Socket.WebSocketConstructor,
    (socketUrl, protocols) =>
      new NodeSocket.NodeWS.WebSocket(socketUrl, protocols) as unknown as globalThis.WebSocket,
  );

  return RpcClient.layerProtocolSocket().pipe(
    Layer.provide(Socket.layerWebSocket(wsUrl).pipe(Layer.provide(webSocketConstructorLayer))),
    Layer.provide(RpcSerialization.layerJson),
  );
};

const makeWsRpcClient = RpcClient.make(WsRpcGroup);
type WsRpcClient =
  typeof makeWsRpcClient extends Effect.Effect<infer Client, any, any> ? Client : never;

const withWsRpcClient = <A, E, R>(
  wsUrl: string,
  fn: (client: WsRpcClient) => Effect.Effect<A, E, R>,
) => makeWsRpcClient.pipe(Effect.flatMap(fn), Effect.provide(wsRpcProtocolLayer(wsUrl)));

const withProjectCliSessionToken = <A, E, R>(
  environmentAuth: EnvironmentAuth.EnvironmentAuth["Service"],
  run: (token: string) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    environmentAuth.issueSession({
      scopes: AuthAdministrativeScopes,
      label: "t3 tui harness",
    }),
    (issued) => run(issued.token),
    (issued) => environmentAuth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
  );

const requestWebSocketTicket = (origin: string, bearerToken: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const request = HttpClientRequest.post(
      new URL("/api/auth/websocket-ticket", origin).toString(),
    ).pipe(HttpClientRequest.setHeader("authorization", `Bearer ${bearerToken}`));
    const response = yield* client.execute(request);
    const json = (yield* response.json) as { readonly ticket: string };
    return json.ticket;
  });

const askQuestion = (promptText: string) =>
  Effect.promise(
    () =>
      new Promise<string>((resolve) => {
        const rl = NodeReadlinePromises.createInterface({
          input: process.stdin,
          output: process.stdout,
        });
        rl.question(promptText)
          .then((answer) => {
            rl.close();
            resolve(answer.trim());
          })
          .catch(() => {
            rl.close();
            resolve("");
          });
      }),
  );

const askApprovalDecision = (
  summary: string,
  detail?: string,
): Effect.Effect<ProviderApprovalDecision> =>
  Effect.gen(function* () {
    process.stdout.write(
      `\n\x1b[1;33m⚠️  [APPROVAL REQUIRED]\x1b[0m ${summary}\n` +
        (detail ? `   \x1b[90m${detail}\x1b[0m\n` : "") +
        `   \x1b[1m[y]\x1b[0m Approve   \x1b[1m[a]\x1b[0m Auto-approve remainder   \x1b[1m[r]\x1b[0m Reject\n`,
    );

    const answer = (yield* askQuestion("   Decision [y/a/r]: ")).toLowerCase();
    if (answer === "a" || answer === "always") {
      return "accept-always" as const;
    }
    if (answer === "r" || answer === "n" || answer === "no") {
      return "reject" as const;
    }
    return "accept" as const;
  });

const promptChoice = (items: ReadonlyArray<string>, label: string) =>
  Effect.gen(function* () {
    for (let i = 0; i < items.length; i++) {
      process.stdout.write(`  \x1b[36m[${i + 1}]\x1b[0m ${items[i]}\n`);
    }
    process.stdout.write("\n");
    const answer = yield* askQuestion(
      `Select ${label} [1-${items.length}] or Enter to keep current: `,
    );
    const num = parseInt(answer, 10);
    return !isNaN(num) && num >= 1 && num <= items.length ? num - 1 : undefined;
  });

const dispatch = (wsClient: WsRpcClient, command: any) =>
  Effect.gen(function* () {
    const id = yield* randomUuid;
    const now = yield* DateTime.now;
    return yield* wsClient[ORCHESTRATION_WS_METHODS.dispatchCommand]({
      commandId: CommandId.make(id),
      createdAt: DateTime.formatIso(now),
      ...command,
    });
  });

interface ModelChoice {
  readonly provider: any;
  readonly modelSlug: string;
}

const getAvailableModelChoices = (providers: readonly any[]): ModelChoice[] => {
  const choices: ModelChoice[] = [];
  for (const prov of providers) {
    if (prov.status === "disabled") continue;
    for (const m of prov.models) {
      choices.push({ provider: prov, modelSlug: m.slug });
    }
  }
  return choices;
};

const printTuiBanner = (info: {
  origin: string;
  projectTitle: string;
  cwd: string;
  threadId: string;
  threadTitle: string;
  provider: string;
  model: string;
  runtimeMode: string;
  interactionMode: string;
}) => {
  const line = "─".repeat(60);
  process.stdout.write(
    `\n\x1b[36m╭${line}╮\x1b[0m\n` +
      `\x1b[36m│\x1b[0m \x1b[1;32m● T3 Code TUI Harness\x1b[0m \x1b[90m(${info.origin})\x1b[0m\n` +
      `\x1b[36m│\x1b[0m \x1b[1mProject:\x1b[0m  ${info.projectTitle} \x1b[90m(${info.cwd})\x1b[0m\n` +
      `\x1b[36m│\x1b[0m \x1b[1mSession:\x1b[0m  \x1b[36m${info.threadId}\x1b[0m (${info.threadTitle})\n` +
      `\x1b[36m│\x1b[0m \x1b[1mModel:\x1b[0m    \x1b[33m${info.provider}/${info.model}\x1b[0m\n` +
      `\x1b[36m│\x1b[0m \x1b[1mMode:\x1b[0m     ${info.runtimeMode} \x1b[90m(${info.interactionMode})\x1b[0m\n` +
      `\x1b[36m╰${line}╯\x1b[0m\n` +
      `Type \x1b[36m/help\x1b[0m for commands (\x1b[90m/models, /threads, /new, /permission, /plan, /exit\x1b[0m)\n\n`,
  );
};

const printTuiHelp = () => {
  process.stdout.write(
    `\n\x1b[1mTUI Harness Commands:\x1b[0m\n` +
      `  \x1b[36m/models\x1b[0m, \x1b[36m/model [num|name]\x1b[0m   Numbered model picker or direct switch (e.g. /model 2)\n` +
      `  \x1b[36m/threads\x1b[0m, \x1b[36m/resume [num|id]\x1b[0m   Numbered session picker or switch (e.g. /resume 1)\n` +
      `  \x1b[36m/new [title]\x1b[0m               Start a fresh session/thread\n` +
      `  \x1b[36m/permission [1-3|mode]\x1b[0m     Set mode (1: approval-required, 2: auto-accept-edits, 3: full-access)\n` +
      `  \x1b[36m/plan\x1b[0m                      Switch to plan mode (architectural/read-only)\n` +
      `  \x1b[36m/normal\x1b[0m                    Switch to normal execution mode\n` +
      `  \x1b[36m/status\x1b[0m                    Show active session status\n` +
      `  \x1b[36m/clear\x1b[0m                     Clear screen\n` +
      `  \x1b[36m/exit\x1b[0m, \x1b[36m/quit\x1b[0m, \x1b[36m:q\x1b[0m              Disconnect (session state preserved on server)\n\n`,
  );
};

export function formatThreadAsMarkdown(
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

export const runTuiCommand = (flags: TuiCliOptions) =>
  Effect.gen(function* () {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig({ baseDir: flags.baseDir }, logLevel);
    const minimumLogLevel = config.logLevel;

    return yield* Effect.gen(function* () {
      const path = yield* Path.Path;
      const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
      const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;

      const runtimeState = yield* readPersistedServerRuntimeState(config.serverRuntimeStatePath);
      if (Option.isNone(runtimeState) || !isProcessAlive(runtimeState.value.pid)) {
        return yield* new NoRunningServerTuiError({
          checkedStatePaths: [config.serverRuntimeStatePath],
        });
      }

      const { origin } = runtimeState.value;

      yield* withProjectCliSessionToken(environmentAuth, (bearerToken) =>
        Effect.gen(function* () {
          const httpClient = yield* HttpApiClient.make(EnvironmentHttpApi, {
            baseUrl: origin,
          });

          const wsTicket = yield* requestWebSocketTicket(origin, bearerToken);
          const wsUrl = new URL("/ws", origin);
          wsUrl.searchParams.set("wsTicket", wsTicket);

          const initialSnapshot = yield* httpClient.orchestration.snapshot({
            headers: { authorization: `Bearer ${bearerToken}` },
          });

          const resolvedCwd = Option.getOrElse(flags.cwd, () => process.env.PWD ?? process.cwd());
          const normalizedCwd = yield* workspacePaths.normalizeWorkspaceRoot(resolvedCwd);

          return yield* withWsRpcClient(wsUrl.toString(), (wsClient) =>
            Effect.gen(function* () {
              let project = initialSnapshot.projects.find(
                (p) => p.deletedAt === null && p.workspaceRoot === normalizedCwd,
              );

              if (!project) {
                const newProjectId = ProjectId.make(yield* randomUuid);
                const projectTitle = path.basename(normalizedCwd) || "project";
                const nowIso = DateTime.formatIso(yield* DateTime.now);

                yield* dispatch(wsClient, {
                  type: "project.create",
                  projectId: newProjectId,
                  title: projectTitle,
                  workspaceRoot: normalizedCwd,
                });

                project = {
                  id: newProjectId,
                  title: projectTitle,
                  workspaceRoot: normalizedCwd,
                  createdAt: nowIso,
                  updatedAt: nowIso,
                  deletedAt: null,
                } as any;
              }

              const serverConfig = yield* wsClient[WS_METHODS.serverGetConfig]({});
              let availableProviders = serverConfig.providers.filter(
                (p) => (p.status === "ready" || p.status === "warning") && isProviderAvailable(p),
              );

              let currentProvider = availableProviders[0];
              if (Option.isSome(flags.provider)) {
                currentProvider = availableProviders.find(
                  (p) => p.instanceId === flags.provider.value,
                );
                if (!currentProvider) {
                  return yield* new TuiProviderNotFoundError({
                    requested: flags.provider.value,
                    available: availableProviders.map((p) => p.instanceId),
                  });
                }
              } else if (!currentProvider) {
                return yield* new NoAvailableProvidersTuiError({});
              }

              let currentModelSlug: string;
              if (Option.isSome(flags.model)) {
                currentModelSlug = flags.model.value;
              } else {
                const defaultModel = currentProvider.models.find((m) => m.isDefault);
                currentModelSlug =
                  defaultModel?.slug ?? currentProvider.models[0]?.slug ?? "default";
              }

              const existingProjectThreads = initialSnapshot.threads
                .filter((t) => t.projectId === project.id && t.deletedAt === null)
                .sort((a, b) => b.createdAt.localeCompare(a.createdAt));

              if (Option.isNone(flags.provider) && existingProjectThreads.length > 0) {
                const latestWithModel = existingProjectThreads.find(
                  (t) => t.modelSelection !== null,
                );
                if (latestWithModel?.modelSelection) {
                  const matchProv = availableProviders.find(
                    (p) => p.instanceId === latestWithModel.modelSelection?.instanceId,
                  );
                  if (matchProv) {
                    currentProvider = matchProv;
                    if (Option.isNone(flags.model)) {
                      currentModelSlug = latestWithModel.modelSelection.model;
                    }
                  }
                }
              }

              let currentRuntimeMode: RuntimeMode = Option.getOrElse(
                flags.permission,
                () => "approval-required" as const,
              );

              let currentInteractionMode: ProviderInteractionMode = flags.plan
                ? "plan"
                : DEFAULT_PROVIDER_INTERACTION_MODE;

              let showDiffs = flags.diff;

              let currentThreadId: ThreadId;
              let currentThreadTitle: string = "TUI Session";
              if (Option.isSome(flags.thread)) {
                const threadIdStr = flags.thread.value;
                const existingThread = initialSnapshot.threads.find((t) => t.id === threadIdStr);
                if (!existingThread) {
                  return yield* new TuiThreadNotFoundError({ threadId: threadIdStr });
                }
                currentThreadId = existingThread.id;
                currentThreadTitle = existingThread.title;
              } else if (Option.isNone(flags.prompt) && existingProjectThreads.length > 0) {
                currentThreadId = existingProjectThreads[0].id;
                currentThreadTitle = existingProjectThreads[0].title;
                if (existingProjectThreads[0].modelSelection) {
                  const matchProv = availableProviders.find(
                    (p) => p.instanceId === existingProjectThreads[0].modelSelection?.instanceId,
                  );
                  if (matchProv) currentProvider = matchProv;
                  currentModelSlug = existingProjectThreads[0].modelSelection.model;
                }
              } else {
                currentThreadId = ThreadId.make(yield* randomUuid);
                const initialPromptPreview = Option.getOrElse(flags.prompt, () => "TUI Session");
                currentThreadTitle = initialPromptPreview.slice(0, 50).trim() || "TUI Session";
                yield* dispatch(wsClient, {
                  type: "thread.create",
                  threadId: currentThreadId,
                  projectId: project.id,
                  title: currentThreadTitle,
                  modelSelection: {
                    instanceId: currentProvider.instanceId,
                    model: currentModelSlug,
                  },
                  runtimeMode: currentRuntimeMode,
                  interactionMode: currentInteractionMode,
                  branch: null,
                  worktreePath: null,
                });
              }

              printTuiBanner({
                origin,
                projectTitle: project.title,
                cwd: normalizedCwd,
                threadId: currentThreadId,
                threadTitle: currentThreadTitle,
                provider: currentProvider.instanceId,
                model: currentModelSlug,
                runtimeMode: currentRuntimeMode,
                interactionMode: currentInteractionMode,
              });

              let isFirstTurn = true;

              while (true) {
                let inputLine: string;
                if (isFirstTurn && Option.isSome(flags.prompt)) {
                  inputLine = flags.prompt.value.trim();
                  process.stdout.write(
                    `\x1b[1;32m[${currentModelSlug}]\x1b[0m \x1b[36m❯\x1b[0m ${inputLine}\n\n`,
                  );
                } else {
                  inputLine = yield* askQuestion(
                    `\x1b[1;32m[${currentModelSlug}]\x1b[0m \x1b[36m❯\x1b[0m `,
                  );
                }
                isFirstTurn = false;

                if (inputLine.length === 0) continue;

                if (inputLine.startsWith("/")) {
                  const [command, ...args] = inputLine.slice(1).trim().split(/\\s+/);
                  const lowerCmd = command.toLowerCase();

                  if (lowerCmd === "help" || lowerCmd === "?") {
                    printTuiHelp();
                    continue;
                  }

                  if (lowerCmd === "exit" || lowerCmd === "quit" || lowerCmd === "q") {
                    process.stdout.write(
                      `\nDisconnecting from session \x1b[36m${currentThreadId}\x1b[0m. State is saved on server.\n`,
                    );
                    break;
                  }

                  if (lowerCmd === "clear") {
                    process.stdout.write("\x1bc");
                    printTuiBanner({
                      origin,
                      projectTitle: project.title,
                      cwd: normalizedCwd,
                      threadId: currentThreadId,
                      threadTitle: currentThreadTitle,
                      provider: currentProvider.instanceId,
                      model: currentModelSlug,
                      runtimeMode: currentRuntimeMode,
                      interactionMode: currentInteractionMode,
                    });
                    continue;
                  }

                  if (lowerCmd === "projects" || (lowerCmd === "project" && args.length === 0)) {
                    const snap = yield* httpClient.orchestration.snapshot({
                      headers: { authorization: `Bearer ${bearerToken}` },
                    });
                    const projectList = snap.projects.filter((p) => p.deletedAt === null);
                    if (projectList.length === 0) {
                      process.stdout.write("\nNo projects found.\n\n");
                      continue;
                    }
                    process.stdout.write(`\n\x1b[1mAvailable Projects:\x1b[0m\n`);
                    const chosen = yield* promptChoice(
                      projectList.map(
                        (p) =>
                          `\x1b[1m${p.title}\x1b[0m \x1b[90m(${p.workspaceRoot})\x1b[0m${p.id === project.id ? " \x1b[32m(active)\x1b[0m" : ""}`,
                      ),
                      "project",
                    );
                    if (chosen !== undefined) {
                      project = projectList[chosen];
                      const latestThread = snap.threads
                        .filter((t) => t.projectId === project.id && t.deletedAt === null)
                        .sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0];
                      if (latestThread) {
                        currentThreadId = latestThread.id;
                        currentThreadTitle = latestThread.title;
                        if (latestThread.modelSelection)
                          currentModelSlug = latestThread.modelSelection.model;
                      } else {
                        currentThreadId = ThreadId.make(yield* randomUuid);
                        currentThreadTitle = "TUI Session";
                        yield* dispatch(wsClient, {
                          type: "thread.create",
                          threadId: currentThreadId,
                          projectId: project.id,
                          title: currentThreadTitle,
                          modelSelection: {
                            instanceId: currentProvider.instanceId,
                            model: currentModelSlug,
                          },
                          runtimeMode: currentRuntimeMode,
                          interactionMode: currentInteractionMode,
                          branch: null,
                          worktreePath: null,
                        });
                      }
                      process.stdout.write(
                        `\n\x1b[32m✔\x1b[0m Switched to project \x1b[36m[${chosen + 1}] ${project.title}\x1b[0m (Session: \x1b[36m${currentThreadId.slice(0, 8)}\x1b[0m)\n\n`,
                      );
                    }
                    continue;
                  }

                  if (lowerCmd === "project") {
                    const targetArg = args[0]?.trim();
                    const snap = yield* httpClient.orchestration.snapshot({
                      headers: { authorization: `Bearer ${bearerToken}` },
                    });
                    const projectList = snap.projects.filter((p) => p.deletedAt === null);
                    const num = parseInt(targetArg, 10);
                    let match =
                      !isNaN(num) && num >= 1 && num <= projectList.length
                        ? projectList[num - 1]
                        : projectList.find(
                            (p) =>
                              p.id === targetArg ||
                              p.title.toLowerCase().includes(targetArg.toLowerCase()) ||
                              p.workspaceRoot.toLowerCase().includes(targetArg.toLowerCase()),
                          );
                    if (!match) {
                      process.stdout.write(
                        `Project ${targetArg} not found. Type \x1b[36m/projects\x1b[0m to list.\n\n`,
                      );
                    } else {
                      project = match;
                      const latestThread = snap.threads
                        .filter((t) => t.projectId === project.id && t.deletedAt === null)
                        .sort((a, b) => b.createdAt.localeCompare(a.createdAt))[0];
                      if (latestThread) {
                        currentThreadId = latestThread.id;
                        currentThreadTitle = latestThread.title;
                        if (latestThread.modelSelection)
                          currentModelSlug = latestThread.modelSelection.model;
                      }
                      process.stdout.write(
                        `\n\x1b[32m✔\x1b[0m Switched to project \x1b[36m${project.title}\x1b[0m (${project.workspaceRoot})\n\n`,
                      );
                    }
                    continue;
                  }

                  if (lowerCmd === "export" || lowerCmd === "export-md") {
                    const detail = yield* httpClient.orchestration.threadSnapshot({
                      headers: { authorization: `Bearer ${bearerToken}` },
                      params: { threadId: currentThreadId },
                    });
                    const thread = detail.thread;
                    const defaultName = `${(thread.title || "conversation").replace(/[^a-zA-Z0-9_-]/g, "_")}.md`;
                    const targetPath = args.join(" ").trim() || defaultName;
                    const mdContent = formatThreadAsMarkdown(thread, project.title);
                    yield* Effect.promise(() => NodeFs.writeFile(targetPath, mdContent, "utf-8"));
                    process.stdout.write(
                      `\n\x1b[32m✔\x1b[0m Conversation exported to \x1b[36m${targetPath}\x1b[0m (${Buffer.byteLength(mdContent)} bytes)\n\n`,
                    );
                    continue;
                  }

                  if (lowerCmd === "status") {
                    process.stdout.write(
                      `\n\x1b[1mSession Status:\x1b[0m\n` +
                        `  Project:     ${project.title} (${project.workspaceRoot})\n` +
                        `  Session ID:  \x1b[36m${currentThreadId}\x1b[0m\n` +
                        `  Title:       ${currentThreadTitle}\n` +
                        `  Provider:    ${currentProvider.instanceId}\n` +
                        `  Model:       ${currentModelSlug}\n` +
                        `  Permission:  ${currentRuntimeMode}\n` +
                        `  Interaction: ${currentInteractionMode}\n\n`,
                    );
                    continue;
                  }

                  if (
                    lowerCmd === "sessions" ||
                    lowerCmd === "threads" ||
                    lowerCmd === "list" ||
                    (lowerCmd === "resume" && args.length === 0)
                  ) {
                    const snap = yield* httpClient.orchestration.snapshot({
                      headers: { authorization: `Bearer ${bearerToken}` },
                    });
                    const projectThreads = snap.threads
                      .filter((t) => t.projectId === project.id && t.deletedAt === null)
                      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
                      .slice(0, 15);

                    if (projectThreads.length === 0) {
                      process.stdout.write("\nNo sessions found in this project.\n\n");
                      continue;
                    }

                    process.stdout.write(`\n\x1b[1mRecent Sessions in this Project:\x1b[0m\n`);
                    const chosen = yield* promptChoice(
                      projectThreads.map(
                        (t) =>
                          `\x1b[36m${t.id.slice(0, 8)}\x1b[0m: ${t.title || "Untitled"}${t.id === currentThreadId ? " \x1b[32m(active)\x1b[0m" : ""}`,
                      ),
                      "session",
                    );
                    if (chosen !== undefined) {
                      const target = projectThreads[chosen];
                      currentThreadId = target.id;
                      currentThreadTitle = target.title;
                      if (target.modelSelection) currentModelSlug = target.modelSelection.model;
                      process.stdout.write(
                        `\n\x1b[32m✔\x1b[0m Switched to session \x1b[36m[${chosen + 1}] ${currentThreadId}\x1b[0m (${target.title})\n\n`,
                      );
                    }
                    continue;
                  }

                  if (lowerCmd === "resume" || lowerCmd === "switch") {
                    const targetArg = args[0]?.trim();
                    const snap = yield* httpClient.orchestration.snapshot({
                      headers: { authorization: `Bearer ${bearerToken}` },
                    });
                    const projectThreads = snap.threads
                      .filter((t) => t.projectId === project.id && t.deletedAt === null)
                      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
                      .slice(0, 15);

                    const num = parseInt(targetArg, 10);
                    let match =
                      !isNaN(num) && num >= 1 && num <= projectThreads.length
                        ? projectThreads[num - 1]
                        : snap.threads.find(
                            (t) =>
                              t.deletedAt === null &&
                              (t.id === targetArg ||
                                t.id.startsWith(targetArg) ||
                                t.title.toLowerCase().includes(targetArg.toLowerCase())),
                          );

                    if (!match) {
                      process.stdout.write(
                        `Session matching ${targetArg} not found. Type \x1b[36m/sessions\x1b[0m to list.\n\n`,
                      );
                    } else {
                      currentThreadId = match.id;
                      currentThreadTitle = match.title;
                      if (match.modelSelection) currentModelSlug = match.modelSelection.model;
                      process.stdout.write(
                        `\n\x1b[32m✔\x1b[0m Switched to session \x1b[36m${currentThreadId}\x1b[0m (${match.title})\n\n`,
                      );
                    }
                    continue;
                  }

                  if (lowerCmd === "new") {
                    const newTitle = args.join(" ").trim() || "TUI Session";
                    const newThreadId = ThreadId.make(yield* randomUuid);
                    yield* dispatch(wsClient, {
                      type: "thread.create",
                      threadId: newThreadId,
                      projectId: project.id,
                      title: newTitle,
                      modelSelection: {
                        instanceId: currentProvider.instanceId,
                        model: currentModelSlug,
                      },
                      runtimeMode: currentRuntimeMode,
                      interactionMode: currentInteractionMode,
                      branch: null,
                      worktreePath: null,
                    });
                    currentThreadId = newThreadId;
                    currentThreadTitle = newTitle;
                    process.stdout.write(
                      `\n\x1b[32m✔\x1b[0m Created new session \x1b[36m${currentThreadId}\x1b[0m (${newTitle})\n\n`,
                    );
                    continue;
                  }

                  if (
                    lowerCmd === "models" ||
                    lowerCmd === "providers" ||
                    (lowerCmd === "model" && args.length === 0)
                  ) {
                    const latestConfig = yield* wsClient[WS_METHODS.serverGetConfig]({});
                    const choices = getAvailableModelChoices(latestConfig.providers);
                    if (choices.length === 0) {
                      process.stdout.write("\nNo available models found.\n\n");
                      continue;
                    }
                    process.stdout.write(`\n\x1b[1mAvailable Models:\x1b[0m\n`);
                    const chosen = yield* promptChoice(
                      choices.map(
                        (c) =>
                          `\x1b[33m${c.provider.instanceId}\x1b[0m / ${c.modelSlug}${c.modelSlug === currentModelSlug && c.provider.instanceId === currentProvider.instanceId ? " \x1b[32m(active)\x1b[0m" : ""}`,
                      ),
                      "model",
                    );
                    if (chosen !== undefined) {
                      const selected = choices[chosen];
                      currentProvider = selected.provider;
                      currentModelSlug = selected.modelSlug;
                      yield* dispatch(wsClient, {
                        type: "thread.meta.update",
                        threadId: currentThreadId,
                        modelSelection: {
                          instanceId: currentProvider.instanceId,
                          model: currentModelSlug,
                        },
                      });
                      process.stdout.write(
                        `\n\x1b[32m✔\x1b[0m Switched active model to \x1b[36m[${chosen + 1}]\x1b[0m \x1b[33m${currentProvider.instanceId}/${currentModelSlug}\x1b[0m\n\n`,
                      );
                    }
                    continue;
                  }

                  if (lowerCmd === "model") {
                    const nextArg = args[0]?.trim();
                    const latestConfig = yield* wsClient[WS_METHODS.serverGetConfig]({});
                    const choices = getAvailableModelChoices(latestConfig.providers);
                    const num = parseInt(nextArg, 10);
                    let selected: ModelChoice | undefined =
                      !isNaN(num) && num >= 1 && num <= choices.length
                        ? choices[num - 1]
                        : choices.find(
                            (c) =>
                              c.modelSlug.toLowerCase() === nextArg.toLowerCase() ||
                              `${c.provider.instanceId}/${c.modelSlug}`.toLowerCase() ===
                                nextArg.toLowerCase(),
                          );
                    if (!selected) {
                      process.stdout.write(
                        `Model ${nextArg} not recognized. Type \x1b[36m/models\x1b[0m to see numbered choices.\n\n`,
                      );
                      continue;
                    }
                    currentProvider = selected.provider;
                    currentModelSlug = selected.modelSlug;
                    yield* dispatch(wsClient, {
                      type: "thread.meta.update",
                      threadId: currentThreadId,
                      modelSelection: {
                        instanceId: currentProvider.instanceId,
                        model: currentModelSlug,
                      },
                    });
                    process.stdout.write(
                      `\n\x1b[32m✔\x1b[0m Switched active model to \x1b[33m${currentProvider.instanceId}/${currentModelSlug}\x1b[0m\n\n`,
                    );
                    continue;
                  }

                  if (lowerCmd === "permission" || lowerCmd === "permissions") {
                    const modes: readonly RuntimeMode[] = [
                      "approval-required",
                      "auto-accept-edits",
                      "full-access",
                    ];
                    const modeDescs = [
                      "approval-required  \x1b[90m(Prompt before tool actions and edits)\x1b[0m",
                      "auto-accept-edits  \x1b[90m(Auto-approve file edits, prompt for shell)\x1b[0m",
                      "full-access        \x1b[90m(Auto-approve all actions)\x1b[0m",
                    ];
                    let targetMode: RuntimeMode | undefined;
                    const targetArg = args[0]?.trim();

                    if (targetArg) {
                      const num = parseInt(targetArg, 10);
                      if (num >= 1 && num <= modes.length) targetMode = modes[num - 1];
                      else if (modes.includes(targetArg as RuntimeMode))
                        targetMode = targetArg as RuntimeMode;
                    }

                    if (!targetMode) {
                      process.stdout.write(`\n\x1b[1mPermission Modes:\x1b[0m\n`);
                      const chosen = yield* promptChoice(
                        modeDescs.map(
                          (desc, idx) =>
                            `${desc}${currentRuntimeMode === modes[idx] ? " \x1b[32m(active)\x1b[0m" : ""}`,
                        ),
                        "mode",
                      );
                      if (chosen !== undefined) targetMode = modes[chosen];
                    }

                    if (targetMode) {
                      currentRuntimeMode = targetMode;
                      yield* dispatch(wsClient, {
                        type: "thread.runtime-mode.set",
                        threadId: currentThreadId,
                        runtimeMode: currentRuntimeMode,
                      });
                      process.stdout.write(
                        `\n\x1b[32m✔\x1b[0m Permission mode set to \x1b[33m${currentRuntimeMode}\x1b[0m\n\n`,
                      );
                    }
                    continue;
                  }

                  if (lowerCmd === "plan") {
                    currentInteractionMode = "plan";
                    yield* dispatch(wsClient, {
                      type: "thread.interaction-mode.set",
                      threadId: currentThreadId,
                      interactionMode: "plan",
                    });
                    process.stdout.write(
                      "\n\x1b[32m✔\x1b[0m Switched to PLAN mode (read-only architectural planning).\n\n",
                    );
                    continue;
                  }

                  if (lowerCmd === "normal") {
                    currentInteractionMode = DEFAULT_PROVIDER_INTERACTION_MODE;
                    yield* dispatch(wsClient, {
                      type: "thread.interaction-mode.set",
                      threadId: currentThreadId,
                      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
                    });
                    process.stdout.write(
                      "\n\x1b[32m✔\x1b[0m Switched to NORMAL execution mode.\n\n",
                    );
                    continue;
                  }

                  process.stdout.write(
                    `Unknown command /${command}. Type \x1b[36m/help\x1b[0m for available commands.\n`,
                  );
                  continue;
                }

                yield* executeTurn({
                  wsClient,
                  threadId: currentThreadId,
                  promptText: inputLine,
                  runtimeMode: currentRuntimeMode,
                  interactionMode: currentInteractionMode,
                  providerInstanceId: currentProvider.instanceId,
                  modelSlug: currentModelSlug,
                  showDiff: showDiffs,
                });
              }
            }),
          );
        }),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(EnvironmentAuth.runtimeLayer, WorkspacePaths.layer).pipe(
          Layer.provideMerge(FetchHttpClient.layer),
          Layer.provide(ServerConfig.layer(config)),
          Layer.provide(Layer.succeed(References.MinimumLogLevel, minimumLogLevel)),
        ),
      ),
    );
  });

const executeTurn = (opts: {
  readonly wsClient: WsRpcClient;
  readonly threadId: ThreadId;
  readonly promptText: string;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly providerInstanceId: ProviderInstanceId;
  readonly modelSlug: string;
  readonly showDiff: boolean;
}) =>
  Effect.gen(function* () {
    const turnSettledDeferred = yield* Deferred.make<void, Error>();
    const activeTurnIdRef = yield* Ref.make<TurnId | null>(null);

    const threadStream = opts.wsClient[ORCHESTRATION_WS_METHODS.subscribeThread]({
      threadId: opts.threadId,
    });

    const streamFiber = yield* Effect.forkChild(
      threadStream.pipe(
        Stream.runForEach((item) =>
          Effect.gen(function* () {
            if (item.kind !== "event") return;

            const { event } = item;
            switch (event.type) {
              case "thread.turn-start-requested": {
                yield* Ref.set(activeTurnIdRef, event.payload.turnId);
                break;
              }

              case "thread.message-sent": {
                if (event.payload.role === "assistant" && event.payload.streaming) {
                  process.stdout.write(event.payload.text);
                }
                break;
              }

              case "thread.activity-appended": {
                const { activity } = event.payload;
                if (activity.tone === "tool") {
                  process.stdout.write(`\n\x1b[36m⚙  ${activity.summary}\x1b[0m\n`);
                } else if (activity.tone === "error") {
                  process.stdout.write(`\n\x1b[31m✖  ${activity.summary}\x1b[0m\n`);
                }

                if (activity.tone === "approval" && activity.kind === "approval.requested") {
                  const requestId = (activity.payload as any)?.requestId;
                  if (!requestId) break;

                  if (opts.runtimeMode === "full-access") {
                    process.stdout.write(
                      `\n\x1b[32m✔  [Auto-approved] ${activity.summary}\x1b[0m\n`,
                    );
                    yield* dispatch(opts.wsClient, {
                      type: "thread.approval.respond",
                      threadId: opts.threadId,
                      requestId,
                      decision: "accept",
                    });
                  } else {
                    const decision = yield* askApprovalDecision(
                      activity.summary,
                      (activity.payload as any)?.detail,
                    );
                    yield* dispatch(opts.wsClient, {
                      type: "thread.approval.respond",
                      threadId: opts.threadId,
                      requestId,
                      decision,
                    });
                  }
                }
                break;
              }

              case "thread.turn-diff-completed": {
                if (opts.showDiff && event.payload.files.length > 0) {
                  process.stdout.write(
                    `\n\x1b[35m──────────────── Modified Files (${event.payload.files.length}) ────────────────\x1b[0m\n`,
                  );
                  for (const file of event.payload.files) {
                    process.stdout.write(
                      `  • ${file.path} \x1b[32m+${file.additions}\x1b[0m \x1b[31m-${file.deletions}\x1b[0m\n`,
                    );
                  }
                  process.stdout.write(
                    `\x1b[35m────────────────────────────────────────────────────────────────\x1b[0m\n\n`,
                  );
                }
                break;
              }

              case "thread.settled": {
                process.stdout.write("\n\n");
                yield* Deferred.succeed(turnSettledDeferred, undefined);
                break;
              }
            }
          }),
        ),
      ),
    );

    const userMessageId = MessageId.make(yield* randomUuid);
    yield* dispatch(opts.wsClient, {
      type: "thread.turn.start",
      threadId: opts.threadId,
      message: {
        messageId: userMessageId,
        role: "user",
        text: opts.promptText,
        attachments: [],
      },
      runtimeMode: opts.runtimeMode,
      interactionMode: opts.interactionMode,
      modelSelection: {
        instanceId: opts.providerInstanceId,
        model: opts.modelSlug,
      },
    });

    yield* Deferred.await(turnSettledDeferred).pipe(
      Effect.onInterrupt(() =>
        Effect.gen(function* () {
          const activeTurnId = yield* Ref.get(activeTurnIdRef);
          yield* dispatch(opts.wsClient, {
            type: "thread.turn.interrupt",
            threadId: opts.threadId,
            ...(activeTurnId ? { turnId: activeTurnId } : {}),
          }).pipe(Effect.ignore);
        }),
      ),
    );

    yield* Fiber.interrupt(streamFiber);
  });

export const tuiFlags = {
  baseDir: baseDirFlag,
  prompt: Argument.string("prompt").pipe(
    Argument.withDescription("Optional prompt instructions to execute upon launching TUI."),
    Argument.optional,
  ),
  cwd: Flag.string("cwd").pipe(
    Flag.withDescription("Working directory for the task (defaults to current directory)."),
    Flag.optional,
  ),
  thread: Flag.string("thread").pipe(
    Flag.withDescription("Existing thread ID to resume or continue."),
    Flag.optional,
  ),
  provider: Flag.string("provider").pipe(
    Flag.withDescription("Provider instance ID (e.g. claudeAgent, codex, antigravity)."),
    Flag.optional,
  ),
  model: Flag.string("model").pipe(
    Flag.withDescription("Model slug override (e.g. claude-3-7-sonnet)."),
    Flag.optional,
  ),
  permission: Flag.choice("permission", [
    "approval-required",
    "auto-accept-edits",
    "full-access",
  ] as const).pipe(Flag.withDescription("Runtime permission mode."), Flag.optional),
  plan: Flag.boolean("plan").pipe(
    Flag.withDescription("Run turn in plan mode."),
    Flag.withDefault(false),
  ),
  diff: Flag.boolean("diff").pipe(
    Flag.withDescription("Display file diffs when turn completes."),
    Flag.withDefault(true),
  ),
};

export const tuiCommand = Command.make("tui", tuiFlags).pipe(
  Command.withDescription("Launch the interactive T3 Code TUI harness."),
  Command.withHandler(runTuiCommand),
);
export type TuiCliOptions = Command.Command.Flags<typeof tuiCommand>;
