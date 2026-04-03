import fs from "node:fs/promises";
import type { Command } from "commander";
import JSON5 from "json5";
import { readBestEffortConfig, type OpenClawConfig } from "../config/config.js";
import {
  collectExecPolicyScopeSnapshots,
  type ExecPolicyScopeSnapshot,
} from "../infra/exec-approvals-effective.js";
import {
  readExecApprovalsSnapshot,
  saveExecApprovals,
  type ExecApprovalsAgent,
  type ExecApprovalsFile,
} from "../infra/exec-approvals.js";
import { resolveExecutablePath } from "../infra/executable-path.js";
import { formatTimeAgo } from "../infra/format-time/format-relative.ts";
import { defaultRuntime } from "../runtime.js";
import { formatDocsLink } from "../terminal/links.js";
import { getTerminalTableWidth, renderTable } from "../terminal/table.js";
import { isRich, theme } from "../terminal/theme.js";
import { describeUnknownError } from "./gateway-cli/shared.js";
import { callGatewayFromCli } from "./gateway-rpc.js";
import { nodesCallOpts, resolveNodeId } from "./nodes-cli/rpc.js";
import type { NodesRpcOpts } from "./nodes-cli/types.js";

type ExecApprovalsSnapshot = {
  path: string;
  exists: boolean;
  hash: string;
  file: ExecApprovalsFile;
};

type ConfigSnapshotLike = {
  config?: OpenClawConfig;
};
type ApprovalsTargetSource = "gateway" | "node" | "local";
type EffectivePolicyReport = {
  scopes: ExecPolicyScopeSnapshot[];
  note?: string;
};

type ExecApprovalsCliOpts = NodesRpcOpts & {
  node?: string;
  gateway?: boolean;
  file?: string;
  stdin?: boolean;
  agent?: string;
};

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function resolveTargetNodeId(opts: ExecApprovalsCliOpts): Promise<string | null> {
  if (opts.gateway) {
    return null;
  }
  const raw = opts.node?.trim() ?? "";
  if (!raw) {
    return null;
  }
  return await resolveNodeId(opts as NodesRpcOpts, raw);
}

async function loadSnapshot(
  opts: ExecApprovalsCliOpts,
  nodeId: string | null,
): Promise<ExecApprovalsSnapshot> {
  const method = nodeId ? "exec.approvals.node.get" : "exec.approvals.get";
  const params = nodeId ? { nodeId } : {};
  const snapshot = (await callGatewayFromCli(method, opts, params)) as ExecApprovalsSnapshot;
  return snapshot;
}

function loadSnapshotLocal(): ExecApprovalsSnapshot {
  const snapshot = readExecApprovalsSnapshot();
  return {
    path: snapshot.path,
    exists: snapshot.exists,
    hash: snapshot.hash,
    file: snapshot.file,
  };
}

function saveSnapshotLocal(file: ExecApprovalsFile): ExecApprovalsSnapshot {
  saveExecApprovals(file);
  return loadSnapshotLocal();
}

async function loadSnapshotTarget(opts: ExecApprovalsCliOpts): Promise<{
  snapshot: ExecApprovalsSnapshot;
  nodeId: string | null;
  source: ApprovalsTargetSource;
}> {
  if (!opts.gateway && !opts.node) {
    return { snapshot: loadSnapshotLocal(), nodeId: null, source: "local" };
  }
  const nodeId = await resolveTargetNodeId(opts);
  const snapshot = await loadSnapshot(opts, nodeId);
  return { snapshot, nodeId, source: nodeId ? "node" : "gateway" };
}

function exitWithError(message: string): never {
  defaultRuntime.error(message);
  defaultRuntime.exit(1);
  throw new Error(message);
}

function requireTrimmedNonEmpty(value: string, message: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    exitWithError(message);
  }
  return trimmed;
}

async function loadWritableSnapshotTarget(opts: ExecApprovalsCliOpts): Promise<{
  snapshot: ExecApprovalsSnapshot;
  nodeId: string | null;
  source: ApprovalsTargetSource;
  targetLabel: string;
  baseHash: string;
}> {
  const { snapshot, nodeId, source } = await loadSnapshotTarget(opts);
  if (source === "local") {
    defaultRuntime.log(theme.muted("Writing local approvals."));
  }
  const targetLabel = source === "local" ? "local" : nodeId ? `node:${nodeId}` : "gateway";
  const baseHash = snapshot.hash;
  if (!baseHash) {
    exitWithError("Exec approvals hash missing; reload and retry.");
  }
  return { snapshot, nodeId, source, targetLabel, baseHash };
}

async function saveSnapshotTargeted(params: {
  opts: ExecApprovalsCliOpts;
  source: ApprovalsTargetSource;
  nodeId: string | null;
  file: ExecApprovalsFile;
  baseHash: string;
  targetLabel: string;
}): Promise<void> {
  const next =
    params.source === "local"
      ? saveSnapshotLocal(params.file)
      : await saveSnapshot(params.opts, params.nodeId, params.file, params.baseHash);
  if (params.opts.json) {
    defaultRuntime.writeJson(next, 0);
    return;
  }
  defaultRuntime.log(theme.muted(`Target: ${params.targetLabel}`));
  renderApprovalsSnapshot(next, params.targetLabel);
}

function formatCliError(err: unknown): string {
  const msg = describeUnknownError(err);
  return msg.includes("\n") ? msg.split("\n")[0] : msg;
}

async function loadConfigForApprovalsTarget(params: {
  opts: ExecApprovalsCliOpts;
  source: ApprovalsTargetSource;
}): Promise<OpenClawConfig | null> {
  try {
    if (params.source === "local") {
      return await readBestEffortConfig();
    }
    const snapshot = (await callGatewayFromCli(
      "config.get",
      params.opts,
      {},
    )) as ConfigSnapshotLike;
    return snapshot.config && typeof snapshot.config === "object" ? snapshot.config : null;
  } catch {
    return null;
  }
}

function buildEffectivePolicyReport(params: {
  cfg: OpenClawConfig | null;
  source: ApprovalsTargetSource;
  approvals: ExecApprovalsFile;
  hostPath: string;
}): EffectivePolicyReport {
  if (params.source === "node") {
    if (!params.cfg) {
      return {
        scopes: [],
        note: "Gateway config unavailable. Node output above shows host approvals state only, and final runtime policy still intersects with gateway tools.exec.",
      };
    }
    return {
      scopes: collectExecPolicyScopeSnapshots({
        cfg: params.cfg,
        approvals: params.approvals,
        hostPath: params.hostPath,
      }),
      note: "Effective exec policy is the node host approvals file intersected with gateway tools.exec policy.",
    };
  }
  if (!params.cfg) {
    return {
      scopes: [],
      note: "Config unavailable.",
    };
  }
  return {
    scopes: collectExecPolicyScopeSnapshots({
      cfg: params.cfg,
      approvals: params.approvals,
      hostPath: params.hostPath,
    }),
    note: "Effective exec policy is the host approvals file intersected with requested tools.exec policy.",
  };
}

function renderEffectivePolicy(params: { report: EffectivePolicyReport }) {
  const rich = isRich();
  const heading = (text: string) => (rich ? theme.heading(text) : text);
  const muted = (text: string) => (rich ? theme.muted(text) : text);
  if (params.report.scopes.length === 0 && !params.report.note) {
    return;
  }
  defaultRuntime.log("");
  defaultRuntime.log(heading("Effective Policy"));
  if (params.report.scopes.length === 0) {
    defaultRuntime.log(muted(params.report.note ?? "No effective policy details available."));
    return;
  }
  const rows = params.report.scopes.map((summary) => ({
    Scope: summary.scopeLabel,
    Requested: `security=${summary.security.requested} (${summary.security.requestedSource})\nask=${summary.ask.requested} (${summary.ask.requestedSource})`,
    Host: `security=${summary.security.host} (${summary.security.hostSource})\nask=${summary.ask.host} (${summary.ask.hostSource})\naskFallback=${summary.askFallback.effective} (${summary.askFallback.source})`,
    Effective: `security=${summary.security.effective}\nask=${summary.ask.effective}`,
    Notes: `${summary.security.note}; ${summary.ask.note}`,
  }));
  defaultRuntime.log(
    renderTable({
      width: getTerminalTableWidth(),
      columns: [
        { key: "Scope", header: "Scope", minWidth: 12 },
        { key: "Requested", header: "Requested", minWidth: 24, flex: true },
        { key: "Host", header: "Host", minWidth: 24, flex: true },
        { key: "Effective", header: "Effective", minWidth: 16 },
        { key: "Notes", header: "Notes", minWidth: 20, flex: true },
      ],
      rows,
    }).trimEnd(),
  );
  defaultRuntime.log("");
  defaultRuntime.log(muted(`Precedence: ${params.report.note}`));
}

function renderApprovalsSnapshot(snapshot: ExecApprovalsSnapshot, targetLabel: string) {
  const rich = isRich();
  const heading = (text: string) => (rich ? theme.heading(text) : text);
  const muted = (text: string) => (rich ? theme.muted(text) : text);
  const tableWidth = getTerminalTableWidth();

  const file = snapshot.file ?? { version: 1 };
  const defaults = file.defaults ?? {};
  const defaultsParts = [
    defaults.security ? `security=${defaults.security}` : null,
    defaults.ask ? `ask=${defaults.ask}` : null,
    defaults.askFallback ? `askFallback=${defaults.askFallback}` : null,
    typeof defaults.autoAllowSkills === "boolean"
      ? `autoAllowSkills=${defaults.autoAllowSkills ? "on" : "off"}`
      : null,
  ].filter(Boolean) as string[];
  const agents = file.agents ?? {};
  const allowlistRows: Array<{ Target: string; Agent: string; Pattern: string; LastUsed: string }> =
    [];
  const now = Date.now();
  for (const [agentId, agent] of Object.entries(agents)) {
    const allowlist = Array.isArray(agent.allowlist) ? agent.allowlist : [];
    for (const entry of allowlist) {
      const pattern = entry?.pattern?.trim() ?? "";
      if (!pattern) {
        continue;
      }
      const lastUsedAt = typeof entry.lastUsedAt === "number" ? entry.lastUsedAt : null;
      allowlistRows.push({
        Target: targetLabel,
        Agent: agentId,
        Pattern: pattern,
        LastUsed: lastUsedAt ? formatTimeAgo(Math.max(0, now - lastUsedAt)) : muted("unknown"),
      });
    }
  }

  const summaryRows = [
    { Field: "Target", Value: targetLabel },
    { Field: "Path", Value: snapshot.path },
    { Field: "Exists", Value: snapshot.exists ? "yes" : "no" },
    { Field: "Hash", Value: snapshot.hash },
    { Field: "Version", Value: String(file.version ?? 1) },
    { Field: "Socket", Value: file.socket?.path ?? "default" },
    { Field: "Defaults", Value: defaultsParts.length > 0 ? defaultsParts.join(", ") : "none" },
    { Field: "Agents", Value: String(Object.keys(agents).length) },
    { Field: "Allowlist", Value: String(allowlistRows.length) },
  ];

  defaultRuntime.log(heading("Approvals"));
  defaultRuntime.log(
    renderTable({
      width: tableWidth,
      columns: [
        { key: "Field", header: "Field", minWidth: 8 },
        { key: "Value", header: "Value", minWidth: 24, flex: true },
      ],
      rows: summaryRows,
    }).trimEnd(),
  );

  if (allowlistRows.length === 0) {
    defaultRuntime.log("");
    defaultRuntime.log(muted("No allowlist entries."));
    return;
  }

  defaultRuntime.log("");
  defaultRuntime.log(heading("Allowlist"));
  defaultRuntime.log(
    renderTable({
      width: tableWidth,
      columns: [
        { key: "Target", header: "Target", minWidth: 10 },
        { key: "Agent", header: "Agent", minWidth: 8 },
        { key: "Pattern", header: "Pattern", minWidth: 20, flex: true },
        { key: "LastUsed", header: "Last Used", minWidth: 10 },
      ],
      rows: allowlistRows,
    }).trimEnd(),
  );
}

async function saveSnapshot(
  opts: ExecApprovalsCliOpts,
  nodeId: string | null,
  file: ExecApprovalsFile,
  baseHash: string,
): Promise<ExecApprovalsSnapshot> {
  const method = nodeId ? "exec.approvals.node.set" : "exec.approvals.set";
  const params = nodeId ? { nodeId, file, baseHash } : { file, baseHash };
  const snapshot = (await callGatewayFromCli(method, opts, params)) as ExecApprovalsSnapshot;
  return snapshot;
}

function resolveAgentKey(value?: string | null): string {
  const trimmed = value?.trim() ?? "";
  return trimmed ? trimmed : "*";
}

function normalizeAllowlistEntry(entry: { pattern?: string } | null): string | null {
  const pattern = entry?.pattern?.trim() ?? "";
  return pattern ? pattern : null;
}

function ensureAgent(file: ExecApprovalsFile, agentKey: string): ExecApprovalsAgent {
  const agents = file.agents ?? {};
  const entry = agents[agentKey] ?? {};
  file.agents = agents;
  return entry;
}

function isEmptyAgent(agent: ExecApprovalsAgent): boolean {
  const allowlist = Array.isArray(agent.allowlist) ? agent.allowlist : [];
  return (
    !agent.security &&
    !agent.ask &&
    !agent.askFallback &&
    agent.autoAllowSkills === undefined &&
    allowlist.length === 0
  );
}

async function loadWritableAllowlistAgent(opts: ExecApprovalsCliOpts): Promise<{
  nodeId: string | null;
  source: "gateway" | "node" | "local";
  targetLabel: string;
  baseHash: string;
  file: ExecApprovalsFile;
  agentKey: string;
  agent: ExecApprovalsAgent;
  allowlistEntries: NonNullable<ExecApprovalsAgent["allowlist"]>;
}> {
  const { snapshot, nodeId, source, targetLabel, baseHash } =
    await loadWritableSnapshotTarget(opts);
  const file = snapshot.file ?? { version: 1 };
  file.version = 1;

  const agentKey = resolveAgentKey(opts.agent);
  const agent = ensureAgent(file, agentKey);
  const allowlistEntries = Array.isArray(agent.allowlist) ? agent.allowlist : [];

  return { nodeId, source, targetLabel, baseHash, file, agentKey, agent, allowlistEntries };
}

type WritableAllowlistAgentContext = Awaited<ReturnType<typeof loadWritableAllowlistAgent>> & {
  trimmedPattern: string;
};
type AllowlistMutation = (context: WritableAllowlistAgentContext) => boolean | Promise<boolean>;

async function runAllowlistMutation(
  pattern: string,
  opts: ExecApprovalsCliOpts,
  mutate: AllowlistMutation,
): Promise<void> {
  try {
    const trimmedPattern = requireTrimmedNonEmpty(pattern, "Pattern required.");
    const context = await loadWritableAllowlistAgent(opts);
    const shouldSave = await mutate({ ...context, trimmedPattern });
    if (!shouldSave) {
      return;
    }
    await saveSnapshotTargeted({
      opts,
      source: context.source,
      nodeId: context.nodeId,
      file: context.file,
      baseHash: context.baseHash,
      targetLabel: context.targetLabel,
    });
  } catch (err) {
    defaultRuntime.error(formatCliError(err));
    defaultRuntime.exit(1);
  }
}

function registerAllowlistMutationCommand(params: {
  allowlist: Command;
  name: "add" | "remove";
  description: string;
  mutate: AllowlistMutation;
}): Command {
  const command = params.allowlist
    .command(`${params.name} <pattern>`)
    .description(params.description)
    .option("--node <node>", "Target node id/name/IP")
    .option("--gateway", "Force gateway approvals", false)
    .option("--agent <id>", 'Agent id (defaults to "*")')
    .action(async (pattern: string, opts: ExecApprovalsCliOpts) => {
      await runAllowlistMutation(pattern, opts, params.mutate);
    });
  nodesCallOpts(command);
  return command;
}

export function registerExecApprovalsCli(program: Command) {
  const formatExample = (cmd: string, desc: string) =>
    `  ${theme.command(cmd)}\n    ${theme.muted(desc)}`;

  const approvals = program
    .command("approvals")
    .alias("exec-approvals")
    .description("Manage exec approvals (gateway or node host)")
    .addHelpText(
      "after",
      () =>
        `\n${theme.muted("Docs:")} ${formatDocsLink("/cli/approvals", "docs.openclaw.ai/cli/approvals")}\n`,
    );

  const getCmd = approvals
    .command("get")
    .description("Fetch exec approvals snapshot")
    .option("--node <node>", "Target node id/name/IP")
    .option("--gateway", "Force gateway approvals", false)
    .action(async (opts: ExecApprovalsCliOpts) => {
      try {
        const { snapshot, nodeId, source } = await loadSnapshotTarget(opts);
        const cfg = await loadConfigForApprovalsTarget({ opts, source });
        const effectivePolicy = buildEffectivePolicyReport({
          cfg,
          source,
          approvals: snapshot.file,
          hostPath: snapshot.path,
        });
        if (opts.json) {
          defaultRuntime.writeJson({ ...snapshot, effectivePolicy }, 0);
          return;
        }

        const muted = (text: string) => (isRich() ? theme.muted(text) : text);
        if (source === "local") {
          defaultRuntime.log(muted("Showing local approvals."));
          defaultRuntime.log("");
        }
        const targetLabel = source === "local" ? "local" : nodeId ? `node:${nodeId}` : "gateway";
        renderApprovalsSnapshot(snapshot, targetLabel);
        renderEffectivePolicy({ report: effectivePolicy });
      } catch (err) {
        defaultRuntime.error(formatCliError(err));
        defaultRuntime.exit(1);
      }
    });
  nodesCallOpts(getCmd);

  const setCmd = approvals
    .command("set")
    .description("Replace exec approvals with a JSON file")
    .option("--node <node>", "Target node id/name/IP")
    .option("--gateway", "Force gateway approvals", false)
    .option("--file <path>", "Path to JSON file to upload")
    .option("--stdin", "Read JSON from stdin", false)
    .action(async (opts: ExecApprovalsCliOpts) => {
      try {
        if (!opts.file && !opts.stdin) {
          exitWithError("Provide --file or --stdin.");
        }
        if (opts.file && opts.stdin) {
          exitWithError("Use either --file or --stdin (not both).");
        }
        const { source, nodeId, targetLabel, baseHash } = await loadWritableSnapshotTarget(opts);
        const raw = opts.stdin ? await readStdin() : await fs.readFile(String(opts.file), "utf8");
        let file: ExecApprovalsFile;
        try {
          file = JSON5.parse(raw);
        } catch (err) {
          exitWithError(`Failed to parse approvals JSON: ${String(err)}`);
        }
        file.version = 1;
        await saveSnapshotTargeted({ opts, source, nodeId, file, baseHash, targetLabel });
      } catch (err) {
        defaultRuntime.error(formatCliError(err));
        defaultRuntime.exit(1);
      }
    });
  nodesCallOpts(setCmd);

  const allowlist = approvals
    .command("allowlist")
    .description("Edit the per-agent allowlist")
    .addHelpText(
      "after",
      () =>
        `\n${theme.heading("Examples:")}\n${formatExample(
          'openclaw approvals allowlist add "~/Projects/**/bin/rg"',
          "Allowlist a local binary pattern for the main agent.",
        )}\n${formatExample(
          'openclaw approvals allowlist add --agent main --node <id|name|ip> "/usr/bin/uptime"',
          "Allowlist on a specific node/agent.",
        )}\n${formatExample(
          'openclaw approvals allowlist add --agent "*" "/usr/bin/uname"',
          "Allowlist for all agents (wildcard).",
        )}\n${formatExample(
          'openclaw approvals allowlist remove "~/Projects/**/bin/rg"',
          "Remove an allowlist pattern.",
        )}\n\n${theme.muted("Docs:")} ${formatDocsLink("/cli/approvals", "docs.openclaw.ai/cli/approvals")}\n`,
    );

  registerAllowlistMutationCommand({
    allowlist,
    name: "add",
    description: "Add a glob pattern to an allowlist",
    mutate: ({ trimmedPattern, file, agent, agentKey, allowlistEntries }) => {
      if (allowlistEntries.some((entry) => normalizeAllowlistEntry(entry) === trimmedPattern)) {
        defaultRuntime.log("Already allowlisted.");
        return false;
      }
      allowlistEntries.push({ pattern: trimmedPattern, lastUsedAt: Date.now() });
      agent.allowlist = allowlistEntries;
      file.agents = { ...file.agents, [agentKey]: agent };
      return true;
    },
  });

  registerAllowlistMutationCommand({
    allowlist,
    name: "remove",
    description: "Remove a glob pattern from an allowlist",
    mutate: ({ trimmedPattern, file, agent, agentKey, allowlistEntries }) => {
      const nextEntries = allowlistEntries.filter(
        (entry) => normalizeAllowlistEntry(entry) !== trimmedPattern,
      );
      if (nextEntries.length === allowlistEntries.length) {
        defaultRuntime.log("Pattern not found.");
        return false;
      }
      if (nextEntries.length === 0) {
        delete agent.allowlist;
      } else {
        agent.allowlist = nextEntries;
      }
      if (isEmptyAgent(agent)) {
        const agents = { ...file.agents };
        delete agents[agentKey];
        file.agents = Object.keys(agents).length > 0 ? agents : undefined;
      } else {
        file.agents = { ...file.agents, [agentKey]: agent };
      }
      return true;
    },
  });

  const removeAllCmd = allowlist
    .command("remove-all")
    .alias("rm-all")
    .alias("delete-all")
    .description("Remove every allowlist entry (optionally scoped to one agent)")
    .option("--node <node>", "Target node id/name/IP")
    .option("--gateway", "Force gateway approvals", false)
    .option("--agent <id>", "Limit to a single agent id (defaults to all agents)")
    .action(async (opts: ExecApprovalsCliOpts) => {
      try {
        const { snapshot, nodeId, source, targetLabel, baseHash } =
          await loadWritableSnapshotTarget(opts);
        const file = snapshot.file ?? { version: 1 };
        file.version = 1;
        const agents = file.agents ?? {};

        const targetAgentKey =
          typeof opts.agent === "string" && opts.agent.trim() ? opts.agent.trim() : null;
        const agentKeys = targetAgentKey
          ? Object.keys(agents).includes(targetAgentKey)
            ? [targetAgentKey]
            : []
          : Object.keys(agents);

        let removedCount = 0;
        const nextAgents: Record<string, ExecApprovalsAgent> = { ...agents };
        for (const agentKey of agentKeys) {
          const agent = nextAgents[agentKey];
          if (!agent) {
            continue;
          }
          const allowlistEntries = Array.isArray(agent.allowlist) ? agent.allowlist : [];
          if (allowlistEntries.length === 0) {
            continue;
          }
          removedCount += allowlistEntries.length;
          delete agent.allowlist;
          if (isEmptyAgent(agent)) {
            delete nextAgents[agentKey];
          } else {
            nextAgents[agentKey] = agent;
          }
        }

        if (removedCount === 0) {
          if (opts.json) {
            defaultRuntime.writeJson({ removed: 0, target: targetLabel }, 0);
          } else {
            defaultRuntime.log("No allowlist entries to remove.");
          }
          return;
        }

        file.agents = Object.keys(nextAgents).length > 0 ? nextAgents : undefined;
        await saveSnapshotTargeted({ opts, source, nodeId, file, baseHash, targetLabel });
        if (!opts.json) {
          const scope = targetAgentKey ? ` for agent ${targetAgentKey}` : "";
          defaultRuntime.log(`Removed ${removedCount} allowlist entry(ies)${scope}.`);
        }
      } catch (err) {
        defaultRuntime.error(formatCliError(err));
        defaultRuntime.exit(1);
      }
    });
  nodesCallOpts(removeAllCmd);

  type BulkResolution =
    | { ok: true; name: string; path: string }
    | { ok: false; name: string; error: string };

  const resolveExecNames = (names: string[]): BulkResolution[] => {
    const seenPaths = new Set<string>();
    const results: BulkResolution[] = [];
    for (const raw of names) {
      const name = raw.trim();
      if (!name) {
        continue;
      }
      const resolved = resolveExecutablePath(name);
      if (!resolved) {
        results.push({ ok: false, name, error: "not found on PATH" });
        continue;
      }
      if (seenPaths.has(resolved)) {
        // Skip duplicates resolved to the same binary so the summary stays accurate.
        continue;
      }
      seenPaths.add(resolved);
      results.push({ ok: true, name, path: resolved });
    }
    return results;
  };

  type BulkOutcome = {
    name: string;
    path: string;
    status: "added" | "already" | "removed" | "missing";
  };

  const printBulkSummary = (params: {
    json: boolean | undefined;
    targetLabel: string;
    action: "added" | "removed";
    resolutions: BulkResolution[];
    outcomes: BulkOutcome[];
  }) => {
    const successCount = params.outcomes.filter((o) =>
      params.action === "added" ? o.status === "added" : o.status === "removed",
    ).length;
    const failures = params.resolutions.filter(
      (r): r is Extract<BulkResolution, { ok: false }> => !r.ok,
    );
    if (params.json) {
      defaultRuntime.writeJson(
        {
          ok: failures.length === 0,
          target: params.targetLabel,
          action: params.action,
          succeeded: successCount,
          outcomes: params.outcomes,
          unresolved: failures,
        },
        0,
      );
      return;
    }
    const verb = params.action === "added" ? "Added" : "Removed";
    defaultRuntime.log(
      `${verb} ${successCount} of ${params.resolutions.length} command(s) on ${params.targetLabel}.`,
    );
    for (const o of params.outcomes) {
      defaultRuntime.log(`  ${o.name} -> ${o.path} (${o.status})`);
    }
    for (const f of failures) {
      defaultRuntime.error(`  ${f.name}: ${f.error}`);
    }
  };

  const addBulkCmd = allowlist
    .command("add-bulk")
    .alias("bulk-add")
    .description(
      "Resolve each exec command name on PATH and add the absolute path to the allowlist",
    )
    .argument("<names...>", "Exec command names (e.g. python3 ls grep cat sed)")
    .option("--node <node>", "Target node id/name/IP")
    .option("--gateway", "Force gateway approvals", false)
    .option("--agent <id>", 'Agent id (defaults to "*")')
    .action(async (names: string[], opts: ExecApprovalsCliOpts) => {
      try {
        const resolutions = resolveExecNames(names);
        const successes = resolutions.filter(
          (r): r is Extract<BulkResolution, { ok: true }> => r.ok,
        );
        if (successes.length === 0) {
          if (opts.json) {
            defaultRuntime.writeJson(
              { ok: false, action: "added", succeeded: 0, outcomes: [], unresolved: resolutions },
              0,
            );
          } else {
            defaultRuntime.error("No exec commands resolved on PATH; nothing to add.");
            for (const r of resolutions) {
              if (!r.ok) {
                defaultRuntime.error(`  ${r.name}: ${r.error}`);
              }
            }
          }
          defaultRuntime.exit(1);
          return;
        }
        const ctx = await loadWritableAllowlistAgent(opts);
        const existing = new Set(
          ctx.allowlistEntries
            .map((entry) => normalizeAllowlistEntry(entry))
            .filter((p): p is string => Boolean(p)),
        );
        const outcomes: BulkOutcome[] = [];
        let mutated = false;
        const now = Date.now();
        for (const r of successes) {
          if (existing.has(r.path)) {
            outcomes.push({ name: r.name, path: r.path, status: "already" });
            continue;
          }
          ctx.allowlistEntries.push({ pattern: r.path, lastUsedAt: now });
          existing.add(r.path);
          outcomes.push({ name: r.name, path: r.path, status: "added" });
          mutated = true;
        }
        if (mutated) {
          ctx.agent.allowlist = ctx.allowlistEntries;
          ctx.file.agents = { ...ctx.file.agents, [ctx.agentKey]: ctx.agent };
          await saveSnapshotTargeted({
            opts,
            source: ctx.source,
            nodeId: ctx.nodeId,
            file: ctx.file,
            baseHash: ctx.baseHash,
            targetLabel: ctx.targetLabel,
          });
        }
        printBulkSummary({
          json: opts.json,
          targetLabel: ctx.targetLabel,
          action: "added",
          resolutions,
          outcomes,
        });
        if (resolutions.some((r) => !r.ok)) {
          defaultRuntime.exit(1);
        }
      } catch (err) {
        defaultRuntime.error(formatCliError(err));
        defaultRuntime.exit(1);
      }
    });
  nodesCallOpts(addBulkCmd);

  const removeBulkCmd = allowlist
    .command("remove-bulk")
    .alias("bulk-remove")
    .alias("rm-bulk")
    .description(
      "Resolve each exec command name on PATH and remove the absolute path from the allowlist",
    )
    .argument("<names...>", "Exec command names (e.g. python3 ls grep cat sed)")
    .option("--node <node>", "Target node id/name/IP")
    .option("--gateway", "Force gateway approvals", false)
    .option("--agent <id>", 'Agent id (defaults to "*")')
    .action(async (names: string[], opts: ExecApprovalsCliOpts) => {
      try {
        const resolutions = resolveExecNames(names);
        const successes = resolutions.filter(
          (r): r is Extract<BulkResolution, { ok: true }> => r.ok,
        );
        if (successes.length === 0) {
          if (opts.json) {
            defaultRuntime.writeJson(
              { ok: false, action: "removed", succeeded: 0, outcomes: [], unresolved: resolutions },
              0,
            );
          } else {
            defaultRuntime.error("No exec commands resolved on PATH; nothing to remove.");
            for (const r of resolutions) {
              if (!r.ok) {
                defaultRuntime.error(`  ${r.name}: ${r.error}`);
              }
            }
          }
          defaultRuntime.exit(1);
          return;
        }
        const ctx = await loadWritableAllowlistAgent(opts);
        const removeSet = new Set(successes.map((r) => r.path));
        const outcomes: BulkOutcome[] = [];
        const nextEntries = ctx.allowlistEntries.filter((entry) => {
          const pattern = normalizeAllowlistEntry(entry);
          return !(pattern && removeSet.has(pattern));
        });
        const removed = ctx.allowlistEntries.length - nextEntries.length;
        const removedPaths = new Set<string>();
        for (const entry of ctx.allowlistEntries) {
          const pattern = normalizeAllowlistEntry(entry);
          if (pattern && removeSet.has(pattern)) {
            removedPaths.add(pattern);
          }
        }
        for (const r of successes) {
          outcomes.push({
            name: r.name,
            path: r.path,
            status: removedPaths.has(r.path) ? "removed" : "missing",
          });
        }
        if (removed > 0) {
          if (nextEntries.length === 0) {
            delete ctx.agent.allowlist;
          } else {
            ctx.agent.allowlist = nextEntries;
          }
          if (isEmptyAgent(ctx.agent)) {
            const agents = { ...ctx.file.agents };
            delete agents[ctx.agentKey];
            ctx.file.agents = Object.keys(agents).length > 0 ? agents : undefined;
          } else {
            ctx.file.agents = { ...ctx.file.agents, [ctx.agentKey]: ctx.agent };
          }
          await saveSnapshotTargeted({
            opts,
            source: ctx.source,
            nodeId: ctx.nodeId,
            file: ctx.file,
            baseHash: ctx.baseHash,
            targetLabel: ctx.targetLabel,
          });
        }
        printBulkSummary({
          json: opts.json,
          targetLabel: ctx.targetLabel,
          action: "removed",
          resolutions,
          outcomes,
        });
        if (resolutions.some((r) => !r.ok)) {
          defaultRuntime.exit(1);
        }
      } catch (err) {
        defaultRuntime.error(formatCliError(err));
        defaultRuntime.exit(1);
      }
    });
  nodesCallOpts(removeBulkCmd);
}
