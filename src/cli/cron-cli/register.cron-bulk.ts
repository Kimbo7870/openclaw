import fs from "node:fs/promises";
import path from "node:path";
import type { Command } from "commander";
import { defaultRuntime } from "../../runtime.js";
import type { GatewayRpcOpts } from "../gateway-rpc.js";
import { addGatewayClientOptions, callGatewayFromCli } from "../gateway-rpc.js";
import { buildCronAddParams } from "./register.cron-add.js";
import { handleCronCliError, printCronJson, warnIfCronSchedulerDisabled } from "./shared.js";

type BulkEntryResult =
  | { ok: true; index: number; name: string; id?: string }
  | { ok: false; index: number; name?: string; error: string };

function kebabToCamel(key: string): string {
  return key.replace(/-([a-z0-9])/g, (_, c: string) => c.toUpperCase());
}

function normalizeBulkEntry(raw: unknown, index: number): Record<string, unknown> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error(`entry at index ${index} must be a JSON object`);
  }
  const out: Record<string, unknown> = {};
  const presentKeys = new Set<string>();
  for (const [rawKey, value] of Object.entries(raw)) {
    const camelKey = kebabToCamel(rawKey);
    // `--no-deliver` semantics: `"no-deliver": true` => `deliver: false`.
    if (camelKey === "noDeliver") {
      if (typeof value !== "boolean") {
        throw new Error(`entry at index ${index}: "${rawKey}" must be a boolean`);
      }
      if (presentKeys.has("deliver")) {
        throw new Error(`entry at index ${index}: cannot specify both "deliver" and "no-deliver"`);
      }
      out.deliver = !value;
      presentKeys.add("deliver");
      continue;
    }
    if (camelKey === "deliver" && presentKeys.has("deliver")) {
      throw new Error(`entry at index ${index}: cannot specify both "deliver" and "no-deliver"`);
    }
    out[camelKey] = value;
    presentKeys.add(camelKey);
  }
  return out;
}

export function registerCronBulkAddCommand(cron: Command) {
  addGatewayClientOptions(
    cron
      .command("add-bulk")
      .alias("bulk-add")
      .description("Add multiple cron jobs from a JSON file (array of cron-add option objects)")
      .argument("<file>", "Path to a JSON file containing an array of cron entries")
      .option("--json", "Output JSON summary", false)
      .action(async (file: string, opts: GatewayRpcOpts & Record<string, unknown>) => {
        try {
          const absPath = path.resolve(String(file));
          let raw: string;
          try {
            raw = await fs.readFile(absPath, "utf8");
          } catch (err) {
            throw new Error(`failed to read ${absPath}: ${(err as Error).message}`, { cause: err });
          }
          let parsed: unknown;
          try {
            parsed = JSON.parse(raw);
          } catch (err) {
            throw new Error(`invalid JSON in ${absPath}: ${(err as Error).message}`, {
              cause: err,
            });
          }
          if (!Array.isArray(parsed)) {
            throw new Error(`expected a JSON array of cron entries in ${absPath}`);
          }

          const entries = parsed;
          const results: BulkEntryResult[] = [];

          for (let i = 0; i < entries.length; i++) {
            const entry = entries[i];
            try {
              const normalized = normalizeBulkEntry(entry, i);
              const sessionExplicitlySet = "session" in normalized;
              const params = buildCronAddParams(normalized, { sessionExplicitlySet });
              const res = (await callGatewayFromCli("cron.add", opts, params)) as {
                id?: string;
              } | null;
              results.push({
                ok: true,
                index: i,
                name: params.name,
                id: typeof res?.id === "string" ? res.id : undefined,
              });
            } catch (err) {
              const entryName =
                entry &&
                typeof entry === "object" &&
                !Array.isArray(entry) &&
                typeof (entry as { name?: unknown }).name === "string"
                  ? (entry as { name: string }).name
                  : undefined;
              results.push({
                ok: false,
                index: i,
                name: entryName,
                error: (err as Error).message ?? String(err),
              });
            }
          }

          const succeeded = results.filter((r) => r.ok).length;
          const failed = results.filter((r) => !r.ok).length;
          const summary = {
            ok: failed === 0,
            file: absPath,
            total: entries.length,
            succeeded,
            failed,
            results,
          };

          if (opts.json) {
            printCronJson(summary);
          } else if (entries.length === 0) {
            defaultRuntime.log("No cron entries to add.");
          } else {
            defaultRuntime.log(
              `Added ${succeeded} of ${entries.length} cron job(s) from ${absPath}.`,
            );
            for (const r of results) {
              if (r.ok) {
                defaultRuntime.log(`  [${r.index}] ok: ${r.name}${r.id ? ` (id=${r.id})` : ""}`);
              } else {
                defaultRuntime.error(
                  `  [${r.index}] failed${r.name ? ` (${r.name})` : ""}: ${r.error}`,
                );
              }
            }
          }

          if (succeeded > 0) {
            await warnIfCronSchedulerDisabled(opts);
          }
          if (failed > 0) {
            defaultRuntime.exit(1);
          }
        } catch (err) {
          handleCronCliError(err);
        }
      }),
  );
}
