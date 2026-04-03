import type { Command } from "commander";
import type { CronJob } from "../../cron/types.js";
import { defaultRuntime } from "../../runtime.js";
import { addGatewayClientOptions, callGatewayFromCli } from "../gateway-rpc.js";
import { handleCronCliError, printCronJson, warnIfCronSchedulerDisabled } from "./shared.js";

function registerCronToggleCommand(params: {
  cron: Command;
  name: "enable" | "disable";
  description: string;
  enabled: boolean;
}) {
  addGatewayClientOptions(
    params.cron
      .command(params.name)
      .description(params.description)
      .argument("<id>", "Job id")
      .action(async (id, opts) => {
        try {
          const res = await callGatewayFromCli("cron.update", opts, {
            id,
            patch: { enabled: params.enabled },
          });
          printCronJson(res);
          await warnIfCronSchedulerDisabled(opts);
        } catch (err) {
          handleCronCliError(err);
        }
      }),
  );
}

export function registerCronSimpleCommands(cron: Command) {
  addGatewayClientOptions(
    cron
      .command("rm")
      .alias("remove")
      .alias("delete")
      .description("Remove a cron job")
      .argument("<id>", "Job id")
      .option("--json", "Output JSON", false)
      .action(async (id, opts) => {
        try {
          const res = await callGatewayFromCli("cron.remove", opts, { id });
          printCronJson(res);
        } catch (err) {
          handleCronCliError(err);
        }
      }),
  );

  addGatewayClientOptions(
    cron
      .command("remove-all")
      .alias("rm-all")
      .alias("delete-all")
      .description("Remove all cron jobs")
      .option("--json", "Output JSON", false)
      .action(async (opts) => {
        try {
          const listRes = await callGatewayFromCli("cron.list", opts, {
            includeDisabled: true,
          });
          const jobs = (listRes as { jobs?: CronJob[] } | null)?.jobs ?? [];
          const removed: string[] = [];
          const failed: Array<{ id: string; error: string }> = [];
          for (const job of jobs) {
            try {
              const res = (await callGatewayFromCli("cron.remove", opts, { id: job.id })) as {
                removed?: boolean;
              } | null;
              if (res?.removed) {
                removed.push(job.id);
              } else {
                failed.push({ id: job.id, error: "not removed" });
              }
            } catch (err) {
              failed.push({ id: job.id, error: String(err) });
            }
          }
          const summary = { ok: failed.length === 0, total: jobs.length, removed, failed };
          if (opts.json) {
            printCronJson(summary);
          } else if (jobs.length === 0) {
            defaultRuntime.log("No cron jobs to remove.");
          } else {
            defaultRuntime.log(`Removed ${removed.length} of ${jobs.length} cron job(s).`);
            for (const entry of failed) {
              defaultRuntime.error(`failed to remove ${entry.id}: ${entry.error}`);
            }
          }
          if (failed.length > 0) {
            defaultRuntime.exit(1);
          }
        } catch (err) {
          handleCronCliError(err);
        }
      }),
  );

  registerCronToggleCommand({
    cron,
    name: "enable",
    description: "Enable a cron job",
    enabled: true,
  });
  registerCronToggleCommand({
    cron,
    name: "disable",
    description: "Disable a cron job",
    enabled: false,
  });

  addGatewayClientOptions(
    cron
      .command("runs")
      .description("Show cron run history (JSONL-backed)")
      .requiredOption("--id <id>", "Job id")
      .option("--limit <n>", "Max entries (default 50)", "50")
      .action(async (opts) => {
        try {
          const limitRaw = Number.parseInt(String(opts.limit ?? "50"), 10);
          const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 50;
          const id = String(opts.id);
          const res = await callGatewayFromCli("cron.runs", opts, {
            id,
            limit,
          });
          printCronJson(res);
        } catch (err) {
          handleCronCliError(err);
        }
      }),
  );

  addGatewayClientOptions(
    cron
      .command("run")
      .description("Run a cron job now (debug)")
      .argument("<id>", "Job id")
      .option("--due", "Run only when due (default behavior in older versions)", false)
      .action(async (id, opts, command) => {
        try {
          if (command.getOptionValueSource("timeout") === "default") {
            opts.timeout = "600000";
          }
          const res = await callGatewayFromCli("cron.run", opts, {
            id,
            mode: opts.due ? "due" : "force",
          });
          printCronJson(res);
          const result = res as { ok?: boolean; ran?: boolean; enqueued?: boolean } | undefined;
          defaultRuntime.exit(result?.ok && (result?.ran || result?.enqueued) ? 0 : 1);
        } catch (err) {
          handleCronCliError(err);
        }
      }),
  );
}
