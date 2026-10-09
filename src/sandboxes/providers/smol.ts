import { randomUUID } from "node:crypto";
import { createServer } from "node:net";
import { posix } from "node:path";
import { Machine, type MachineSummary } from "smolmachines";

import { SandboxAdapter } from "../base";
import {
  SandboxProvider,
  type AsyncCommandHandle,
  type CommandEvent,
  type CommandOptions,
  type CommandResult,
  type SandboxDescriptor,
  type SandboxListOptions,
  type SmolSandboxOptions,
} from "../types";
import { collectAllAgentReservedPorts } from "../../agents/ports";
import { AsyncQueue } from "../../shared/async-queue";
import { suppressUnhandledRejection } from "../../shared/errors";
import { toShellCommand } from "../../shared/shell";

export type SmolRaw = { machine?: Machine };
const connection = {
  target: "local",
  handleSignals: false,
  waitForPorts: false,
} as const;
const ownerLabel = { "agentbox.provider": "smol" };

// Smol requires explicit host ports; zero does not allocate an ephemeral one.
async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const listener = createServer();
    listener.once("error", reject);
    listener.listen(0, "127.0.0.1", () => {
      const address = listener.address();
      listener.close(() => {
        if (address && typeof address !== "string") resolve(address.port);
        else reject(new Error("Could not allocate a local Smol port."));
      });
    });
  });
}

export class SmolSandboxAdapter extends SandboxAdapter<
  "smol",
  SmolSandboxOptions,
  SmolRaw
> {
  private machine?: Machine;
  private readonly declaredPorts = new Set<number>();

  get provider(): "smol" {
    return SandboxProvider.Smol;
  }
  get raw(): SmolRaw {
    return { machine: this.machine };
  }
  get id(): string | undefined {
    return this.machine?.name;
  }

  protected async provision(): Promise<void> {
    if (
      this.options.provider?.target &&
      this.options.provider.target !== "local"
    ) {
      throw new Error(
        "AgentBox's Smol provider currently supports local machines only.",
      );
    }
    if (this.options.idleTimeoutMs || this.options.autoStopMs) {
      throw new Error(
        "Smol local machines do not support idleTimeoutMs or autoStopMs.",
      );
    }
    if (!this.options.image) {
      throw new Error(
        "Smol requires options.image with the agent CLI installed.",
      );
    }
    const existing = (await this.ownedMachines(this.options.tags))[0];
    if (existing) {
      this.machine = await Machine.connect(existing.id, connection);
      this.isWarmFlag = true;
      return;
    }
    const ports = new Set([
      ...collectAllAgentReservedPorts(),
      ...(this.options.provider?.publishedPorts ?? []),
      ...this.declaredPorts,
    ]);
    const mappings = [];
    const allocated = new Set<number>();
    for (const guest of ports) {
      if (!Number.isInteger(guest) || guest < 1 || guest > 65535) {
        throw new Error(`Invalid Smol guest port: ${guest}`);
      }
      let host = await freePort();
      while (allocated.has(host)) host = await freePort();
      allocated.add(host);
      mappings.push({ host, guest });
    }
    // Each machine must survive the process that created it for tagged warm reattach.
    // Smol's default local lifetime would otherwise reap the VM on host exit.
    this.machine = await Machine.create(
      {
        name: `agentbox-${randomUUID()}`,
        image: this.options.image,
        labels: { ...this.options.tags, ...ownerLabel },
        env: this.getMergedEnv(),
        workdir: this.workingDir,
        detach: true,
        waitForPorts: false,
        ports: mappings,
        resources: {
          ...(this.options.resources?.cpu
            ? { cpus: this.options.resources.cpu }
            : {}),
          ...(this.options.resources?.memoryMiB
            ? { memoryMb: this.options.resources.memoryMiB }
            : {}),
          ...(this.options.provider?.allowHosts
            ? { allowHosts: this.options.provider.allowHosts }
            : { network: this.options.provider?.network ?? true }),
        },
      },
      connection,
    );
  }

  private async ownedMachines(
    tags?: Record<string, string>,
  ): Promise<MachineSummary[]> {
    return Machine.list(connection, { labels: { ...tags, ...ownerLabel } });
  }

  async list(options?: SandboxListOptions): Promise<SandboxDescriptor[]> {
    return (await this.ownedMachines(options?.tags)).map((machine) => ({
      provider: this.provider,
      id: machine.id,
      state: machine.state,
      tags: machine.labels,
      createdAt: machine.createdAt,
      raw: machine,
    }));
  }

  private get readyMachine(): Machine {
    this.requireProvisioned();
    if (!this.machine) throw new Error("Smol machine is not provisioned.");
    return this.machine;
  }

  async run(
    command: string | string[],
    options?: CommandOptions,
  ): Promise<CommandResult> {
    const result = await this.readyMachine.exec(
      ["/bin/sh", "-c", toShellCommand(command)],
      {
        workdir: options?.cwd ?? this.workingDir,
        env: this.getMergedEnv(options?.env),
        ...(options?.timeoutMs
          ? { timeout: Math.ceil(options.timeoutMs / 1000) }
          : {}),
      },
    );
    return {
      exitCode: result.exitCode,
      stdout: result.stdout,
      stderr: result.stderr,
      combinedOutput: result.stdout + result.stderr,
      raw: result,
    };
  }

  async runAsync(
    command: string | string[],
    options?: CommandOptions,
  ): Promise<AsyncCommandHandle> {
    const machine = this.readyMachine;
    const abort = new AbortController();
    const queue = new AsyncQueue<CommandEvent>();
    const chunks = { stdout: "", stderr: "" };
    const completion = (async (): Promise<CommandResult> => {
      let exitCode: number | undefined;
      for await (const event of machine.execStream(
        ["/bin/sh", "-c", toShellCommand(command)],
        {
          workdir: options?.cwd ?? this.workingDir,
          env: this.getMergedEnv(options?.env),
          signal: abort.signal,
          ...(options?.timeoutMs
            ? { timeout: Math.ceil(options.timeoutMs / 1000) }
            : {}),
        },
      )) {
        if (event.kind === "error") throw new Error(event.message);
        if (event.kind === "exit") {
          exitCode = event.exitCode;
          queue.push({
            type: "exit",
            exitCode,
            timestamp: new Date().toISOString(),
          });
        } else {
          chunks[event.kind] += event.data;
          queue.push({
            type: event.kind,
            chunk: event.data,
            timestamp: new Date().toISOString(),
          });
        }
      }
      if (exitCode === undefined)
        throw new Error("Smol command stream ended without an exit status.");
      queue.finish();
      return {
        exitCode,
        ...chunks,
        combinedOutput: chunks.stdout + chunks.stderr,
      };
    })().catch((error: unknown) => {
      queue.fail(error);
      throw error;
    });
    suppressUnhandledRejection(completion);
    return {
      id: randomUUID(),
      wait: () => completion,
      kill: async () => {
        abort.abort(new Error("Smol command cancelled."));
        await completion.catch(() => undefined);
      },
      [Symbol.asyncIterator]: () => queue[Symbol.asyncIterator](),
    };
  }

  async snapshot(): Promise<string | null> {
    this.requireProvisioned();
    return null;
  }
  async stop(): Promise<void> {
    if (this.machine) await this.machine.stop();
  }
  async delete(): Promise<void> {
    if (this.machine) await this.machine.delete();
    this.machine = undefined;
  }
  async openPort(port: number): Promise<void> {
    if (this.machine && !this.machine.endpoint(port).httpUrl) {
      throw new Error(
        `Smol port ${port} must be opened before findOrProvision().`,
      );
    }
    this.declaredPorts.add(port);
  }
  async getPreviewLink(port: number): Promise<string> {
    return this.readyMachine.endpoint(port).httpUrl;
  }
  get previewHeaders(): Record<string, string> {
    return {};
  }
  async uploadFile(
    content: Buffer | string,
    targetPath: string,
  ): Promise<void> {
    const machine = this.readyMachine;
    const directory = posix.dirname(targetPath);
    const mkdir = await machine.exec(["mkdir", "-p", directory]);
    if (mkdir.exitCode !== 0)
      throw new Error(`Smol mkdir failed: ${mkdir.stderr}`);
    await machine.writeFile(targetPath, content);
  }
  async downloadFile(sourcePath: string): Promise<Buffer> {
    return this.readyMachine.readFile(sourcePath);
  }
}
