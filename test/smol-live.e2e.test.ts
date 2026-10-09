import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { Sandbox } from "../src/sandboxes/Sandbox";

const live =
  process.env.AGENTBOX_RUN_SMOL_E2E === "1" ? describe : describe.skip;

live("Smol local microVM provider", () => {
  it("reattaches, streams commands, transfers files and exposes an agent port", async () => {
    const tags = { "agentbox.test": randomUUID() };
    const options = {
      image: "node:24-alpine",
      tags,
      workingDir: "/workspace",
      provider: { publishedPorts: [18090] },
    };
    const sandbox = new Sandbox("smol", options);
    try {
      await sandbox.openPort(18090);
      await sandbox.findOrProvision();
      expect(sandbox.isWarm).toBe(false);
      expect((await sandbox.list({ tags })).map((x) => x.id)).toContain(
        sandbox.id,
      );
      await sandbox.uploadFile(
        Buffer.from([0, 1, 255, 2]),
        "/workspace/data/file.bin",
      );
      expect(await sandbox.downloadFile("/workspace/data/file.bin")).toEqual(
        Buffer.from([0, 1, 255, 2]),
      );
      const result = await sandbox.run("pwd && echo hello", {
        cwd: "/workspace",
      });
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("/workspace\nhello");
      const handle = await sandbox.runAsync(
        "printf hello; printf error >&2; exit 3",
      );
      const events = [];
      for await (const event of handle) events.push(event);
      expect(events.map((event) => event.type)).toEqual([
        "stdout",
        "stderr",
        "exit",
      ]);
      expect(await handle.wait()).toMatchObject({
        exitCode: 3,
        stdout: "hello",
        stderr: "error",
      });
      const slow = await sandbox.runAsync("sleep 30");
      await slow.kill();
      await expect(slow.wait()).rejects.toThrow("Smol command cancelled");
      expect((await sandbox.run("echo still-alive")).stdout.trim()).toBe("still-alive");
      const server = await sandbox.run(
        'nohup node -e \'require("http").createServer((_, res) => res.end("smol-ready")).listen(18090, "0.0.0.0")\' >/workspace/server.log 2>&1 </dev/null &',
      );
      expect(server.exitCode).toBe(0);
      let response: Response | undefined;
      for (let i = 0; i < 50; i++) {
        try {
          response = await fetch(await sandbox.getPreviewLink(18090));
          break;
        } catch {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }
      expect(await response?.text()).toBe("smol-ready");
      const other = new Sandbox("smol", options);
      await other.findOrProvision();
      expect(other.isWarm).toBe(true);
      expect(other.id).toBe(sandbox.id);
      expect(
        (await other.run("cat /workspace/data/file.bin | wc -c")).stdout.trim(),
      ).toBe("4");
    } finally {
      await sandbox.delete();
    }
  }, 120_000);
});
