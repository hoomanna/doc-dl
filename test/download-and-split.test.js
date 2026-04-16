const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const crypto = require("node:crypto");
const { EventEmitter } = require("node:events");
const fs = require("node:fs/promises");
const http = require("node:http");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");
const { Readable } = require("node:stream");
const test = require("node:test");

const {
  createChunkName,
  run,
} = require("../download-and-split");

async function createTempDir() {
  return fs.mkdtemp(path.join(os.tmpdir(), "docker-dl-"));
}

async function withMockedRequests(routes, action) {
  const originalHttpGet = http.get;
  const originalHttpsGet = https.get;

  function getRoute(urlValue) {
    const key = urlValue instanceof URL ? urlValue.toString() : String(urlValue);
    return routes[key];
  }

  function mockedGet(urlValue, callback) {
    const request = new EventEmitter();

    process.nextTick(() => {
      const route = getRoute(urlValue);

      if (!route) {
        request.emit("error", new Error(`No mocked response for ${urlValue}`));
        return;
      }

      const response = Readable.from(route.chunks ?? [route.body ?? Buffer.alloc(0)]);
      response.statusCode = route.statusCode;
      response.headers = route.headers ?? {};
      callback(response);
    });

    return request;
  }

  http.get = mockedGet;
  https.get = mockedGet;

  try {
    return await action();
  } finally {
    http.get = originalHttpGet;
    https.get = originalHttpsGet;
  }
}

function createPayload(size, seed = "x") {
  return Buffer.from(seed.repeat(size), "utf8");
}

test("downloads a single chunk and generates one explicit Docker COPY", async (t) => {
  const payload = Buffer.from("hello world");
  const tempDir = await createTempDir();

  t.after(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  const result = await withMockedRequests({
    "http://example.test/file.bin": {
      statusCode: 200,
      headers: { "content-type": "application/octet-stream" },
      body: payload,
    },
  }, () =>
    run({
      url: "http://example.test/file.bin",
      chunkSizeBytes: 1_000,
      outDir: tempDir,
      fileName: "file.bin",
      force: false,
    }),
  );

  assert.equal(result.chunkNames.length, 1);
  assert.deepEqual(result.chunkNames, [createChunkName("file.bin", 1)]);

  const chunkContent = await fs.readFile(path.join(result.chunksDir, result.chunkNames[0]));
  assert.deepEqual(chunkContent, payload);

  const dockerfile = await fs.readFile(result.dockerfilePath, "utf8");
  assert.match(dockerfile, /COPY chunks\/file\.bin\.part0001 \/opt\/chunks\/file\.bin\.part0001/);
  assert.equal((dockerfile.match(/^COPY chunks\//gm) || []).length, 1);
});

test("splits larger payloads into ordered chunks", async (t) => {
  const payload = Buffer.concat([
    createPayload(10, "a"),
    createPayload(10, "b"),
    createPayload(10, "c"),
    createPayload(5, "d"),
  ]);
  const tempDir = await createTempDir();

  t.after(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  const result = await withMockedRequests({
    "http://example.test/multi.bin": {
      statusCode: 200,
      chunks: [payload.subarray(0, 7), payload.subarray(7, 21), payload.subarray(21)],
    },
  }, () =>
    run({
      url: "http://example.test/multi.bin",
      chunkSizeBytes: 10,
      outDir: tempDir,
      fileName: "multi.bin",
      force: false,
    }),
  );

  assert.deepEqual(result.chunkNames, [
    "multi.bin.part0001",
    "multi.bin.part0002",
    "multi.bin.part0003",
    "multi.bin.part0004",
  ]);

  const restored = Buffer.concat(
    await Promise.all(
      result.chunkNames.map((chunkName) =>
        fs.readFile(path.join(result.chunksDir, chunkName)),
      ),
    ),
  );

  assert.deepEqual(restored, payload);
});

test("follows redirects before downloading", async (t) => {
  const payload = Buffer.from("redirect payload");
  const tempDir = await createTempDir();

  t.after(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  const result = await withMockedRequests({
    "http://example.test/redirect.bin": {
      statusCode: 302,
      headers: { location: "/target.bin" },
      body: Buffer.alloc(0),
    },
    "http://example.test/target.bin": {
      statusCode: 200,
      headers: { "content-type": "application/octet-stream" },
      body: payload,
    },
  }, () =>
    run({
      url: "http://example.test/redirect.bin",
      chunkSizeBytes: 1_000,
      outDir: tempDir,
      fileName: "redirect.bin",
      force: false,
    }),
  );

  const chunkContent = await fs.readFile(path.join(result.chunksDir, result.chunkNames[0]));
  assert.deepEqual(chunkContent, payload);
});

test("removes partial output after non-2xx failures", async (t) => {
  const tempDir = await createTempDir();

  t.after(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  await assert.rejects(() =>
    withMockedRequests({
      "http://example.test/missing.bin": {
        statusCode: 404,
        body: Buffer.from("missing"),
      },
    }, () =>
      run({
        url: "http://example.test/missing.bin",
        chunkSizeBytes: 100,
        outDir: tempDir,
        fileName: "missing.bin",
        force: false,
      }),
    ),
  );

  await assert.rejects(fs.stat(path.join(tempDir, "missing.bin")), { code: "ENOENT" });
});

test("generated entrypoint restores bytes at runtime and then executes the passed command", async (t) => {
  const payload = crypto.randomBytes(64);
  const tempDir = await createTempDir();

  t.after(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  const result = await withMockedRequests({
    "http://example.test/runtime.bin": {
      statusCode: 200,
      headers: { "content-type": "application/octet-stream" },
      chunks: [payload.subarray(0, 9), payload.subarray(9, 31), payload.subarray(31)],
    },
  }, () =>
    run({
      url: "http://example.test/runtime.bin",
      chunkSizeBytes: 13,
      outDir: tempDir,
      fileName: "runtime.bin",
      force: false,
    }),
  );

  const restoreOutputPath = path.join(tempDir, "restored", "runtime.bin");

  await assert.rejects(fs.stat(restoreOutputPath), { code: "ENOENT" });

  const execution = spawnSync("/bin/sh", [result.entrypointPath, "printf", "restored"], {
    cwd: result.targetRoot,
    encoding: "utf8",
    env: {
      ...process.env,
      CHUNKS_DIR: result.chunksDir,
      RESTORE_OUTPUT_PATH: restoreOutputPath,
    },
  });

  assert.equal(execution.status, 0, execution.stderr);
  assert.equal(execution.stdout, "restored");

  const restoredFile = await fs.readFile(restoreOutputPath);
  assert.deepEqual(restoredFile, payload);
});
