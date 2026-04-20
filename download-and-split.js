#!/usr/bin/env node

const { createHash } = require("node:crypto");
const { once } = require("node:events");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const http = require("node:http");
const https = require("node:https");
const path = require("node:path");
const { finished } = require("node:stream/promises");

const DEFAULT_CHUNK_MB = 500;
const DEFAULT_CHUNK_SIZE_BYTES = 500_000_000;
const DEFAULT_OUT_DIR = "./output";
const DEFAULT_FILE_NAME = "download.bin";
const MAX_REDIRECTS = 5;

class UsageError extends Error {}

function usage() {
  return [
    "Usage:",
    "  node download-and-split.js <url-or-file> [--chunk-mb 500] [--out-dir ./output] [--name <filename>] [--force]",
  ].join("\n");
}

function sanitizeFileName(input) {
  const normalized = String(input || "")
    .normalize("NFKD")
    .replace(/[^\x20-\x7E]/g, "")
    .replace(/[^A-Za-z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^[.-]+|[.-]+$/g, "");

  if (!normalized || normalized === "." || normalized === "..") {
    return DEFAULT_FILE_NAME;
  }

  return normalized;
}

function deriveFileName(sourceInput, overrideName) {
  if (overrideName) {
    return sanitizeFileName(overrideName);
  }

  try {
    const parsedUrl = new URL(sourceInput);

    if (parsedUrl.protocol === "file:") {
      const filePath = decodeURIComponent(parsedUrl.pathname);
      return sanitizeFileName(path.basename(filePath) || DEFAULT_FILE_NAME);
    }

    const baseName = path.posix.basename(decodeURIComponent(parsedUrl.pathname));
    return sanitizeFileName(baseName || DEFAULT_FILE_NAME);
  } catch {
    return sanitizeFileName(path.basename(String(sourceInput)) || DEFAULT_FILE_NAME);
  }
}

function toChunkSizeBytes(chunkMb) {
  const parsed = Number.parseFloat(String(chunkMb));

  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new UsageError("--chunk-mb must be a positive number.");
  }

  const bytes = Math.floor(parsed * 1_000_000);
  return Math.max(bytes, 1);
}

function isWindowsDrivePath(value) {
  return /^[A-Za-z]:[\\/]/.test(String(value));
}

function parseSourceInput(sourceInput) {
  const rawValue = String(sourceInput || "");

  if (!rawValue) {
    throw new UsageError("A download URL or local file path is required.");
  }

  try {
    const parsedUrl = new URL(rawValue);

    if (["http:", "https:"].includes(parsedUrl.protocol)) {
      return { url: parsedUrl.toString() };
    }

    if (parsedUrl.protocol === "file:") {
      return { filePath: path.resolve(decodeURIComponent(parsedUrl.pathname)) };
    }

    throw new UsageError("Only http, https, and local file paths are supported.");
  } catch (error) {
    if (error instanceof UsageError) {
      throw error;
    }

    if (/^[A-Za-z][A-Za-z0-9+.-]*:/.test(rawValue) && !isWindowsDrivePath(rawValue)) {
      throw new UsageError("Only http, https, and local file paths are supported.");
    }

    return { filePath: path.resolve(rawValue) };
  }
}

function parseArgs(argv) {
  if (!Array.isArray(argv) || argv.length === 0) {
    throw new UsageError("A download URL or local file path is required.");
  }

  const args = [...argv];
  let sourceInput = null;
  let chunkMb = DEFAULT_CHUNK_MB;
  let outDir = DEFAULT_OUT_DIR;
  let name = null;
  let force = false;

  while (args.length > 0) {
    const arg = args.shift();

    if (arg === "--help" || arg === "-h") {
      throw new UsageError(usage());
    }

    if (arg === "--force") {
      force = true;
      continue;
    }

    if (arg === "--chunk-mb") {
      if (args.length === 0) {
        throw new UsageError("--chunk-mb requires a value.");
      }

      chunkMb = args.shift();
      continue;
    }

    if (arg === "--out-dir") {
      if (args.length === 0) {
        throw new UsageError("--out-dir requires a value.");
      }

      outDir = args.shift();
      continue;
    }

    if (arg === "--name") {
      if (args.length === 0) {
        throw new UsageError("--name requires a value.");
      }

      name = args.shift();
      continue;
    }

    if (arg.startsWith("--")) {
      throw new UsageError(`Unknown option: ${arg}`);
    }

    if (sourceInput) {
      throw new UsageError("Only one download URL or local file path may be provided.");
    }

    sourceInput = arg;
  }

  const source = parseSourceInput(sourceInput);
  const fileName = deriveFileName(sourceInput, name);

  return {
    ...source,
    chunkMb: Number.parseFloat(String(chunkMb)),
    chunkSizeBytes: toChunkSizeBytes(chunkMb),
    outDir: path.resolve(outDir),
    fileName,
    force,
  };
}

function createChunkName(fileName, chunkIndex) {
  return `${fileName}.part${String(chunkIndex).padStart(4, "0")}`;
}

function formatShellString(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function generateDockerfile(chunkNames) {
  const lines = [
    "FROM alpine:3.20",
    "WORKDIR /opt/app",
    "",
    ...chunkNames.map(
      (chunkName) => `COPY chunks/${chunkName} /opt/chunks/${chunkName}`,
    ),
    "COPY entrypoint.sh /usr/local/bin/entrypoint.sh",
    "",
    "RUN chmod +x /usr/local/bin/entrypoint.sh && mkdir -p /opt/chunks /opt/restored",
    "",
    'ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]',
    "",
  ];

  return lines.join("\n");
}

function generateEntrypoint({ chunkNames, fileName, sha256 }) {
  const defaultOutputPath = `/opt/restored/${fileName}`;
  const chunkList = chunkNames.join("\n");

  return [
    "#!/bin/sh",
    "set -eu",
    "",
    `EXPECTED_SHA256=${formatShellString(sha256)}`,
    'CHUNKS_DIR="${CHUNKS_DIR:-/opt/chunks}"',
    `OUTPUT_PATH="\${RESTORE_OUTPUT_PATH:-${defaultOutputPath}}"`,
    'TMP_PATH="${OUTPUT_PATH}.tmp"',
    "",
    "cleanup() {",
    '  rm -f "$TMP_PATH"',
    "}",
    "",
    "verify_sha256() {",
    '  if command -v sha256sum >/dev/null 2>&1; then',
    '    printf \'%s  %s\\n\' "$EXPECTED_SHA256" "$TMP_PATH" | sha256sum -c - >/dev/null',
    "    return",
    "  fi",
    "",
    '  if command -v shasum >/dev/null 2>&1; then',
    '    actual_sha256="$(shasum -a 256 "$TMP_PATH" | awk \'{print $1}\')"',
    '    if [ "$actual_sha256" != "$EXPECTED_SHA256" ]; then',
    '      echo "SHA-256 mismatch for restored file." >&2',
    "      return 1",
    "    fi",
    "    return",
    "  fi",
    "",
    '  echo "No SHA-256 tool found in the container." >&2',
    "  return 1",
    "}",
    "",
    "trap cleanup EXIT INT TERM",
    'mkdir -p "$(dirname "$OUTPUT_PATH")"',
    'rm -f "$OUTPUT_PATH" "$TMP_PATH"',
    ': > "$TMP_PATH"',
    "",
    "while IFS= read -r chunk_name; do",
    '  [ -n "$chunk_name" ] || continue',
    '  cat "$CHUNKS_DIR/$chunk_name" >> "$TMP_PATH"',
    "done <<'CHUNKS'",
    chunkList,
    "CHUNKS",
    "",
    "verify_sha256",
    'mv "$TMP_PATH" "$OUTPUT_PATH"',
    "trap - EXIT INT TERM",
    "",
    'if [ "$#" -gt 0 ]; then',
    '  exec "$@"',
    "fi",
    "",
  ].join("\n");
}

function request(urlString) {
  const url = new URL(urlString);
  const client = url.protocol === "https:" ? https : http;

  return new Promise((resolve, reject) => {
    const req = client.get(url, (response) => resolve(response));
    req.on("error", reject);
  });
}

async function requestWithRedirects(urlString, redirectCount = 0) {
  if (redirectCount > MAX_REDIRECTS) {
    throw new Error(`Too many redirects while downloading ${urlString}`);
  }

  const response = await request(urlString);
  const statusCode = response.statusCode ?? 0;

  if (statusCode >= 300 && statusCode < 400 && response.headers.location) {
    const nextUrl = new URL(response.headers.location, urlString);
    response.resume();
    return requestWithRedirects(nextUrl.toString(), redirectCount + 1);
  }

  if (statusCode < 200 || statusCode >= 300) {
    response.resume();
    throw new Error(`Download failed with status ${statusCode} for ${urlString}`);
  }

  return response;
}

async function writeToStream(stream, buffer) {
  if (!stream.write(buffer)) {
    await once(stream, "drain");
  }
}

async function splitStreamIntoChunks({ stream, chunkSizeBytes, chunksDir, fileName }) {
  const sha256 = createHash("sha256");
  const chunkNames = [];
  let currentStream = null;
  let currentChunkBytes = 0;
  let currentChunkIndex = 0;

  async function rotateChunk() {
    if (currentStream) {
      currentStream.end();
      await finished(currentStream);
    }

    currentChunkIndex += 1;
    currentChunkBytes = 0;

    const chunkName = createChunkName(fileName, currentChunkIndex);
    const chunkPath = path.join(chunksDir, chunkName);

    currentStream = fs.createWriteStream(chunkPath);
    chunkNames.push(chunkName);
  }

  await rotateChunk();

  for await (const rawChunk of stream) {
    const chunk = Buffer.isBuffer(rawChunk) ? rawChunk : Buffer.from(rawChunk);
    let offset = 0;

    while (offset < chunk.length) {
      if (currentChunkBytes === chunkSizeBytes) {
        await rotateChunk();
      }

      const remainingBytes = chunkSizeBytes - currentChunkBytes;
      const nextOffset = Math.min(offset + remainingBytes, chunk.length);
      const slice = chunk.subarray(offset, nextOffset);

      sha256.update(slice);
      await writeToStream(currentStream, slice);

      currentChunkBytes += slice.length;
      offset = nextOffset;
    }
  }

  if (currentStream) {
    currentStream.end();
    await finished(currentStream);
  }

  return {
    chunkNames,
    sha256: sha256.digest("hex"),
  };
}

async function downloadFileInChunks({ url, chunkSizeBytes, chunksDir, fileName }) {
  const response = await requestWithRedirects(url);
  return splitStreamIntoChunks({ stream: response, chunkSizeBytes, chunksDir, fileName });
}

async function splitLocalFileInChunks({ filePath, chunkSizeBytes, chunksDir, fileName }) {
  const stream = fs.createReadStream(filePath);
  return splitStreamIntoChunks({ stream, chunkSizeBytes, chunksDir, fileName });
}

async function writeGeneratedFiles(targetRoot, metadata) {
  const dockerfilePath = path.join(targetRoot, "Dockerfile");
  const entrypointPath = path.join(targetRoot, "entrypoint.sh");

  await Promise.all([
    fsp.writeFile(dockerfilePath, generateDockerfile(metadata.chunkNames)),
    fsp.writeFile(entrypointPath, generateEntrypoint(metadata)),
  ]);

  return {
    dockerfilePath,
    entrypointPath,
  };
}

async function run(options) {
  const targetRoot = path.join(options.outDir, options.fileName);
  const chunksDir = path.join(targetRoot, "chunks");
  let targetPrepared = false;

  try {
    if (options.force) {
      await fsp.rm(targetRoot, { recursive: true, force: true });
    } else {
      const existing = await fsp
        .stat(targetRoot)
        .then(() => true)
        .catch((error) => {
          if (error && error.code === "ENOENT") {
            return false;
          }

          throw error;
        });

      if (existing) {
        throw new Error(`Output directory already exists: ${targetRoot}`);
      }
    }

    await fsp.mkdir(chunksDir, { recursive: true });
    targetPrepared = true;

    let downloadResult;

    if (options.url) {
      downloadResult = await downloadFileInChunks({
        url: options.url,
        chunkSizeBytes: options.chunkSizeBytes,
        chunksDir,
        fileName: options.fileName,
      });
    } else if (options.filePath) {
      downloadResult = await splitLocalFileInChunks({
        filePath: options.filePath,
        chunkSizeBytes: options.chunkSizeBytes,
        chunksDir,
        fileName: options.fileName,
      });
    } else {
      throw new Error("Either a download URL or local file path must be provided.");
    }

    const generatedFiles = await writeGeneratedFiles(targetRoot, {
      ...downloadResult,
      fileName: options.fileName,
    });

    return {
      targetRoot,
      chunksDir,
      fileName: options.fileName,
      chunkNames: downloadResult.chunkNames,
      sha256: downloadResult.sha256,
      ...generatedFiles,
    };
  } catch (error) {
    if (targetPrepared) {
      await fsp.rm(targetRoot, { recursive: true, force: true });
    }

    throw error;
  }
}

async function runCli(argv = process.argv.slice(2), io = {}) {
  const stdout = io.stdout ?? process.stdout;
  const options = parseArgs(argv);
  const result = await run(options);

  stdout.write(`Output: ${result.targetRoot}\n`);
  stdout.write(`Chunks: ${result.chunkNames.length}\n`);

  return result;
}

async function main() {
  try {
    await runCli();
  } catch (error) {
    const message = error instanceof UsageError ? error.message : `Error: ${error.message}`;
    process.stderr.write(`${message}\n`);

    if (!(error instanceof UsageError && error.message === usage())) {
      process.stderr.write(`${usage()}\n`);
    }

    process.exitCode = 1;
  }
}

if (require.main === module) {
  void main();
}

module.exports = {
  DEFAULT_CHUNK_SIZE_BYTES,
  UsageError,
  createChunkName,
  deriveFileName,
  downloadFileInChunks,
  generateDockerfile,
  generateEntrypoint,
  parseArgs,
  parseSourceInput,
  run,
  runCli,
  sanitizeFileName,
  splitLocalFileInChunks,
  toChunkSizeBytes,
};
