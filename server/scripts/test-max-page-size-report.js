#!/usr/bin/env node
/* eslint-disable no-console */

const DEFAULT_BASE_URL = process.env.BASE_URL || 'http://localhost:3000';
const DEFAULT_METADATA_ID = Number(process.env.METADATA_ID || 59);
const DEFAULT_ROUNDS = Number(process.env.ROUNDS || 5);
const DEFAULT_DELAY_MS = Number(process.env.DELAY_MS || 1200);
const DEFAULT_TIMEOUT_MS = Number(process.env.TIMEOUT_MS || 90000);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowTime() {
  const d = new Date();
  return d.toISOString().replace('T', ' ').slice(0, 19);
}

function parseCliArgs() {
  const args = process.argv.slice(2);
  const parsed = {
    baseUrl: DEFAULT_BASE_URL,
    metadataId: DEFAULT_METADATA_ID,
    rounds: DEFAULT_ROUNDS,
    delayMs: DEFAULT_DELAY_MS,
    timeoutMs: DEFAULT_TIMEOUT_MS,
  };

  for (let i = 0; i < args.length; i += 1) {
    const key = args[i];
    const value = args[i + 1];
    if (!value || value.startsWith('--')) continue;

    if (key === '--base-url') parsed.baseUrl = value;
    if (key === '--metadata-id') parsed.metadataId = Number(value);
    if (key === '--rounds') parsed.rounds = Number(value);
    if (key === '--delay-ms') parsed.delayMs = Number(value);
    if (key === '--timeout-ms') parsed.timeoutMs = Number(value);
  }

  return parsed;
}

async function postWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
      signal: controller.signal,
    });
    const text = await response.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch (_e) {
      json = { raw: text };
    }
    return { ok: response.ok, status: response.status, body: json };
  } finally {
    clearTimeout(timer);
  }
}

function isFailureResult(body) {
  const maxPageSize = body?.data?.maxPageSize ?? body?.maxPageSize;
  return maxPageSize === null || maxPageSize === undefined;
}

async function main() {
  const { baseUrl, metadataId, rounds, delayMs, timeoutMs } = parseCliArgs();
  const url = `${baseUrl.replace(/\/$/, '')}/api/metadata/${metadataId}/test-max-page-size`;

  if (!Number.isFinite(metadataId) || metadataId <= 0) {
    console.error('Invalid metadataId. Use --metadata-id <positive integer>.');
    process.exit(1);
  }
  if (!Number.isFinite(rounds) || rounds <= 0) {
    console.error('Invalid rounds. Use --rounds <positive integer>.');
    process.exit(1);
  }

  console.log(`\n[${nowTime()}] Start max-page-size test`);
  console.log(`URL: ${url}`);
  console.log(`Rounds: ${rounds}, Delay: ${delayMs}ms, Timeout: ${timeoutMs}ms\n`);

  const results = [];

  for (let i = 1; i <= rounds; i += 1) {
    const startedAt = Date.now();
    try {
      const res = await postWithTimeout(url, timeoutMs);
      const duration = Date.now() - startedAt;
      const fail = !res.ok || isFailureResult(res.body);
      const msg = String(res.body?.data?.message || res.body?.message || '');
      const maxPageSize = res.body?.data?.maxPageSize ?? res.body?.maxPageSize;
      results.push({
        round: i,
        status: res.status,
        duration,
        fail,
        maxPageSize,
        message: msg,
      });

      console.log(
        `[${nowTime()}] #${i} status=${res.status} duration=${duration}ms fail=${fail ? 'Y' : 'N'} maxPageSize=${maxPageSize ?? 'null'} msg=${msg || '-'}`
      );
    } catch (error) {
      const duration = Date.now() - startedAt;
      const message = error && typeof error === 'object' && 'message' in error ? error.message : String(error);
      results.push({
        round: i,
        status: 'ERR',
        duration,
        fail: true,
        maxPageSize: null,
        message,
      });
      console.log(`[${nowTime()}] #${i} status=ERR duration=${duration}ms fail=Y maxPageSize=null msg=${message}`);
    }

    if (i < rounds && delayMs > 0) {
      await sleep(delayMs);
    }
  }

  const failCount = results.filter((r) => r.fail).length;
  const successCount = results.length - failCount;
  const avgMs = Math.round(results.reduce((sum, item) => sum + item.duration, 0) / results.length);
  const failRate = ((failCount / results.length) * 100).toFixed(1);

  console.log('\n================ Summary ================');
  console.log(`Total rounds: ${results.length}`);
  console.log(`Success: ${successCount}`);
  console.log(`Failure: ${failCount}`);
  console.log(`Failure rate: ${failRate}%`);
  console.log(`Average latency: ${avgMs}ms`);
  console.log('==========================================\n');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
