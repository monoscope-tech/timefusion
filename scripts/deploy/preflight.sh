#!/usr/bin/env bash
set -euo pipefail
: "${CAPROVER_SERVER:?CAPTAINROVER_SERVER is required}"
: "${CAPROVER_APP:?CAPTAINROVER_APP_NAME is required}"
: "${CAPROVER_TOKEN:?CAPTAINROVER_APP_TOKEN is required}"
: "${GITHUB_OUTPUT:?deployment diagnostics output is required}"

# CapRover's app token is opaque and scoped to one app. An empty upload reaches
# ILLEGAL_OPERATION (1108) only after the server has matched the token to the
# app in the URL; the handler rejects the empty payload before scheduling a
# build. This binds the configured name and token before HANDOFF fences writes.
node <<'JS'
const server = process.env.CAPROVER_SERVER;
const app = process.env.CAPROVER_APP;
const endpoint = new URL(`/api/v2/user/apps/appData/${encodeURIComponent(app)}/`, server);
const timeoutMs = Number(process.env.CAPROVER_PREFLIGHT_TIMEOUT_MS || 10_000);

async function main() {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'x-captain-app-token': process.env.CAPROVER_TOKEN,
      'x-namespace': 'captain',
      'content-type': 'application/x-www-form-urlencoded',
      'content-length': '0',
    },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch (_) {
    throw new Error(`CapRover identity probe returned a non-JSON response (HTTP ${response.status})`);
  }
  if (response.status !== 200 || body.status !== 1108) {
    throw new Error(`CapRover rejected the ${JSON.stringify(app)} app/token identity (HTTP ${response.status}, status ${body.status ?? 'missing'})`);
  }
  const fs = require('node:fs');
  const record = [
    `target_server_host=${endpoint.host}`,
    `target_app=${app}`,
    'token_bound=true',
    `verified_at=${new Date().toISOString()}`,
  ].join('\n') + '\n';
  fs.appendFileSync(process.env.GITHUB_OUTPUT, record, { encoding: 'utf8' });
  process.stdout.write(`CapRover app/token identity verified for ${app}.\n`);
}

main().catch(error => {
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
});
JS
