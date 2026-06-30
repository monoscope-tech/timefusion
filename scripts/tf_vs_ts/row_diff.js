#!/usr/bin/env node
// Field-level parity check for a single (project, window).
// For the id-intersection of TS and TF, compares a representative column set
// value-by-value, normalizing the known representation differences:
//   - JSON object columns: parse + deep-compare (jsonb reorders keys)
//   - timestamp columns: normalize fractional seconds to 6 digits
//   - nulls: TS '<null>' / TF '' treated equal
// Also reports id-set divergence (TS-only / TF-only).
// NOTE: rows are keyed by id, so a value comparison is only meaningful when ids
// are distinct on both sides. If counts_matrix.sh flags TF_DUP for the window,
// field comparison here picks one arbitrary copy — interpret with care.
// Usage: node row_diff.js <project_id> <ISO_start> <ISO_end>
const { execFileSync } = require('child_process');
const fs = require('fs');

const [PID, T0, T1] = process.argv.slice(2);
if (!PID || !T0 || !T1) { console.error('usage: row_diff.js <project_id> <start> <end>'); process.exit(1); }

const TS_URL = '***REMOVED-CREDENTIAL***';
const TF_URL = (fs.readFileSync('/Users/tonyalaribe/Projects/apitoolkit/monoscope/.env', 'utf8')
  .match(/^TIMEFUSION_PG_URL=(.*)$/m) || [])[1];

const US = '\x1f'; // unit separator between id and value
const WHERE = `project_id='${PID}' and timestamp >= '${T0}+00' and timestamp < '${T1}+00'`;

const jsonCols   = ['body','resource','attributes','errors','events','links','hashes'];
const tsCols     = ['observed_timestamp','start_time','end_time'];
const scalarCols = ['name','kind','status_code','status_message','severity___severity_text',
  'severity___severity_number','duration','message_size_bytes','context___trace_id',
  'context___span_id','resource___service___name','summary'];

function q(url, sql) {
  const out = execFileSync('psql', [url, '-At', '-F', US, '-c', sql],
    { encoding: 'utf8', maxBuffer: 1 << 30, stdio: ['ignore','pipe','ignore'] });
  const m = new Map();
  for (const line of out.split('\n')) {
    if (!line) continue;
    const i = line.indexOf(US);
    if (i < 0) continue;
    m.set(line.slice(0, i), line.slice(i + 1));
  }
  return m;
}
// sanitize embedded newlines so one row == one line (text/int/ts cols only)
const sani = e => `replace(replace(${e}, chr(10), '\\n'), chr(13), '')`;
function fetch(url, col, isTF) {
  if (jsonCols.includes(col)) {
    // Variant on TF can't be wrapped in replace(); pgwire serializes it to json
    // text natively when projected directly. Fetch id+col as two columns.
    const expr = isTF ? col : `${col}::text`;
    return q(url, `select id, ${expr} from otel_logs_and_spans where ${WHERE}`);
  }
  return q(url, `select id || '${US}' || coalesce(${sani(`(${col})::text`)}, '') from otel_logs_and_spans where ${WHERE} order by id`);
}

// canonicalize any timestamp spelling (TS '... .038+00' / TF '...T..038Z') to epoch-us
const normTs = v => {
  if (!v) return v;
  const m = v.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?/);
  if (!m) return v;
  const frac = (m[7] || '').padEnd(6, '0').slice(0, 6);
  return `${m[1]}${m[2]}${m[3]}${m[4]}${m[5]}${m[6]}${frac}`;
};
// summary: TS postgres-array {..}, TF json-ish [..] — compare content ignoring delimiters/quotes
const normSummary = v => (v || '').replace(/[{}\[\]"\\ ]/g, '');
function jsonEq(a, b) {
  if (a === b) return true;
  try { return JSON.stringify(canon(JSON.parse(a))) === JSON.stringify(canon(JSON.parse(b))); }
  catch { return false; }
}
function canon(x) {
  if (Array.isArray(x)) return x.map(canon);
  if (x && typeof x === 'object') return Object.keys(x).sort().reduce((o,k)=>(o[k]=canon(x[k]),o),{});
  return x;
}

// id sets
const tsIds = q(TS_URL, `select id || '${US}' || '1' from otel_logs_and_spans where ${WHERE}`);
const tfIds = q(TF_URL, `select id || '${US}' || '1' from otel_logs_and_spans where ${WHERE}`);
const tsOnly = [...tsIds.keys()].filter(k => !tfIds.has(k));
const tfOnly = [...tfIds.keys()].filter(k => !tsIds.has(k));
const both   = [...tsIds.keys()].filter(k => tfIds.has(k));
console.log(`window ${T0} .. ${T1}  project ${PID.slice(0,8)}`);
console.log(`id counts: TS=${tsIds.size} TF=${tfIds.size} intersection=${both.length} TS_only=${tsOnly.length} TF_only=${tfOnly.length}`);
if (tsOnly.length) console.log('  TS_only sample:', tsOnly.slice(0,3).join(' '));
if (tfOnly.length) console.log('  TF_only sample:', tfOnly.slice(0,3).join(' '));

let totalMismatch = 0;
for (const col of [...scalarCols, ...tsCols, ...jsonCols]) {
  const isTs = tsCols.includes(col), isJson = jsonCols.includes(col);
  let tsm, tfm;
  try { tsm = fetch(TS_URL, col, false); tfm = fetch(TF_URL, col, true); }
  catch (e) { console.log(`  ${col.padEnd(34)} QUERY_ERROR`); continue; }
  let mism = 0; const ex = [];
  for (const id of both) {
    let a = tsm.get(id) ?? '', b = tfm.get(id) ?? '';
    let eq;
    if (isJson) eq = (a === '' && b === '') || jsonEq(a, b);
    else if (isTs) eq = normTs(a) === normTs(b);
    else if (col === 'summary') eq = normSummary(a) === normSummary(b);
    else eq = a === b;
    if (!eq) { mism++; if (ex.length < 2) ex.push(`${id.slice(0,8)}[TS=${JSON.stringify(a).slice(0,40)} TF=${JSON.stringify(b).slice(0,40)}]`); }
  }
  totalMismatch += mism;
  const tag = mism === 0 ? 'ok' : `MISMATCH x${mism}`;
  console.log(`  ${col.padEnd(34)} ${tag}${mism ? '  ' + ex.join('  ') : ''}`);
}
console.log(`TOTAL field mismatches over ${both.length} shared rows: ${totalMismatch}`);
