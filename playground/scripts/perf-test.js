import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Custom metrics for accurate independent reporting
const writeCountMetric = new Counter('custom_write_count');
const readCountMetric = new Counter('custom_read_count');
const writeTrendMetric = new Trend('custom_write_latency');
const readTrendMetric = new Trend('custom_read_latency');

// Test execution parameters
const vusCount = 10;
const phaseDurationSeconds = 60;
const phaseDurationString = `${phaseDurationSeconds}s`;

export const options = {
  scenarios: {
    // Scenario 1: Pure Write using all VUs at maximum possible speed
    write_stress: {
      executor: 'constant-vus',
      vus: vusCount,                     // Strictly locked to VUs max
      duration: phaseDurationString,     // Run for specified phase duration
      exec: 'writeOnly',
    },
    // Scenario 2: Pure Read using all VUs at maximum possible speed
    read_stress: {
      executor: 'constant-vus',
      vus: vusCount,                     // Strictly locked to VUs max
      duration: phaseDurationString,     // Run for specified phase duration
      startTime: phaseDurationString,    // DELAYED START: Starts exactly when write ends
      exec: 'readOnly',
    },
  },
};

const host = 'http://kvdb-service:9000';
const totalKeys = 50000;

// MANDATORY DUMMY PLACEHOLDER FOR K6 ENGINE
export default function () {}

// ISOLATED WRITE-ONLY WORKLOAD
export function writeOnly() {
  const writeKey = `key_${Math.floor(Math.random() * totalKeys)}`;
  const postRes = http.post(`${host}/data/mydb/mytable/${writeKey}`, 'value_data', {
    headers: {
      'Content-Type': 'text/plain',
      'Connection': 'keep-alive'       // Keep TCP connections open and warm
    },
  });

  writeCountMetric.add(1);
  writeTrendMetric.add(postRes.timings.duration);
  check(postRes, { 'write success': (r) => r.status >= 200 && r.status < 300 });
}

// ISOLATED READ-ONLY WORKLOAD
export function readOnly() {
  const readKey = `key_${Math.floor(Math.random() * totalKeys)}`;
  const getRes = http.get(`${host}/data/mydb/mytable/${readKey}`, {
    headers: {
      'Connection': 'keep-alive'       // Keep TCP connections open and warm
    }
  });

  readCountMetric.add(1);
  readTrendMetric.add(getRes.timings.duration);
  check(getRes, { 'read success': (r) => r.status >= 200 && r.status < 300 || r.status === 404 });
}

// Clean ASCII text summary output generator
export function handleSummary(data) {
  // Extract and calculate honest Write metrics
  const writeCount = data.metrics.custom_write_count ? data.metrics.custom_write_count.values.count : 0;
  const writeHonestRps = (writeCount / phaseDurationSeconds).toFixed(2);
  const writeP95 = data.metrics.custom_write_latency ? data.metrics.custom_write_latency.values['p(95)'].toFixed(2) : '0.00';

  // Extract and calculate honest Read metrics
  const readCount = data.metrics.custom_read_count ? data.metrics.custom_read_count.values.count : 0;
  const readHonestRps = (readCount / phaseDurationSeconds).toFixed(2);
  const readP95 = data.metrics.custom_read_latency ? data.metrics.custom_read_latency.values['p(95)'].toFixed(2) : '0.00';

  return {
    'stdout': `
==================================================
        BENCHMARK REPORT
==================================================
 WRITE PERFORMANCE:
  - Write Throughput:       ${writeHonestRps} req/s (RPS)
  - Total Writes Attempted: ${writeCount} requests
  - Write Latency (p95):    ${writeP95} ms

 READ PERFORMANCE:
  - Read Throughput:        ${readHonestRps} req/s (RPS)
  - Total Reads Attempted:  ${readCount} requests
  - Read Latency (p95):     ${readP95} ms
==================================================
\n`,
  };
}
