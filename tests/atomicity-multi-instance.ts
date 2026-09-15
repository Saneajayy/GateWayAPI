import axios from 'axios';
import { spawn } from 'child_process';
import { redisClient } from '../gateway/src/redis';

const PORTS = [3001, 3002, 3003];

async function runTest() {
  console.log('--- Multi-Instance Atomicity Test ---');
  
  // 1. Clear redis rate limits for tenant atomicity-test
  const tenantId = 'atomicity-test';
  await redisClient.del(`rl:${tenantId}:tokens`, `rl:${tenantId}:ts`, `rl:${tenantId}:reqs`);

  // 2. Spawn 3 gateway instances
  console.log('Spawning 3 gateway instances...');
  const instances = PORTS.map(port => {
    return spawn('npx', ['ts-node', 'gateway/src/index.ts'], {
      env: { ...process.env, PORT: port.toString() },
      stdio: 'ignore'
    });
  });

  // Give instances time to boot
  await new Promise(r => setTimeout(r, 5000));

  // 3. Configure tenant to 100 requests limit
  console.log('Configuring tenant to 100 limit...');
  await axios.post(`http://localhost:${PORTS[0]}/admin/tenants`, {
    id: tenantId,
    algorithm: 'sliding_window',
    limit: 100,
    rateOrWindow: 60000
  });

  // Wait for config to propagate to DB and cache
  await new Promise(r => setTimeout(r, 1000));

  // 4. Fire 200 concurrent requests distributed across the 3 instances
  console.log('Firing 200 concurrent requests across all instances...');
  const requests = [];
  for (let i = 0; i < 200; i++) {
    const port = PORTS[i % PORTS.length];
    requests.push(
      axios.get(`http://localhost:${port}/api/test`, {
        headers: { 'X-Tenant-ID': tenantId },
        validateStatus: () => true
      })
    );
  }

  const responses = await Promise.all(requests);

  let allowed = 0;
  let rejected = 0;

  responses.forEach(res => {
    if (res.status === 429) {
      rejected++;
    } else {
      allowed++;
    }
  });

  console.log(`Results across ${PORTS.length} processes:`);
  console.log(`Allowed: ${allowed} (Expected: 100)`);
  console.log(`Rejected: ${rejected} (Expected: 100)`);

  // Kill instances
  instances.forEach(p => p.kill());

  if (allowed === 100 && rejected === 100) {
    console.log('✅ Multi-Instance Atomicity Test Passed!');
    process.exit(0);
  } else {
    console.log('❌ Multi-Instance Atomicity Test Failed! Race condition detected.');
    process.exit(1);
  }
}

runTest().catch(err => {
  console.error('Test error:', err.message);
  process.exit(1);
});
