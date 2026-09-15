import axios from 'axios';
import { redisClient } from '../gateway/src/redis';

async function runTest() {
  console.log('--- HALF_OPEN Edge Case Test ---');

  // 1. Configure the test tenant so requests don't get rejected with 403
  console.log('Configuring test tenant...');
  await axios.post('http://localhost:3000/admin/tenants', {
    id: 'test-tenant',
    algorithm: 'sliding_window',
    limit: 100,
    rateOrWindow: 60000
  });

  // 2. Force the Circuit Breaker cooldown to expire so next check yields HALF_OPEN
  console.log('Setting Circuit Breaker for recovery (cooldown expired)...');
  const baseKey = 'cb:http://localhost:4000';
  await redisClient.del(`${baseKey}:state`);
  await redisClient.set(`${baseKey}:was_open`, '1');
  
  // 2. Fire 5 concurrent requests at the Gateway
  // We expect exactly 3 to receive a trial slot (proxy synchronously, so either 200 or failure)
  // and 2 to be rejected as "OPEN" (and therefore queued, returning 202)
  console.log('Firing 5 concurrent requests at the gateway...');
  
  const requests = [];
  for (let i = 0; i < 5; i++) {
    requests.push(
      axios.get('http://localhost:3000/api/test', {
        headers: { 'X-Tenant-ID': 'test-tenant' },
        validateStatus: () => true
      })
    );
  }

  const responses = await Promise.all(requests);
  
  let syncProxied = 0;
  let queued = 0;

  responses.forEach((res, idx) => {
    if (res.status === 202) {
      console.log(`Request ${idx + 1}: Queued (Status 202)`);
      queued++;
    } else {
      console.log(`Request ${idx + 1}: Proxied Synchronously (Status ${res.status})`);
      syncProxied++;
    }
  });

  console.log(`\nSummary:`);
  console.log(`Trials Proxied: ${syncProxied} (Expected: 3)`);
  console.log(`Requests Queued: ${queued} (Expected: 2)`);

  if (syncProxied === 3 && queued === 2) {
    console.log('✅ HALF_OPEN edge case behaves perfectly: exactly 3 trials are permitted, remainder are queued!');
    process.exit(0);
  } else {
    console.log('❌ Unexpected behavior.');
    process.exit(1);
  }
}

runTest().catch(err => {
  console.error(err);
  process.exit(1);
});
