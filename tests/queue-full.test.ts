import axios from 'axios';
import { redisClient } from '../gateway/src/redis';

async function runTest() {
  console.log('--- Queue Full Integration Test ---');
  
  // Set queue depth to exactly 1000 to trigger 503 on next request
  await redisClient.set('queue_depth', '1000');
  // Also force Circuit Breaker OPEN so it tries to queue
  await redisClient.set('cb:http://localhost:4000:state', 'OPEN');

  try {
    const res = await axios.get('http://localhost:3000/api/test', {
      headers: { 'X-Tenant-ID': 'tenantA' },
      validateStatus: () => true // don't throw on 503
    });

    console.log(`Status Code: ${res.status} (Expected: 503)`);
    console.log(`Response Body: ${JSON.stringify(res.data)}`);

    if (res.status === 503 && res.data.error === 'queue_full' && res.data.retry_after === 30) {
      console.log('✅ Queue Full Test Passed! Received strict 503 contract.');
    } else {
      console.log('❌ Queue Full Test Failed!');
      process.exit(1);
    }
  } catch (err: any) {
    console.error('Test error:', err.message);
    process.exit(1);
  }

  // Cleanup
  await redisClient.set('queue_depth', '0');
  await redisClient.del('cb:http://localhost:4000:state');
  process.exit(0);
}

runTest();
