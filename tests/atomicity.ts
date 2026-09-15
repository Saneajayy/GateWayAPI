import { TokenBucketRateLimiter } from '../gateway/src/rate-limiter/TokenBucket';
import { redisClient } from '../gateway/src/redis';

async function runAtomicityTest() {
  const limiter = new TokenBucketRateLimiter(redisClient);
  const tenantId = 'atomicity-test-tenant';
  const config = { algorithm: 'token_bucket', limit: 15, rateOrWindow: 100 }; // 15 req allowed
  
  console.log('Clearing old redis state...');
  await redisClient.del(`rl:tb:${tenantId}`);
  
  console.log('Firing 100 parallel requests...');
  
  // Fire 100 requests completely in parallel
  const promises = [];
  for (let i = 0; i < 100; i++) {
    promises.push(limiter.isAllowed(tenantId, config as any));
  }
  
  const results = await Promise.all(promises);
  
  const allowed = results.filter(r => r.allowed).length;
  const rejected = results.filter(r => !r.allowed).length;
  
  console.log(`\n--- RESULTS ---`);
  console.log(`Total Requests: 100`);
  console.log(`Allowed: ${allowed} (Expected: 15)`);
  console.log(`Rejected: ${rejected} (Expected: 85)`);
  
  if (allowed === 15 && rejected === 85) {
    console.log('\n✅ ATOMICITY TEST PASSED: Zero race conditions detected. The Lua script perfectly isolated parallel executions.');
  } else {
    console.log('\n❌ ATOMICITY TEST FAILED: Race condition leaked requests.');
  }
  
  process.exit(0);
}

runAtomicityTest().catch(console.error);
