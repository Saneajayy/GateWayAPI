import { CircuitBreaker } from '../gateway/src/circuit-breaker/CircuitBreaker';
import { redisClient } from '../gateway/src/redis';

async function runTest() {
  const cb = new CircuitBreaker(redisClient);
  const targetId = 'test-target';
  const config = { windowMs: 10000, thresholdPercent: 50, minVolume: 2, cooldownMs: 2000 };

  console.log('--- Circuit Breaker Transition Test ---');
  await redisClient.del(`cb:${targetId}:state`, `cb:${targetId}:was_open`, `cb:${targetId}:trials`, `cb:${targetId}:successes`, `cb:${targetId}:backoff`);
  
  // 1. Initially CLOSED
  let state = await cb.checkState(targetId);
  console.log(`Initial state: ${state} (Expected: CLOSED)`);
  if (state !== 'CLOSED') throw new Error('Failed initial state');

  // 2. Fail 2 requests (minVolume is 2, 100% failure rate)
  await cb.recordResult(targetId, false, config);
  let stateAfterFail = await cb.recordResult(targetId, false, config);
  console.log(`State after failures: ${stateAfterFail} (Expected: OPEN)`);
  if (stateAfterFail !== 'OPEN') throw new Error('Failed to trip OPEN');

  // 3. Wait for cooldown to expire
  console.log('Waiting for cooldown (2.1s)...');
  await new Promise(r => setTimeout(r, 2100));

  // 4. Next check should transition to HALF_OPEN and decrement trial to 2
  state = await cb.checkState(targetId);
  console.log(`State after cooldown: ${state} (Expected: HALF_OPEN)`);
  if (state !== 'HALF_OPEN') throw new Error('Failed to transition to HALF_OPEN');

  // 5. Consume remaining 2 trials
  let t2 = await cb.checkState(targetId);
  let t3 = await cb.checkState(targetId);
  console.log(`Consuming trials: ${t2}, ${t3} (Expected: HALF_OPEN, HALF_OPEN)`);
  if (t2 !== 'HALF_OPEN' || t3 !== 'HALF_OPEN') throw new Error('Trials failed');

  // 6. Next request should be treated as OPEN because trials are exhausted
  let t4 = await cb.checkState(targetId);
  console.log(`Check when trials exhausted: ${t4} (Expected: OPEN)`);
  if (t4 !== 'OPEN') throw new Error('Trial limit not enforced');

  // 7. Fail a trial request -> Should immediately trip back to OPEN with exponential backoff
  state = await cb.recordResult(targetId, false, config);
  console.log(`State after trial failed: ${state} (Expected: OPEN)`);
  if (state !== 'OPEN') throw new Error('Failed to trip OPEN on trial fail');

  // Wait for new backoff (2 * cooldownMs = 4000ms)
  console.log('Waiting for new exponential backoff cooldown (4.1s)...');
  await new Promise(r => setTimeout(r, 4100));

  // 8. Next check should transition to HALF_OPEN again
  state = await cb.checkState(targetId);
  console.log(`State after 2nd cooldown: ${state} (Expected: HALF_OPEN)`);

  // 9. Succeed 3 trials to heal
  await cb.recordResult(targetId, true, config);
  await cb.recordResult(targetId, true, config);
  state = await cb.recordResult(targetId, true, config);
  console.log(`State after 3 trial successes: ${state} (Expected: CLOSED)`);
  if (state !== 'CLOSED') throw new Error('Failed to heal to CLOSED');

  console.log('✅ Circuit Breaker State Transition Test Passed!');
  process.exit(0);
}

runTest().catch(err => {
  console.error(err);
  process.exit(1);
});
