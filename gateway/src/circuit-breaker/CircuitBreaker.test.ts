import Redis from 'ioredis';
import { CircuitBreaker, CircuitBreakerConfig } from './CircuitBreaker';

describe('CircuitBreaker', () => {
  let redis: Redis;
  let cb: CircuitBreaker;

  const config: CircuitBreakerConfig = {
    windowMs: 1000,
    thresholdPercent: 50,
    minVolume: 5,
    cooldownMs: 500, // Short cooldown for testing
  };

  beforeAll(() => {
    redis = new Redis('redis://localhost:6380');
    cb = new CircuitBreaker(redis);
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await redis.flushdb();
  });

  it('should remain CLOSED if error rate is below threshold', async () => {
    const target = 'target1';
    
    // 4 successes, 1 fail -> 20% error rate
    for (let i = 0; i < 4; i++) {
      await cb.recordResult(target, true, config);
    }
    const state = await cb.recordResult(target, false, config);
    
    expect(state).toBe('CLOSED');
  });

  it('should trip to OPEN if error rate exceeds threshold', async () => {
    const target = 'target2';
    
    // 2 successes, 3 fails -> 60% error rate -> should trip on 5th
    await cb.recordResult(target, true, config);
    await cb.recordResult(target, true, config);
    await cb.recordResult(target, false, config);
    await cb.recordResult(target, false, config);
    const state = await cb.recordResult(target, false, config);
    
    expect(state).toBe('OPEN');
    
    // Check state is now OPEN
    const checkedState = await cb.checkState(target);
    expect(checkedState).toBe('OPEN');
  });

  it('should transition to HALF_OPEN after cooldown, and then back to CLOSED on success', async () => {
    const target = 'target3';
    
    // Trip it
    for (let i = 0; i < 5; i++) {
      await cb.recordResult(target, false, config);
    }
    expect(await cb.checkState(target)).toBe('OPEN');

    // Wait for cooldown
    await new Promise((resolve) => setTimeout(resolve, 600));

    // Check should be HALF_OPEN
    const halfOpenState = await cb.checkState(target);
    expect(halfOpenState).toBe('HALF_OPEN');

    // Success -> CLOSED
    const finalState = await cb.recordResult(target, true, config);
    expect(finalState).toBe('CLOSED');
  });

  it('should transition to HALF_OPEN after cooldown, and back to OPEN on failure', async () => {
    const target = 'target4';
    
    // Trip it
    for (let i = 0; i < 5; i++) {
      await cb.recordResult(target, false, config);
    }
    expect(await cb.checkState(target)).toBe('OPEN');

    // Wait for cooldown
    await new Promise((resolve) => setTimeout(resolve, 600));

    // Fail -> OPEN
    const stateAfterFail = await cb.recordResult(target, false, config);
    expect(stateAfterFail).toBe('OPEN');
  });
});
