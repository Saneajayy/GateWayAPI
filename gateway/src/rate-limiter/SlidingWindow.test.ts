import Redis from 'ioredis';
import { SlidingWindowRateLimiter } from './SlidingWindow';

describe('SlidingWindowRateLimiter', () => {
  let redis: Redis;
  let limiter: SlidingWindowRateLimiter;

  beforeAll(() => {
    redis = new Redis('redis://localhost:6380');
    limiter = new SlidingWindowRateLimiter(redis);
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await redis.flushdb();
  });

  it('should strictly limit requests under high concurrency', async () => {
    const tenantId = 'test-sw-1';
    const config = {
      algorithm: 'sliding_window' as const,
      limit: 10,
      rateOrWindow: 1000, // 1 second window
    };

    const promises = Array.from({ length: 50 }).map(() =>
      limiter.isAllowed(tenantId, config)
    );

    const results = await Promise.all(promises);

    const allowedCount = results.filter((res) => res.allowed).length;
    const rejectedCount = results.filter((res) => !res.allowed).length;

    expect(allowedCount).toBe(10);
    expect(rejectedCount).toBe(40);
  });

  it('should allow more requests as the window slides', async () => {
    const tenantId = 'test-sw-2';
    const config = {
      algorithm: 'sliding_window' as const,
      limit: 5,
      rateOrWindow: 500, // 500ms window
    };

    // Consume all 5
    for (let i = 0; i < 5; i++) {
      const res = await limiter.isAllowed(tenantId, config);
      expect(res.allowed).toBe(true);
    }

    // Should be blocked
    const blocked = await limiter.isAllowed(tenantId, config);
    expect(blocked.allowed).toBe(false);

    // Wait for the window to partially slide (e.g., 260ms -> approx 50% weight reduced)
    await new Promise((resolve) => setTimeout(resolve, 260));

    // After 260ms, the previous window weight is approx 0.48. 
    // 5 * 0.48 = 2.4 => estimated_count is 2. So we should have limit - estimated = 5 - 2 = 3 requests available.
    
    // We should be able to make at least 2 requests
    const res1 = await limiter.isAllowed(tenantId, config);
    expect(res1.allowed).toBe(true);
    const res2 = await limiter.isAllowed(tenantId, config);
    expect(res2.allowed).toBe(true);
  });
});
