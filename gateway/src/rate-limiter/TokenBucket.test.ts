import Redis from 'ioredis';
import { TokenBucketRateLimiter } from './TokenBucket';

describe('TokenBucketRateLimiter (Concurrency Test)', () => {
  let redis: Redis;
  let limiter: TokenBucketRateLimiter;

  beforeAll(() => {
    redis = new Redis('redis://localhost:6380');
    limiter = new TokenBucketRateLimiter(redis);
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await redis.flushdb(); // Clear DB before each test
  });

  it('should strictly limit requests exactly to capacity under high concurrency', async () => {
    const tenantId = 'test-tenant-1';
    const config = {
      algorithm: 'token_bucket' as const,
      limit: 10,
      rateOrWindow: 1, // 1 token per second
    };

    // Fire 50 concurrent requests when capacity is 10
    const promises = Array.from({ length: 50 }).map(() =>
      limiter.isAllowed(tenantId, config)
    );

    const results = await Promise.all(promises);

    const allowedCount = results.filter((res) => res.allowed).length;
    const rejectedCount = results.filter((res) => !res.allowed).length;

    expect(allowedCount).toBe(10);
    expect(rejectedCount).toBe(40);
  });

  it('should refill tokens over time', async () => {
    const tenantId = 'test-tenant-2';
    const config = {
      algorithm: 'token_bucket' as const,
      limit: 5,
      rateOrWindow: 10, // 10 tokens per second (1 token per 100ms)
    };

    // Consume all 5
    for (let i = 0; i < 5; i++) {
      const res = await limiter.isAllowed(tenantId, config);
      expect(res.allowed).toBe(true);
    }

    // Next should be blocked
    const blockedRes = await limiter.isAllowed(tenantId, config);
    expect(blockedRes.allowed).toBe(false);

    // Wait 250ms -> Should refill 2 tokens
    await new Promise((resolve) => setTimeout(resolve, 250));

    // Fire 3 concurrent requests
    const promises = Array.from({ length: 3 }).map(() =>
      limiter.isAllowed(tenantId, config)
    );
    const results = await Promise.all(promises);

    const allowedCount = results.filter((res) => res.allowed).length;
    expect(allowedCount).toBe(2);
  });
});
