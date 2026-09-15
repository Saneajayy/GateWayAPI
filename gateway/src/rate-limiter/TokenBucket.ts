import { Redis } from 'ioredis';
import { RateLimiter, RateLimitConfig, RateLimiterResponse } from './RateLimiter';

const LUA_SCRIPT = `
  local key = KEYS[1]
  local capacity = tonumber(ARGV[1])
  local refill_rate = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])
  local requested = 1

  local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
  local tokens = tonumber(bucket[1])
  local last_refill = tonumber(bucket[2])

  if not tokens or not last_refill then
    tokens = capacity
    last_refill = now
  end

  local elapsed_ms = math.max(0, now - last_refill)
  local refilled = math.floor(elapsed_ms * refill_rate / 1000)

  if refilled > 0 then
    tokens = math.min(capacity, tokens + refilled)
    -- only advance last_refill by the time it took to generate 'refilled' tokens
    last_refill = last_refill + math.floor((refilled * 1000) / refill_rate)
  end

  local allowed = 0
  if tokens >= requested then
    allowed = 1
    tokens = tokens - requested
  end

  redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
  
  -- Expire the key when it would fully refill. 
  -- time to refill fully = (capacity / refill_rate) seconds
  local ttl = math.ceil(capacity / refill_rate)
  redis.call('EXPIRE', key, ttl)

  return { allowed, tokens, last_refill }
`;

export class TokenBucketRateLimiter implements RateLimiter {
  private redis: Redis;

  constructor(redisClient: Redis) {
    this.redis = redisClient;
    // Define the custom command
    this.redis.defineCommand('tokenBucketLimiter', {
      numberOfKeys: 1,
      lua: LUA_SCRIPT,
    });
  }

  async isAllowed(tenantId: string, config: RateLimitConfig): Promise<RateLimiterResponse> {
    const key = `rate_limit:tb:${tenantId}`;
    const capacity = config.limit;
    const refillRate = config.rateOrWindow;
    const now = Date.now();

    // @ts-ignore - custom command
    const result = await this.redis.tokenBucketLimiter(
      key,
      capacity,
      refillRate,
      now
    );

    const allowed = result[0] === 1;
    const remaining = result[1];
    const lastRefill = result[2];

    // Calculate reset time: when the next token will be available
    // time to next token = 1000 / refillRate ms from last_refill
    const msToNextToken = (1000 / refillRate) - (now - lastRefill);
    const resetTimeMs = now + Math.max(0, msToNextToken);

    return {
      allowed,
      limit: capacity,
      remaining,
      reset: Math.floor(resetTimeMs / 1000) // in seconds
    };
  }
}
