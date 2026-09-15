import { Redis } from 'ioredis';
import { RateLimiter, RateLimitConfig, RateLimiterResponse } from './RateLimiter';

const LUA_SCRIPT = `
  local base_key = KEYS[1]
  local window_ms = tonumber(ARGV[1])
  local limit = tonumber(ARGV[2])
  local now = tonumber(ARGV[3])

  local curr_window_start = math.floor(now / window_ms) * window_ms
  local prev_window_start = curr_window_start - window_ms

  local curr_key = base_key .. ':' .. curr_window_start
  local prev_key = base_key .. ':' .. prev_window_start

  local elapsed_ms = now - curr_window_start
  local prev_weight = 1 - (elapsed_ms / window_ms)

  local prev_count = tonumber(redis.call('GET', prev_key) or '0')
  local curr_count = tonumber(redis.call('GET', curr_key) or '0')

  local estimated_count = prev_count * prev_weight + curr_count

  if estimated_count >= limit then
    return { 0, math.floor(limit - estimated_count) }
  end

  redis.call('INCR', curr_key)
  redis.call('PEXPIRE', curr_key, window_ms * 2)

  return { 1, math.floor(limit - (estimated_count + 1)) }
`;

export class SlidingWindowRateLimiter implements RateLimiter {
  private redis: Redis;

  constructor(redisClient: Redis) {
    this.redis = redisClient;
    this.redis.defineCommand('slidingWindowLimiter', {
      numberOfKeys: 1,
      lua: LUA_SCRIPT,
    });
  }

  async isAllowed(tenantId: string, config: RateLimitConfig): Promise<RateLimiterResponse> {
    const key = `rate_limit:sw:${tenantId}`;
    const windowMs = config.rateOrWindow;
    const limit = config.limit;
    const now = Date.now();

    // @ts-ignore - custom command
    const result = await this.redis.slidingWindowLimiter(
      key,
      windowMs,
      limit,
      now
    );

    const allowed = result[0] === 1;
    const remaining = Math.max(0, result[1]);

    // Reset time is the end of the current window
    const currentWindowStart = Math.floor(now / windowMs) * windowMs;
    const resetTimeMs = currentWindowStart + windowMs;

    return {
      allowed,
      limit,
      remaining,
      reset: Math.floor(resetTimeMs / 1000)
    };
  }
}
