import { RateLimitConfig } from './rate-limiter/RateLimiter';
import { pgPool } from './db';

interface CacheEntry {
  config: RateLimitConfig;
  expiresAt: number;
}

const CACHE_TTL_MS = 60000; // 1 minute
const cache = new Map<string, CacheEntry>();

export async function getTenantConfig(tenantId: string): Promise<RateLimitConfig | null> {
  const now = Date.now();
  const entry = cache.get(tenantId);

  if (entry && entry.expiresAt > now) {
    return entry.config;
  }

  // Fetch from DB
  const res = await pgPool.query('SELECT algorithm, rate_limit, rate_or_window FROM tenants WHERE id = $1', [tenantId]);
  if (res.rows.length === 0) {
    return null;
  }

  const config: RateLimitConfig = {
    algorithm: res.rows[0].algorithm,
    limit: res.rows[0].rate_limit,
    rateOrWindow: res.rows[0].rate_or_window,
  };

  cache.set(tenantId, {
    config,
    expiresAt: now + CACHE_TTL_MS,
  });

  return config;
}

export function invalidateCache(tenantId: string) {
  cache.delete(tenantId);
}
