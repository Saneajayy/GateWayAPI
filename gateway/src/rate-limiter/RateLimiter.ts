export interface RateLimiterResponse {
  allowed: boolean;
  limit: number;
  remaining: number;
  reset: number; // Unix timestamp in seconds
}

export interface RateLimitConfig {
  algorithm: 'token_bucket' | 'sliding_window';
  limit: number;
  rateOrWindow: number; // TokenBucket: refill rate (tokens/sec). SlidingWindow: window size (ms)
}

export interface RateLimiter {
  isAllowed(tenantId: string, config: RateLimitConfig): Promise<RateLimiterResponse>;
}
