import { Redis } from 'ioredis';
import { recordCircuitState } from '../metrics';

const RECORD_LUA = `
  local base_key = KEYS[1]
  local is_success = tonumber(ARGV[1])
  local window_ms = tonumber(ARGV[2])
  local threshold_percent = tonumber(ARGV[3])
  local min_volume = tonumber(ARGV[4])
  local cooldown_ms = tonumber(ARGV[5])
  local now = tonumber(ARGV[6])

  local state_key = base_key .. ':state'
  local state = redis.call('GET', state_key)

  if not state then
    local was_open = redis.call('GET', base_key .. ':was_open')
    if was_open == '1' then
      state = 'HALF_OPEN'
      redis.call('SET', state_key, state)
      redis.call('SET', base_key .. ':trials', '3')
      redis.call('SET', base_key .. ':successes', '0')
    else
      state = 'CLOSED'
      redis.call('SET', state_key, state)
    end
  end

  if state == 'OPEN' then
    return state
  end

  if state == 'HALF_OPEN' then
    if is_success == 0 then
      -- Fail in half-open -> trip back to open, exponential backoff
      local backoff = tonumber(redis.call('GET', base_key .. ':backoff') or tostring(cooldown_ms))
      local new_backoff = math.min(backoff * 2, 60000)
      redis.call('SET', base_key .. ':backoff', tostring(new_backoff))
      
      redis.call('SET', state_key, 'OPEN', 'PX', new_backoff)
      redis.call('SET', base_key .. ':was_open', '1', 'PX', new_backoff * 2)
      return 'OPEN'
    else
      -- Success in half-open
      local successes = redis.call('INCR', base_key .. ':successes')
      if tonumber(successes) >= 3 then
        redis.call('SET', state_key, 'CLOSED')
        redis.call('DEL', base_key .. ':was_open')
        redis.call('DEL', base_key .. ':backoff')
        return 'CLOSED'
      else
        return 'HALF_OPEN'
      end
    end
  end

  -- State is CLOSED. Record metrics using sliding window.
  local curr_window_start = math.floor(now / window_ms) * window_ms
  local prev_window_start = curr_window_start - window_ms

  local total_curr_key = base_key .. ':t:' .. curr_window_start
  local total_prev_key = base_key .. ':t:' .. prev_window_start
  local fail_curr_key = base_key .. ':f:' .. curr_window_start
  local fail_prev_key = base_key .. ':f:' .. prev_window_start

  local elapsed_ms = now - curr_window_start
  local prev_weight = 1 - (elapsed_ms / window_ms)

  redis.call('INCR', total_curr_key)
  redis.call('PEXPIRE', total_curr_key, window_ms * 2)
  if is_success == 0 then
    redis.call('INCR', fail_curr_key)
    redis.call('PEXPIRE', fail_curr_key, window_ms * 2)
  end

  local total_curr = tonumber(redis.call('GET', total_curr_key) or '0')
  local total_prev = tonumber(redis.call('GET', total_prev_key) or '0')
  local fail_curr = tonumber(redis.call('GET', fail_curr_key) or '0')
  local fail_prev = tonumber(redis.call('GET', fail_prev_key) or '0')

  local est_total = total_prev * prev_weight + total_curr
  local est_fail = fail_prev * prev_weight + fail_curr

  if est_total >= min_volume then
    local error_rate = (est_fail / est_total) * 100
    if error_rate >= threshold_percent then
      -- Trip breaker
      redis.call('SET', state_key, 'OPEN', 'PX', cooldown_ms)
      redis.call('SET', base_key .. ':was_open', '1', 'PX', cooldown_ms * 2)
      redis.call('SET', base_key .. ':backoff', tostring(cooldown_ms))
      return 'OPEN'
    end
  end

  return 'CLOSED'
`;

const CHECK_LUA = `
  local base_key = KEYS[1]
  local state_key = base_key .. ':state'
  local state = redis.call('GET', state_key)

  if not state then
    local was_open = redis.call('GET', base_key .. ':was_open')
    if was_open == '1' then
      -- Transition to HALF_OPEN
      redis.call('SET', state_key, 'HALF_OPEN')
      redis.call('SET', base_key .. ':trials', '2') -- First check consumes a trial
      redis.call('SET', base_key .. ':successes', '0')
      return 'HALF_OPEN'
    end
    return 'CLOSED'
  end

  if state == 'HALF_OPEN' then
    local trials = tonumber(redis.call('GET', base_key .. ':trials') or '0')
    if trials > 0 then
      redis.call('DECR', base_key .. ':trials')
      return 'HALF_OPEN'
    else
      return 'OPEN'
    end
  end

  return state
`;

const PEEK_LUA = `
  local base_key = KEYS[1]
  local state_key = base_key .. ':state'
  local state = redis.call('GET', state_key)

  if not state then
    local was_open = redis.call('GET', base_key .. ':was_open')
    if was_open == '1' then
      return 'HALF_OPEN'
    end
    return 'CLOSED'
  end
  
  -- If HALF_OPEN but no trials, it acts as OPEN to clients
  if state == 'HALF_OPEN' then
    local trials = tonumber(redis.call('GET', base_key .. ':trials') or '0')
    if trials == 0 then
      return 'OPEN'
    end
  end

  return state
`;

export interface CircuitBreakerConfig {
  windowMs: number;
  thresholdPercent: number;
  minVolume: number;
  cooldownMs: number;
}

export class CircuitBreaker {
  private redis: Redis;
  private lastState: string = 'CLOSED';

  constructor(redisClient: Redis) {
    this.redis = redisClient;
    this.redis.defineCommand('cbRecord', { numberOfKeys: 1, lua: RECORD_LUA });
    this.redis.defineCommand('cbCheck', { numberOfKeys: 1, lua: CHECK_LUA });
    this.redis.defineCommand('cbPeek', { numberOfKeys: 1, lua: PEEK_LUA });
  }

  private updateState(targetId: string, newState: string) {
    if (this.lastState !== newState) {
      if (this.lastState) {
        console.log(`[CircuitBreaker] ${targetId} transitioned: ${this.lastState} -> ${newState}`);
      }
      this.lastState = newState;
      recordCircuitState(targetId, newState);
    }
  }

  async checkState(targetId: string): Promise<'CLOSED' | 'OPEN' | 'HALF_OPEN'> {
    const key = `cb:${targetId}`;
    // @ts-ignore
    const state = await this.redis.cbCheck(key) as string;
    
    if (state === 'HALF_OPEN') {
      console.log(`[CircuitBreaker] Trial slot granted for ${targetId}`);
    }
    
    this.updateState(targetId, state);
    return state as 'CLOSED' | 'OPEN' | 'HALF_OPEN';
  }

  async peekState(targetId: string): Promise<'CLOSED' | 'OPEN' | 'HALF_OPEN'> {
    const key = `cb:${targetId}`;
    // @ts-ignore
    const state = await this.redis.cbPeek(key) as string;
    return state as 'CLOSED' | 'OPEN' | 'HALF_OPEN';
  }

  async recordResult(targetId: string, isSuccess: boolean, config: CircuitBreakerConfig): Promise<'CLOSED' | 'OPEN' | 'HALF_OPEN'> {
    const key = `cb:${targetId}`;
    const now = Date.now();
    // @ts-ignore
    const state = await this.redis.cbRecord(
      key, 
      isSuccess ? 1 : 0, 
      config.windowMs, 
      config.thresholdPercent, 
      config.minVolume, 
      config.cooldownMs, 
      now
    ) as string;
    
    if (this.lastState === 'HALF_OPEN' && state === 'HALF_OPEN' && isSuccess) {
      console.log(`[CircuitBreaker] Trial request succeeded for ${targetId}`);
    }

    this.updateState(targetId, state);
    return state as 'CLOSED' | 'OPEN' | 'HALF_OPEN';
  }
}
