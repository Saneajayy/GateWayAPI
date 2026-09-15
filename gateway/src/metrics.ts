import { EventEmitter } from 'events';
import { redisClient } from './redis';

export const metricsBus = new EventEmitter();

const PUBSUB_CHANNEL = 'metrics_pubsub';
const pubClient = redisClient.duplicate();
const subClient = redisClient.duplicate();

subClient.subscribe(PUBSUB_CHANNEL).catch(console.error);
subClient.on('message', (channel, message) => {
  if (channel === PUBSUB_CHANNEL) {
    try {
      const data = JSON.parse(message);
      if (data.type === 'request') {
        metricsBus.emit('request', data.payload);
      }
    } catch (e) {}
  }
});

export function publishWorkerRequest(tenantId: string, latencyMs: number, status: number) {
  pubClient.publish(PUBSUB_CHANNEL, JSON.stringify({
    type: 'request',
    payload: { tenantId, latencyMs, status, isQueued: true }
  }));
}

// Helper to emit request metrics
export function recordRequest(tenantId: string, latencyMs: number, status: number, isQueued: boolean = false) {
  metricsBus.emit('request', { tenantId, latencyMs, status, isQueued });
}

export function recordRejection(tenantId: string, reason: string) {
  metricsBus.emit('rejection', { tenantId, reason });
}

export function recordCircuitState(targetId: string, state: string) {
  metricsBus.emit('circuit-state', { targetId, state });
}

export function recordQueueDepth(depth: number) {
  metricsBus.emit('queue-depth', { depth });
}
