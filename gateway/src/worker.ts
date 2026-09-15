import { consumer } from './kafka';
import axios from 'axios';
import { CircuitBreaker } from './circuit-breaker/CircuitBreaker';
import { redisClient } from './redis';
import { publishWorkerRequest } from './metrics';

const DOWNSTREAM_URL = process.env.DOWNSTREAM_URL || 'http://localhost:4000';
const cb = new CircuitBreaker(redisClient);
const cbConfig = { windowMs: 10000, thresholdPercent: 50, minVolume: 5, cooldownMs: 10000 };

const QUEUE_TIMEOUT_MS = 300000;

// ─────────────────────────────────────────────────────────────────────────────
// Concurrency Semaphore
// Limits parallel downstream calls to WORKER_CONCURRENCY slots.
// With 50ms downstream latency, concurrency=5 → ~100 req/sec drain rate,
// matching the original traffic rate so the queue fills and drains
// at the same pace — intentional and symmetric for the demo.
//
// This also prevents the "thundering herd" problem: dumping all 2000+ queued
// requests onto the downstream simultaneously on circuit CLOSE could re-crash
// it immediately — defeating the entire purpose of the circuit breaker.
// ─────────────────────────────────────────────────────────────────────────────
const WORKER_CONCURRENCY = 5;

class Semaphore {
  private slots: number;
  private queue: Array<() => void> = [];

  constructor(concurrency: number) {
    this.slots = concurrency;
  }

  async acquire(): Promise<void> {
    if (this.slots > 0) {
      this.slots--;
      return;
    }
    return new Promise(resolve => this.queue.push(resolve));
  }

  release(): void {
    const next = this.queue.shift();
    if (next) {
      next(); // hand slot directly to next waiter
    } else {
      this.slots++;
    }
  }

  get active(): number {
    return WORKER_CONCURRENCY - this.slots;
  }
}

const semaphore = new Semaphore(WORKER_CONCURRENCY);

async function processMessage(message: string) {
  const data = JSON.parse(message);

  // Check message timeout — decrement and discard
  if (Date.now() - data.queuedAt > QUEUE_TIMEOUT_MS) {
    await redisClient.decr('queue_depth');
    console.log(`[Worker] Request ${data.trackingId} expired in queue.`);
    return;
  }

  // Check if a reset occurred after this message was queued — decrement and discard
  const resetTs = await redisClient.get('reset_ts');
  if (resetTs && data.queuedAt < parseInt(resetTs, 10)) {
    await redisClient.decr('queue_depth');
    console.log(`[Worker] Skipping stale request ${data.trackingId} (pre-reset).`);
    return;
  }

  // Acquire a concurrency slot — this is where back-pressure happens.
  // While the circuit is OPEN, slots are held here and queue_depth stays high.
  await semaphore.acquire();

  try {
    let state = await cb.checkState(DOWNSTREAM_URL);
    while (state === 'OPEN') {
      console.log(`[Worker] Circuit OPEN, waiting 2s...`);
      await new Promise(r => setTimeout(r, 2000));
      state = await cb.checkState(DOWNSTREAM_URL);
    }

    console.log(`[Worker] Processing ${data.trackingId} (slots: ${semaphore.active}/${WORKER_CONCURRENCY})`);
    const res = await axios({
      method: data.method,
      url: `${DOWNSTREAM_URL}${data.path}`,
      headers: data.headers,
      data: data.body,
      timeout: 5000,
    });

    await cb.recordResult(DOWNSTREAM_URL, true, cbConfig).catch(console.error);
    const latency = Date.now() - data.queuedAt;
    publishWorkerRequest(data.tenantId || 'unknown', latency, res.status);
    console.log(`[Worker] Request ${data.trackingId} succeeded: ${res.status}`);
  } catch (err: any) {
    await cb.recordResult(DOWNSTREAM_URL, false, cbConfig).catch(console.error);
    const latency = Date.now() - data.queuedAt;
    publishWorkerRequest(data.tenantId || 'unknown', latency, err.response?.status || 500);
    console.error(`[Worker] Request ${data.trackingId} failed:`, err.message);
  } finally {
    // Decrement only when the message is truly done (success or fail)
    // This keeps queue_depth accurate while messages wait for circuit to close
    await redisClient.decr('queue_depth');
    semaphore.release();
  }
}

async function startWorker() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'pending-requests', fromBeginning: false });

  await consumer.run({
    autoCommitThreshold: 10,
    eachMessage: async ({ message }) => {
      if (message.value) {
        // Fire-and-forget — don't await full processMessage so KafkaJS keeps
        // reading from the Kafka buffer while downstream calls are in flight.
        // The semaphore caps actual in-flight downstream calls at WORKER_CONCURRENCY.
        processMessage(message.value.toString()).catch(console.error);
      }
    },
  });

  console.log(`Worker listening (concurrency: ${WORKER_CONCURRENCY})...`);
}

startWorker().catch(console.error);
