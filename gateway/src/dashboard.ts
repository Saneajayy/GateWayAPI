import { Server } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { metricsBus } from './metrics';
import express from 'express';
import path from 'path';
import { admin } from './kafka';
import { redisClient } from './redis';
import { CircuitBreaker } from './circuit-breaker/CircuitBreaker';
import axios from 'axios';

const cb = new CircuitBreaker(redisClient);
const DOWNSTREAM_URL = process.env.DOWNSTREAM_URL || 'http://localhost:4000';

// Always-ready helper: ensures tenantA exists with the right limit.
// Called both by the explicit "Configure Tenant" step AND by Reset,
// so skipping step 1 never causes silent 403 rejections.
  async function configureTenant() {
    const selfUrl = process.env.SELF_URL || `http://127.0.0.1:${process.env.PORT || 3000}`;
    await axios.post(`${selfUrl}/admin/tenants`, {
      id: 'tenantA',
      algorithm: 'sliding_window',
      limit: 600, // Reduced from 3000 to match new 20 req/sec load
      rateOrWindow: 60000
    });
  }

  export function setupDashboardRoutes(app: express.Application) {
    app.use('/dashboard', express.static(path.join(__dirname, '../public')));
    app.use('/dashboard/api', express.json());

    // --- Terminal UI API Routes ---
    
    app.post('/dashboard/api/tenant', async (req, res) => {
      try {
        await configureTenant();
        res.json({ message: 'Tenant configured successfully (Limit: 600)' });
      } catch (e: any) {
        res.status(500).json({ error: e.message });
      }
    });

    app.post('/dashboard/api/fault', async (req, res) => {
      try {
        await axios.post(`${DOWNSTREAM_URL}/admin/fault`, { state: 'failing' });
        res.json({ message: 'Downstream mock set to failing' });
      } catch (e: any) {
        res.status(500).json({ error: e.message });
      }
    });

    app.post('/dashboard/api/recover', async (req, res) => {
      try {
        await axios.post(`${DOWNSTREAM_URL}/admin/fault`, { state: 'healthy' });
        res.json({ message: 'Downstream mock set to healthy' });
      } catch (e: any) {
        res.status(500).json({ error: e.message });
      }
    });

    app.post('/dashboard/api/load', (req, res) => {
      // On Railway PORT is dynamic — use SELF_URL or fall back to localhost:PORT
      const selfUrl = process.env.SELF_URL || `http://127.0.0.1:${process.env.PORT || 3000}`;
      const targetUrl = `${selfUrl}/api/test`;
      const durationMs = 50000;
      const intervalMs = 250; // Increased interval to ease CPU
      const reqsPerInterval = 5; // 5 reqs / 250ms = 20 reqs/sec
      let elapsed = 0;

    const intervalId = setInterval(() => {
      elapsed += intervalMs;
      if (elapsed >= durationMs) {
        clearInterval(intervalId);
        return;
      }
      
      const batch = [];
      for (let i = 0; i < reqsPerInterval; i++) {
        batch.push(axios.get(targetUrl, {
          headers: { 'X-Tenant-ID': 'tenantA' },
          validateStatus: () => true
        }));
      }
      Promise.allSettled(batch).catch(console.error);
    }, intervalMs);

    res.json({ message: `Started sustained traffic simulation (20 rps for 50s)` });
  });

  app.post('/dashboard/api/reset', async (req, res) => {
    try {
      // 1. Reset Mock Server to healthy
      await axios.post(`${DOWNSTREAM_URL}/admin/fault`, { state: 'healthy' });
      
      // 2. Auto-configure the tenant so the simulation works even if step 1 was skipped
      await configureTenant().catch(e => console.error('[Reset] Could not configure tenant:', e.message));
      
      // 2. Clear ALL Redis state atomically
      const baseKey = `cb:${DOWNSTREAM_URL}`;
      const rateLimitKeys = await redisClient.keys('rate_limit:*');
      const keysToDelete = [
        `${baseKey}:state`,
        `${baseKey}:trials`,
        `${baseKey}:successes`,
        `${baseKey}:was_open`,
        'queue_depth',
        'reset_ts',
        ...rateLimitKeys
      ];
      if (keysToDelete.length > 0) {
        await redisClient.del(...keysToDelete);
      }

      // 3. Immediately zero the queue depth counter in Redis.
      // The worker will keep consuming old messages but reset_ts will make it skip them.
      // queue_depth decrements still happen on skip, but we SET it to 0 now, so it just
      // goes slightly negative (clamped to 0 on the dashboard display).
      await redisClient.set('queue_depth', '0');
      console.log('[Reset] queue_depth zeroed in Redis.');

      // 4. Set a fresh reset_ts so any in-flight worker messages are also skipped
      await redisClient.set('reset_ts', Date.now().toString());

      res.json({ ok: true, message: 'Full reset complete. Queue is empty and all state is cleared.' });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // --- End Terminal UI API Routes ---
}

export function setupDashboardSockets(server: Server) {
  const wss = new WebSocketServer({ server, path: '/ws' });

  const state = {
    reqsPerSec: 0,
    rejectionsPerSec: 0,
    queueDepth: 0,
    circuitState: 'CLOSED',
    latencies: [] as number[],
  };

  metricsBus.on('request', (data) => {
    state.reqsPerSec++;
    state.latencies.push(data.latencyMs);
    // broadcast(JSON.stringify({ type: 'request', data })); // Too noisy under load
  });

  metricsBus.on('rejection', (data) => {
    state.rejectionsPerSec++;
  });

  metricsBus.on('circuit-state', (data) => {
    state.circuitState = data.state;
    broadcast(JSON.stringify({ type: 'circuit-state', data }));
  });

  metricsBus.on('queue-depth', (data) => {
    state.queueDepth = data.depth;
    broadcast(JSON.stringify({ type: 'queue-depth', data }));
  });

  function broadcast(msg: string) {
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(msg);
      }
    });
  }

  // Calculate percentiles
  const getPercentile = (arr: number[], p: number) => {
    if (arr.length === 0) return 0;
    const sorted = arr.slice().sort((a, b) => a - b);
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[index];
  };

  // Periodic summary broadcast
  setInterval(async () => {
    try {
      const redisDepth = await redisClient.get('queue_depth');
      state.queueDepth = Math.max(0, parseInt(redisDepth || '0', 10));

      // Also sync Circuit Breaker state since worker updates it in another process
      // We use peekState so we don't accidentally consume HALF_OPEN trial requests!
      const cbState = await cb.peekState(DOWNSTREAM_URL);
      if (cbState) {
        state.circuitState = cbState;
      }
    } catch (err) {
      console.error('Error fetching Kafka lag:', err);
    }

    const summary = {
      type: 'summary',
      data: {
        rps: state.reqsPerSec,
        rejections: state.rejectionsPerSec,
        queueDepth: state.queueDepth,
        circuitState: state.circuitState,
        p50: getPercentile(state.latencies, 50),
        p95: getPercentile(state.latencies, 95),
        p99: getPercentile(state.latencies, 99),
      }
    };
    broadcast(JSON.stringify(summary));

    // Reset for next window
    state.reqsPerSec = 0;
    state.rejectionsPerSec = 0;
    state.latencies = [];
  }, 1000);
}
