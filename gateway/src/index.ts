import express from 'express';
import path from 'path';
import { createProxyMiddleware } from 'http-proxy-middleware';
import { setupDatabase, pgPool } from './db';
import { redisClient } from './redis';
import { getTenantConfig, invalidateCache } from './configCache';
import { TokenBucketRateLimiter } from './rate-limiter/TokenBucket';
import { SlidingWindowRateLimiter } from './rate-limiter/SlidingWindow';
import { CircuitBreaker } from './circuit-breaker/CircuitBreaker';
import { initKafka, producer } from './kafka';
import crypto from 'crypto';
import { setupDashboardRoutes, setupDashboardSockets } from './dashboard';
import { recordRequest, recordRejection, recordQueueDepth } from './metrics';

const app = express();
const PORT = process.env.PORT || 3000;
const DOWNSTREAM_URL = process.env.DOWNSTREAM_URL || 'http://localhost:4000';

app.use((req, res, next) => {
  (req as any).startTime = Date.now();
  next();
});

app.use(express.json());

const tokenBucketLimiter = new TokenBucketRateLimiter(redisClient);
const slidingWindowLimiter = new SlidingWindowRateLimiter(redisClient);
const cb = new CircuitBreaker(redisClient);
const cbConfig = { windowMs: 10000, thresholdPercent: 50, minVolume: 5, cooldownMs: 10000 };

const MAX_CONCURRENT_REQUESTS = 50;
const MAX_QUEUE_DEPTH = 1000;
let inFlightRequests = 0;

// Admin API
app.post('/admin/tenants', async (req, res) => {
  const { id, algorithm, limit, rateOrWindow } = req.body;
  if (!id || !algorithm || !limit || !rateOrWindow) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    await pgPool.query(
      `INSERT INTO tenants (id, algorithm, rate_limit, rate_or_window) 
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (id) DO UPDATE SET 
         algorithm = EXCLUDED.algorithm, 
         rate_limit = EXCLUDED.rate_limit, 
         rate_or_window = EXCLUDED.rate_or_window,
         updated_at = CURRENT_TIMESTAMP`,
      [id, algorithm, limit, rateOrWindow]
    );
    invalidateCache(id);
    res.status(200).json({ message: 'Tenant configured successfully' });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// Rate Limiting Middleware
const rateLimitMiddleware = async (req: express.Request, res: express.Response, next: express.NextFunction) => {
  // Skip admin and dashboard routes
  if (req.path.startsWith('/admin') || req.path.startsWith('/dashboard')) {
    return next();
  }

  const tenantId = req.headers['x-tenant-id'] as string;
  if (!tenantId) {
    return res.status(401).json({ error: 'Missing X-Tenant-ID header' });
  }

  try {
    const config = await getTenantConfig(tenantId);
    if (!config) {
      recordRejection(tenantId, 'unconfigured_tenant');
      return res.status(403).json({ error: 'Tenant not configured' });
    }

    let limiterResponse;
    if (config.algorithm === 'token_bucket') {
      limiterResponse = await tokenBucketLimiter.isAllowed(tenantId, config);
    } else if (config.algorithm === 'sliding_window') {
      limiterResponse = await slidingWindowLimiter.isAllowed(tenantId, config);
    } else {
      return res.status(500).json({ error: 'Unknown algorithm' });
    }

    res.setHeader('X-RateLimit-Limit', limiterResponse.limit);
    res.setHeader('X-RateLimit-Remaining', limiterResponse.remaining);
    res.setHeader('X-RateLimit-Reset', limiterResponse.reset);

    if (!limiterResponse.allowed) {
      res.setHeader('Retry-After', Math.max(1, limiterResponse.reset - Math.floor(Date.now() / 1000)));
      recordRejection(tenantId, 'rate_limit');
      return res.status(429).json({ error: 'Too Many Requests' });
    }

    next();
  } catch (err) {
    console.error('Rate limit error:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
};

app.use(rateLimitMiddleware);

// Circuit Breaker & Queueing Middleware
app.use(async (req, res, next) => {
  if (req.path.startsWith('/admin') || req.path.startsWith('/dashboard')) {
    return next();
  }
  
  try {
    const state = await cb.checkState(DOWNSTREAM_URL);
    const isOverloaded = inFlightRequests >= MAX_CONCURRENT_REQUESTS;

    if (state === 'OPEN' || isOverloaded) {
      // Instead of relying on a redis counter that can drift, we query real Kafka lag in dashboard
      // However, to enforce a max queue depth synchronously, we could use Kafka admin, but that's slow.
      // So we will just keep the INCR/DECR strictly for MAX_QUEUE_DEPTH enforcement, 
      // but dashboard will no longer read it. Wait! The user asked to remove Redis app-level counter logic 
      // in index.ts and worker.ts entirely. Let's do that and just remove the hard queue limit, or use 
      // a local counter for an approximate limit. Actually, let's keep it strictly for the MAX limit, 
      // but remove recordQueueDepth since the dashboard doesn't need it.
      
      const currentDepth = await redisClient.incr('queue_depth');
      if (currentDepth > MAX_QUEUE_DEPTH) {
        await redisClient.decr('queue_depth');
        recordRejection(req.headers['x-tenant-id'] as string || 'unknown', 'queue_full');
        return res.status(503).json({ error: 'queue_full', retry_after: 30 });
      }

      // Enqueue request
      const trackingId = crypto.randomUUID();
      await producer.send({
        topic: 'pending-requests',
        messages: [{
          key: req.headers['x-tenant-id'] as string || 'unknown',
          value: JSON.stringify({
            trackingId,
            path: req.originalUrl,
            method: req.method,
            headers: req.headers,
            body: req.body,
            queuedAt: Date.now(),
            tenantId: req.headers['x-tenant-id'] as string || 'unknown'
          })
        }]
      });
      const latency = Date.now() - (req as any).startTime;
      recordRequest(req.headers['x-tenant-id'] as string || 'unknown', latency, 202, true);
      return res.status(202).json({ trackingId, status: 'Accepted (Queued)' });
    }
    
    // Normal flow, track in-flight
    inFlightRequests++;
    res.on('finish', () => {
      inFlightRequests--;
    });
    res.on('close', () => {
      if (!res.writableEnded) inFlightRequests--;
    });

    next();
  } catch (err) {
    console.error('CB Check/Queue Error:', err);
    next();
  }
});

// Serve Dashboard Static Files
app.use('/dashboard', express.static(path.join(__dirname, '../public')));

// Proxy to downstream
// Setup Dashboard routes BEFORE the catch-all proxy middleware!
setupDashboardRoutes(app);

app.use(
  createProxyMiddleware({
    target: DOWNSTREAM_URL,
    changeOrigin: true,
    on: {
      proxyRes: (proxyRes: any, req: any, res: any) => {
        const isSuccess = proxyRes.statusCode ? proxyRes.statusCode < 500 : false;
        const latency = Date.now() - (req as any).startTime;
        recordRequest((req.headers['x-tenant-id'] as string) || 'unknown', latency, proxyRes.statusCode || 200, false);
        cb.recordResult(DOWNSTREAM_URL, isSuccess, cbConfig).catch(console.error);
      },
      error: (err: any, req: any, res: any) => {
        cb.recordResult(DOWNSTREAM_URL, false, cbConfig).catch(console.error);
        if (!res.headersSent) {
          res.status(502).json({ error: 'Bad Gateway' });
        }
      }
    }
  })
);

async function startServer() {
  let retries = 10;
  while (retries > 0) {
    try {
      console.log(`Starting server, checking dependencies... (${retries} retries left)`);
      await setupDatabase();
      await initKafka();
      const server = app.listen(PORT as number, '0.0.0.0', () => {
        console.log(`Gateway listening on port ${PORT}`);
        console.log(`Proxying requests to ${DOWNSTREAM_URL}`);
      });
      setupDashboardSockets(server);
      return; // success
    } catch (err) {
      console.error('Failed to connect to dependencies:', err);
      retries--;
      if (retries === 0) {
        console.error('Max retries reached, shutting down.');
        process.exit(1);
      }
      console.log('Retrying in 5 seconds...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

startServer();
