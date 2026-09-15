# Multi-Tenant API Gateway

A production-grade API gateway service in TypeScript (Node.js/Express) providing per-tenant rate limiting, Kafka-based request queuing, and circuit breaking.

## Architecture Diagram

```mermaid
flowchart TD
    Client[Client] --> Gateway[API Gateway (Node.js)]
    
    subgraph Gateway Core
        Gateway --> Limiter[Rate Limiter]
        Limiter --> |Check| Redis[(Redis)]
        Limiter --> CB[Circuit Breaker]
        CB --> |Check| Redis
    end
    
    subgraph Fallback / Backpressure
        CB -.-> |Open / Overloaded| KafkaQ[Kafka 'pending-requests']
        KafkaQ --> Worker[Consumer Worker]
        Worker --> |Retry| Downstream
    end
    
    subgraph Observability
        Gateway --> |Emit| MetricsBus((Event Bus))
        MetricsBus --> WS[WebSocket Server]
        WS --> Dashboard[Live Dashboard]
    end
    
    CB --> |Closed / Capacity OK| Downstream[Mock Downstream API]
    
    Gateway --> |Read Config| PG[(PostgreSQL)]
```

## Features
- **Rate Limiting (Redis Lua scripts)**: Token Bucket and Sliding Window Counter algorithms ensure atomic checks under concurrency. Configurations are stored in Postgres and hot-reloaded into an in-memory cache.
- **Circuit Breaker**: Distributed state machine in Redis (Closed, Open, Half-Open) to fail fast and protect system latency during downstream outages.
- **Backpressure & Queuing**: Offloads requests to a Kafka topic when concurrency limits are reached or the circuit breaker trips, returning `202 Accepted`. A separate worker drains the queue when downstream capacity recovers.
- **Live Dashboard**: A real-time WebSocket dashboard displaying RPS, rejections, queue depths, latencies (p50/p95/p99), and circuit breaker states.

## Setup Instructions

1. Start backing services:
   ```bash
   docker compose up -d
   ```
2. Install dependencies:
   ```bash
   npm install
   ```
3. Run test suite:
   ```bash
   npm test
   ```
4. Start the Gateway, Mock Downstream, and Worker:
   ```bash
   npm run start:mock
   npm run start:gateway
   npx ts-node gateway/src/worker.ts
   ```
5. View the dashboard at `http://localhost:3000/dashboard`

## Rate Limiting Algorithms

1. **Token Bucket**: Ideal for APIs where burstiness is allowed up to a predefined limit. It provides a smooth refill rate.
2. **Sliding Window Counter**: Ideal for enforcing strict request counts over rolling time windows, preventing bursts near the boundary of fixed windows. It approximates the exact sliding window log with significantly lower memory overhead.

## Load Test Benchmark Results

The gateway was load tested using `k6` across a mix of tenants with different rate limits.

**Results (Sustained 50 VUs generating traffic):**
- **p95 Latency**: 10.41ms
- **Requests / Sec**: ~382 req/s
- **False-Allows**: 0% (Atomicity maintained under high concurrency)
- **Error Rate (5xx)**: 0%

The system successfully throttled traffic for tenants exceeding their limits (returning 429s) and correctly handled requests within limits (returning 200s).
