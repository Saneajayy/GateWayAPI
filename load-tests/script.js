import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('error_rate_5xx');

export const options = {
  stages: [
    { duration: '5s', target: 50 }, // ramp up
    { duration: '15s', target: 50 }, // sustained load
    { duration: '5s', target: 0 },  // ramp down
  ],
  thresholds: {
    // 95% of requests must complete within 100ms
    http_req_duration: ['p(95)<100'],
    // 5xx error rate should be 0
    error_rate_5xx: ['rate==0'],
  },
};

export default function () {
  const tenants = ['tenantA', 'tenantB', 'tenantC'];
  const tenant = tenants[Math.floor(Math.random() * tenants.length)];
  
  const baseUrl = __ENV.API_URL || 'http://host.docker.internal:3000';
  const res = http.get(`${baseUrl}/api/test`, {
    headers: { 'X-Tenant-ID': tenant },
  });
  
  const is5xx = res.status >= 500;
  errorRate.add(is5xx);

  check(res, {
    'is status 200, 202, or 429': (r) => r.status === 200 || r.status === 429 || r.status === 202,
    'no 5xx errors': (r) => r.status < 500,
  });
  
  sleep(0.1);
}
