import Redis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6380';
export const redisClient = new Redis(redisUrl);

redisClient.on('error', (err) => {
  console.error('Redis connection error:', err);
});
