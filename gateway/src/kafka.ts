import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
dotenv.config();

const brokerUrl = process.env.KAFKA_BROKER || 'localhost:9093';

// SASL/SSL is activated only when KAFKA_SASL_USERNAME is set (i.e. on Railway/Upstash).
// Local dev (no env var) uses plain TCP — no changes needed to local docker-compose setup.
const useSSL = !!process.env.KAFKA_SASL_USERNAME;

export const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: [brokerUrl],
  ...(useSSL && {
    ssl: { rejectUnauthorized: false },
    sasl: {
      mechanism: (process.env.KAFKA_SASL_MECHANISM || 'scram-sha-256') as any,
      username: process.env.KAFKA_SASL_USERNAME!,
      password: process.env.KAFKA_SASL_PASSWORD!,
    },
  }),
});

export const producer = kafka.producer();
export const consumer = kafka.consumer({ groupId: 'gateway-workers' });
export const admin = kafka.admin();

export async function initKafka() {
  await producer.connect();
  await admin.connect();
  
  const topics = await admin.listTopics();
  if (!topics.includes('pending-requests')) {
    console.log('Topic pending-requests not found. Creating it...');
    await admin.createTopics({
      topics: [{ topic: 'pending-requests', numPartitions: 1 }]
    });
  }
  
  console.log('Kafka Producer & Admin connected, topics verified.');
}
