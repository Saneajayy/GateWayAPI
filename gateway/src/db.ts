import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

// Railway injects DATABASE_URL automatically when Postgres plugin is added.
// Fall back to individual env vars for local dev.
export const pgPool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
  : new Pool({
      user: process.env.POSTGRES_USER || 'admin',
      password: process.env.POSTGRES_PASSWORD || 'password',
      host: process.env.POSTGRES_HOST || 'localhost',
      port: parseInt(process.env.POSTGRES_PORT || '5432'),
      database: process.env.POSTGRES_DB || 'gateway_db',
    });


export async function setupDatabase() {
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS tenants (
      id VARCHAR(255) PRIMARY KEY,
      algorithm VARCHAR(50) NOT NULL,
      rate_limit INT NOT NULL,
      rate_or_window INT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
}
