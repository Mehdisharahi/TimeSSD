import { Pool } from 'pg';

export class PgFriendStore {
  private pool: Pool;

  constructor(databaseUrl: string) {
    this.pool = new Pool({ connectionString: databaseUrl, max: 5 });
  }

  async init() {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS co_presence (
        guild_id   TEXT NOT NULL,
        user_id    TEXT NOT NULL,
        partner_id TEXT NOT NULL,
        total_ms   BIGINT NOT NULL,
        PRIMARY KEY (guild_id, user_id, partner_id)
      );
    `);
  }

  async addDuration(guildId: string, a: string, b: string, deltaMs: number) {
    const d = Math.max(0, Math.floor(deltaMs));
    if (d <= 0) return;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO co_presence (guild_id, user_id, partner_id, total_ms)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (guild_id, user_id, partner_id)
         DO UPDATE SET total_ms = co_presence.total_ms + EXCLUDED.total_ms`,
        [guildId, a, b, d]
      );
      await client.query(
        `INSERT INTO co_presence (guild_id, user_id, partner_id, total_ms)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (guild_id, user_id, partner_id)
         DO UPDATE SET total_ms = co_presence.total_ms + EXCLUDED.total_ms`,
        [guildId, b, a, d]
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async loadGuild(guildId: string): Promise<Map<string, Map<string, number>>> {
    const res = await this.pool.query(
      `SELECT user_id, partner_id, total_ms FROM co_presence WHERE guild_id = $1`,
      [guildId]
    );
    const map: Map<string, Map<string, number>> = new Map();
    for (const row of res.rows) {
      const u = String(row.user_id);
      const p = String(row.partner_id);
      const ms = Number(row.total_ms) || 0;
      if (!map.has(u)) map.set(u, new Map());
      map.get(u)!.set(p, ms);
    }
    return map;
  }
}
