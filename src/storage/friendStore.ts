import Database = require('better-sqlite3');

export class FriendStore {
  private db: Database.Database;

  constructor(dbPath: string) {
    this.db = new Database(dbPath);
  }

  init() {
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS co_presence (
        guild_id   TEXT NOT NULL,
        user_id    TEXT NOT NULL,
        partner_id TEXT NOT NULL,
        total_ms   INTEGER NOT NULL,
        PRIMARY KEY (guild_id, user_id, partner_id)
      );
      CREATE TABLE IF NOT EXISTS voice_activity (
        guild_id   TEXT NOT NULL,
        user_id    TEXT NOT NULL,
        total_ms   INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
      );
    `);
  }

  addVoiceDuration(guildId: string, userId: string, deltaMs: number) {
    const d = Math.max(0, Math.floor(deltaMs));
    if (d <= 0) return;
    this.db.prepare(`
      INSERT INTO voice_activity (guild_id, user_id, total_ms)
      VALUES (@g, @u, @d)
      ON CONFLICT(guild_id, user_id)
      DO UPDATE SET total_ms = total_ms + excluded.total_ms;
    `).run({ g: guildId, u: userId, d });
  }

  getVoiceDuration(guildId: string, userId: string): number {
    const row = this.db.prepare(`
      SELECT total_ms FROM voice_activity WHERE guild_id = ? AND user_id = ?
    `).get(guildId, userId) as any;
    return row ? Number(row.total_ms) : 0;
  }

  getTopVoice(guildId: string, limit: number = 20): { user_id: string, total_ms: number }[] {
    const rows = this.db.prepare(`
      SELECT user_id, total_ms FROM voice_activity 
      WHERE guild_id = ? 
      ORDER BY total_ms DESC 
      LIMIT ?
    `).all(guildId, limit) as any[];
    return rows.map(row => ({
      user_id: String(row.user_id),
      total_ms: Number(row.total_ms)
    }));
  }

  addDuration(guildId: string, a: string, b: string, deltaMs: number) {
    const upsert = this.db.prepare(`
      INSERT INTO co_presence (guild_id, user_id, partner_id, total_ms)
      VALUES (@g, @u, @p, @d)
      ON CONFLICT(guild_id, user_id, partner_id)
      DO UPDATE SET total_ms = total_ms + excluded.total_ms;
    `);
    const d = Math.max(0, Math.floor(deltaMs));
    if (d <= 0) return;
    const tx = this.db.transaction((g: string, u: string, p: string, ms: number) => {
      upsert.run({ g, u, p, d: ms });
      upsert.run({ g, u: p, p: u, d: ms });
    });
    tx(guildId, a, b, d);
  }

  loadGuild(guildId: string): Map<string, Map<string, number>> {
    const stmt = this.db.prepare(`
      SELECT user_id, partner_id, total_ms
      FROM co_presence
      WHERE guild_id = ?
    `);
    const map: Map<string, Map<string, number>> = new Map();
    for (const row of stmt.iterate(guildId) as Iterable<any>) {
      const u = String(row.user_id);
      const p = String(row.partner_id);
      const ms = Number(row.total_ms) || 0;
      if (!map.has(u)) map.set(u, new Map());
      map.get(u)!.set(p, ms);
    }
    return map;
  }
}
