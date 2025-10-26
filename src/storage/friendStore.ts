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
    `);
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
