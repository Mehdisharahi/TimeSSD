import 'dotenv/config';
import { Client, GatewayIntentBits, Interaction, Message, EmbedBuilder, VoiceState, Collection, AttachmentBuilder, PermissionsBitField } from 'discord.js';
import { createCanvas, loadImage, GlobalFonts } from '@napi-rs/canvas';
import fs from 'fs';
import path from 'path';
import { PgFriendStore } from './storage/pgFriendStore';
import { handleTimerInteraction, TimerManager, parseDuration, makeTimerSetEmbed } from './modules/timerManager';

const token = process.env.BOT_TOKEN;
if (!token) {
  console.error('Missing BOT_TOKEN in .env');
  process.exit(1);
}

// Ensure a font is registered so text (numbers) renders on all environments
let ssdFontAvailable = false;
let ssdFontFamily = 'Sarbaz';
try {
  const envFont = process.env.FONT_PATH && path.isAbsolute(process.env.FONT_PATH) ? process.env.FONT_PATH : null;
  const localAsset = path.join(process.cwd(), 'assets', 'fonts', 'Sarbaz.ttf');
  const candidates = [
    envFont,
    localAsset,
    'C:/Windows/Fonts/Sarbaz.ttf',
    'C:/Windows/Fonts/arial.ttf',
    'C:/Windows/Fonts/segoeui.ttf',
    '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
    '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
  ].filter(Boolean) as string[];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        GlobalFonts.registerFromPath(p, ssdFontFamily);
        ssdFontAvailable = true;
        console.log(`[canvas] Registered font from: ${p} as ${ssdFontFamily}`);
        break;
      }
    } catch {}
  }
  if (!ssdFontAvailable) {
    console.warn('[canvas] No custom font registered, will fallback to Arial');
  }
} catch {}

//

const client = new Client({ intents: [
  GatewayIntentBits.Guilds,
  GatewayIntentBits.GuildMembers,
  GatewayIntentBits.GuildMessages,
  GatewayIntentBits.MessageContent,
  GatewayIntentBits.GuildVoiceStates,
] });

export const timerManager = new TimerManager(client);

// simple per-process duplicate guard for messageCreate
const processedMessages = new Set<string>();
// additional guard to avoid double .ll replies per message
const llInFlight = new Set<string>();

// ===== Voice co-presence tracking (for .friend) =====
// channelMembers[guildId][channelId] -> Set<userId>
const channelMembers: Map<string, Map<string, Set<string>>> = new Map();
// pairStarts[guildId][pairKey] -> startEpochMs (active session per channel)
const pairStarts: Map<string, Map<string, number>> = new Map();
// partnerTotals[guildId][userId][partnerId] -> totalMs
const partnerTotals: Map<string, Map<string, Map<string, number>>> = new Map();

const loveOverrides: Map<string, Map<string, number>> = new Map();
const loveFile = path.join(process.cwd(), 'data', 'love-overrides.json');
function loveKey(a: string, b: string): string {
  return a < b ? `${a}:${b}` : `${b}:${a}`;
}
function loadLoveOverrides() {
  try {
    fs.mkdirSync(path.dirname(loveFile), { recursive: true });
    const raw = fs.existsSync(loveFile) ? fs.readFileSync(loveFile, 'utf8') : '';
    if (raw) {
      const obj = JSON.parse(raw) as Record<string, Record<string, number>>;
      loveOverrides.clear();
      for (const [g, pairs] of Object.entries(obj)) {
        const m = new Map<string, number>();
        for (const [k, v] of Object.entries(pairs)) m.set(k, v);
        loveOverrides.set(g, m);
      }
    }
  } catch {}
}
function saveLoveOverrides() {
  try {
    fs.mkdirSync(path.dirname(loveFile), { recursive: true });
    const obj: Record<string, Record<string, number>> = {};
    for (const [g, m] of loveOverrides) obj[g] = Object.fromEntries(m.entries());
    fs.writeFileSync(loveFile, JSON.stringify(obj, null, 2), 'utf8');
  } catch {}
}

function getMap<K, V>(map: Map<K, V>, key: K, mk: () => V): V {
  let v = map.get(key);
  if (!v) { v = mk(); map.set(key, v); }
  return v;
}

function pairKey(a: string, b: string, channelId: string): string {
  return (a < b ? `${a}:${b}:${channelId}` : `${b}:${a}:${channelId}`);
}

//

// Try to fetch all guild members but give up after a timeout (ms)
async function fetchMembersWithTimeout(g: any, timeoutMs: number) {
  return Promise.race([
    g.members.fetch(),
    new Promise<null>((resolve) => setTimeout(() => resolve(null), timeoutMs)),
  ]).catch(() => null);
}

// Fetch recent message authors quickly to build a candidate pool
async function recentAuthorsFallback(msg: Message, limit = 100, timeoutMs = 2000) {
  try {
    const p = (msg.channel as any).messages.fetch({ limit });
    const coll = await Promise.race([
      p,
      new Promise<null>((resolve) => setTimeout(() => resolve(null), timeoutMs)),
    ]);
    if (!coll) return [] as string[];
    const ids = new Set<string>();
    for (const m of coll.values()) {
      if (m.author?.bot) continue;
      ids.add(m.author.id);
    }
    return Array.from(ids);
  } catch {
    return [] as string[];
  }
}

type Store = {
  init: () => Promise<void>;
  addDuration: (guildId: string, a: string, b: string, deltaMs: number) => Promise<void> | void;
  loadGuild: (guildId: string) => Promise<Map<string, Map<string, number>>>;
};

let store: Store;
const pgUrl = process.env.DATABASE_URL;
if (pgUrl) {
  const pg = new PgFriendStore(pgUrl);
  store = {
    init: () => pg.init(),
    addDuration: (g, a, b, ms) => pg.addDuration(g, a, b, ms),
    loadGuild: (g) => pg.loadGuild(g),
  };
} else {
  const dbPath = process.env.FRIENDS_DB_PATH || path.join(process.cwd(), 'data', 'friends.db');
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  // Dynamic require to avoid loading better-sqlite3 when not needed
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { FriendStore } = require('./storage/friendStore');
  const sqlite = new FriendStore(dbPath);
  // Adapter to async interface
  store = {
    init: async () => { sqlite.init(); },
    addDuration: async (g, a, b, ms) => { sqlite.addDuration(g, a, b, ms); },
    loadGuild: async (g) => sqlite.loadGuild(g),
  };
}

loadLoveOverrides();

async function addDuration(guildId: string, a: string, b: string, deltaMs: number) {
  if (deltaMs <= 0) return;
  const gMap = getMap(partnerTotals, guildId, () => new Map());
  const aMap = getMap(gMap, a, () => new Map());
  const bMap = getMap(gMap, b, () => new Map());
  aMap.set(b, (aMap.get(b) || 0) + deltaMs);
  bMap.set(a, (bMap.get(a) || 0) + deltaMs);
  // persist to SQLite/Postgres
  await store.addDuration(guildId, a, b, deltaMs);
}

// Compute live totals for a user: persisted totals + ongoing sessions until now
function computeTotalsUpToNow(guildId: string, userId: string): Map<string, number> | null {
  const baseGuild = partnerTotals.get(guildId);
  const base = baseGuild?.get(userId);
  const out = new Map<string, number>();
  if (base) {
    for (const [pid, ms] of base.entries()) out.set(pid, ms);
  }
  const pMap = pairStarts.get(guildId);
  if (!pMap || pMap.size === 0) return out.size ? out : null;
  const now = Date.now();
  for (const [key, start] of pMap.entries()) {
    // key format: idA:idB:channelId
    const parts = key.split(':');
    if (parts.length < 3) continue;
    const idA = parts[0];
    const idB = parts[1];
    const other = userId === idA ? idB : (userId === idB ? idA : null);
    if (!other) continue;
    const delta = now - start;
    if (delta > 0) out.set(other, (out.get(other) || 0) + delta);
  }
  return out.size ? out : null;
}

client.once('clientReady', async () => {
  console.log(`TimeSSD is online as ${client.user?.tag}`);
  // Initialize current voice channel membership and start sessions for existing pairs
  try {
    await store.init();
    for (const g of client.guilds.cache.values()) {
      const gId = g.id;
      // Load persisted totals for this guild
      try {
        const loaded = await store.loadGuild(gId);
        if (loaded && loaded.size) {
          partnerTotals.set(gId, loaded);
        }
      } catch {}
      const chMap = getMap<string, Map<string, Set<string>>>(channelMembers, gId, () => new Map<string, Set<string>>());
      // Fetch current voice states
      let vs: Collection<string, VoiceState>;
      try {
        const full = await g.fetch();
        vs = full.voiceStates.cache;
      } catch {
        vs = g.voiceStates.cache;
      }
      vs.forEach((st) => {
        const cid = st.channelId;
        const uid = st.id as string;
        if (!cid) return;
        const set = getMap<string, Set<string>>(chMap, cid, () => new Set<string>());
        set.add(uid);
      });
      // Start sessions for all pairs currently in each channel
      const pMap = getMap(pairStarts, gId, () => new Map());
      const now = Date.now();
      for (const [cid, set] of chMap) {
        const arr = Array.from(set);
        for (let i = 0; i < arr.length; i++) {
          for (let j = i + 1; j < arr.length; j++) {
            const key = pairKey(arr[i], arr[j], cid);
            if (!pMap.has(key)) pMap.set(key, now);
          }
        }
      }
    }
  } catch {}
});

client.on('voiceStateUpdate', async (oldState: VoiceState, newState: VoiceState) => {
  const guildId = oldState.guild.id;
  const userId = oldState.id;
  const oldCid = oldState.channelId;
  const newCid = newState.channelId;
  if (oldCid === newCid) return; // ignore mute/deaf changes
  const chMap = getMap<string, Map<string, Set<string>>>(channelMembers, guildId, () => new Map<string, Set<string>>());
  const pMap = getMap<string, Map<string, number>>(pairStarts, guildId, () => new Map<string, number>());
  const now = Date.now();

  // Leaving old channel: finalize sessions with remaining members there
  if (oldCid) {
    const set = chMap.get(oldCid);
    if (set && set.has(userId)) {
      set.delete(userId);
      for (const otherId of set) {
        const key = pairKey(userId, otherId, oldCid);
        const start = pMap.get(key);
        if (start) {
          addDuration(guildId, userId, otherId, now - start);
          pMap.delete(key);
        }
      }
      if (set.size === 0) chMap.delete(oldCid);
    }
  }

  // Joining new channel: start sessions with existing members there
  if (newCid) {
    const set = getMap<string, Set<string>>(chMap, newCid, () => new Set<string>());
    for (const otherId of set) {
      const key = pairKey(userId, otherId, newCid);
      if (!pMap.has(key)) pMap.set(key, now);
    }
    set.add(userId);
  }
});

client.on('interactionCreate', async (interaction: Interaction) => {
  // Slash
  if (interaction.isChatInputCommand()) {
    if (interaction.commandName === 'timer') {
      await handleTimerInteraction(interaction, timerManager);
    }
    return;
  }
});

// Dot-prefix command: .t <duration> [reason]
client.on('messageCreate', async (msg: Message) => {
  if (!msg.inGuild()) return;
  if (msg.author.bot) return;
  if (processedMessages.has(msg.id)) return;
  processedMessages.add(msg.id);
  setTimeout(() => processedMessages.delete(msg.id), 60_000);
  const content = msg.content.trim();

  // .friend [@user|userId] — list top 10 voice partners by co-presence time
  if (content.startsWith('.friend')) {
    const arg = content.slice(7).trim();
    let target = msg.mentions.users.first() || null;
    if (!target && arg) {
      let id: string | null = null;
      const m = arg.match(/^<@!?(\d+)>$/);
      if (m) id = m[1];
      else if (/^\d+$/.test(arg)) id = arg;
      if (id) {
        try { target = await msg.client.users.fetch(id); } catch {}
      }
    }
    if (!target) target = msg.author;
    const map = computeTotalsUpToNow(msg.guildId!, target.id);
    if (!map || map.size === 0) {
      await msg.reply({ content: 'داده‌ای برای این کاربر یافت نشد.' });
      return;
    }
    const entries = Array.from(map.entries()).filter(([pid]) => pid !== target!.id);
    entries.sort((a, b) => b[1] - a[1]);
    const top = entries.slice(0, 10);
    const fmt = (ms: number) => {
      let s = Math.floor(ms / 1000);
      const h = Math.floor(s / 3600); s -= h * 3600;
      const m = Math.floor(s / 60); s -= m * 60;
      if (h > 0) return `${h}h ${m}m`;
      if (m > 0) return `${m}m ${s}s`;
      return `${s}s`;
    };
    const lines: string[] = [];
    top.forEach(([pid, ms], i) => {
      const mention = `<@${pid}>`;
      lines.push(`${i + 1}. ${mention} — ${fmt(ms)}`);
    });
    const embed = new EmbedBuilder()
      .setTitle('friends')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .av [@user|userId] — send avatar of mentioned user or the author
  if (content.startsWith('.av')) {
    const arg = content.slice(3).trim();
    let user = msg.mentions.users.first() || null;
    if (!user && arg) {
      let id: string | null = null;
      const m = arg.match(/^<@!?(\d+)>$/);
      if (m) id = m[1];
      else if (/^\d+$/.test(arg)) id = arg;
      if (id) {
        try {
          user = await msg.client.users.fetch(id);
        } catch {}
      }
    }
    if (!user) user = msg.author;
    const url = user.displayAvatarURL({ size: 1024, extension: 'png' });
    let display = user.username;
    try {
      const member = await msg.guild?.members.fetch(user.id).catch(() => null);
      display = member?.displayName ?? user.username;
    } catch {}
    const embed = new EmbedBuilder()
      .setTitle(`Avatar: ${display}`)
      .setImage(url)
      .setURL(url);
    await msg.reply({ embeds: [embed] });
    return;
  }

  if (content.startsWith('.ba')) {
    const arg = content.slice(3).trim();
    let user = msg.mentions.users.first() || null;
    if (!user && arg) {
      let id: string | null = null;
      const m = arg.match(/^<@!?(\d+)>$/);
      if (m) id = m[1];
      else if (/^\d+$/.test(arg)) id = arg;
      if (id) {
        try {
          user = await msg.client.users.fetch(id);
        } catch {}
      }
    }
    if (!user) user = msg.author;
    try { user = await user.fetch(); } catch {}
    const banner = user.bannerURL({ size: 1024, extension: 'png' });
    if (!banner) {
      await msg.reply({ content: 'این کاربر بنری تنظیم نکرده است.' });
      return;
    }
    let display = user.username;
    try {
      const member = await msg.guild?.members.fetch(user.id).catch(() => null);
      display = member?.displayName ?? user.username;
    } catch {}
    const embed = new EmbedBuilder()
      .setTitle(`Banner: ${display}`)
      .setImage(banner)
      .setURL(banner);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .llset @user1 @user2 <0-100> — admin only: set fixed love percent for a pair
  if (content.startsWith('.llset')) {
    const isAdmin = !!msg.member?.permissions.has(PermissionsBitField.Flags.Administrator);
    if (!isAdmin) {
      await msg.reply({ content: 'فقط مدیران می‌توانند از این دستور استفاده کنند.' });
      return;
    }
    const arg = content.slice(6).trim();
    const parts = arg.split(/\s+/).filter(Boolean);
    if (parts.length < 3 && msg.mentions.users.size < 2) {
      await msg.reply({ content: 'استفاده: `.llset @user1 @user2 89` یا با آیدی دو کاربر و عدد بین 0 تا 100.' });
      return;
    }
    let u1 = msg.mentions.users.at(0) || null;
    let u2 = msg.mentions.users.at(1) || null;
    const pStr = parts[parts.length - 1];
    if (!u1 || !u2) {
      const a = parts[0];
      const b = parts[1];
      if (!u1 && a && /^\d+$/.test(a)) { try { u1 = await msg.client.users.fetch(a); } catch {} }
      if (!u2 && b && /^\d+$/.test(b)) { try { u2 = await msg.client.users.fetch(b); } catch {} }
    }
    const p = Number(pStr);
    if (!u1 || !u2 || !Number.isInteger(p) || p < 0 || p > 100) {
      await msg.reply({ content: 'ورودی نامعتبر. عدد باید بین 0 تا 100 باشد و دو کاربر مشخص شوند.' });
      return;
    }
    const gId = msg.guildId!;
    const m = loveOverrides.get(gId) || new Map<string, number>();
    m.set(loveKey(u1.id, u2.id), p);
    loveOverrides.set(gId, m);
    saveLoveOverrides();
    await msg.reply({ content: `درصد عشق بین <@${u1.id}> و <@${u2.id}> روی ${p}% تنظیم شد.` });
    return;
  }

  // .llunset @user1 @user2 — admin only: remove fixed love percent
  if (content.startsWith('.llunset')) {
    const isAdmin = !!msg.member?.permissions.has(PermissionsBitField.Flags.Administrator);
    if (!isAdmin) {
      await msg.reply({ content: 'فقط مدیران می‌توانند از این دستور استفاده کنند.' });
      return;
    }
    const arg = content.slice(8).trim();
    const parts = arg.split(/\s+/).filter(Boolean);
    if (parts.length < 2 && msg.mentions.users.size < 2) {
      await msg.reply({ content: 'استفاده: `.llunset @user1 @user2` یا با آیدی دو کاربر.' });
      return;
    }
    let u1 = msg.mentions.users.at(0) || null;
    let u2 = msg.mentions.users.at(1) || null;
    if (!u1 || !u2) {
      const a = parts[0];
      const b = parts[1];
      if (!u1 && a && /^\d+$/.test(a)) { try { u1 = await msg.client.users.fetch(a); } catch {} }
      if (!u2 && b && /^\d+$/.test(b)) { try { u2 = await msg.client.users.fetch(b); } catch {} }
    }
    if (!u1 || !u2) {
      await msg.reply({ content: 'دو کاربر را مشخص کنید.' });
      return;
    }
    const gId = msg.guildId!;
    const m = loveOverrides.get(gId);
    if (m) {
      m.delete(loveKey(u1.id, u2.id));
      if (m.size === 0) loveOverrides.delete(gId); else loveOverrides.set(gId, m);
      saveLoveOverrides();
    }
    await msg.reply({ content: `تنظیم ثابت بین <@${u1.id}> و <@${u2.id}> حذف شد.` });
    return;
  }

  // .ll command
  if (content.startsWith('.ll')) {
    if (llInFlight.has(msg.id)) return;
    llInFlight.add(msg.id);
    try {
      const arg = content.slice(3).trim();
      let userA = msg.author;
      let userB = msg.mentions.users.first() || null;
      if (!userB && arg) {
        let id: string | null = null;
        const m = arg.match(/^<@!?(\d+)>$/);
        if (m) id = m[1];
        else if (/^\d+$/.test(arg)) id = arg;
        if (id) {
          try { userB = await msg.client.users.fetch(id); } catch {}
        }
      }
      if (!userB) {
        // Equal probability among ALL guild members (requires Server Members Intent on the bot)
        let all: any[] = [];
        const fetchedAll = await fetchMembersWithTimeout(msg.guild, 5000);
        if (fetchedAll) all = Array.from(fetchedAll.values());
        if (!all || all.length === 0) {
          // fallback to cache if fetch failed (still try to avoid author)
          all = msg.guild?.members.cache ? Array.from(msg.guild.members.cache.values()) : [];
        }
        if (!all || all.length === 0) {
          // final fallback: recent message authors
          const ids = await recentAuthorsFallback(msg, 100, 2000);
          if (ids.length) {
            const candidateIds = ids.filter(id => id !== userA.id);
            const id = (candidateIds.length ? candidateIds : ids)[Math.floor(Math.random() * (candidateIds.length ? candidateIds.length : ids.length))];
            try { const u = await msg.client.users.fetch(id); userB = u; } catch {}
          }
        }
        if (!userB && all && all.length > 0) {
          // Uniform random selection on full list; try to exclude author and bots if possible
          const pref = all.filter(m => !m.user.bot && m.id !== userA.id);
          const base = pref.length ? pref : all.filter(m => m.id !== userA.id);
          const pool = (base.length ? base : all);
          const pick = pool[Math.floor(Math.random() * pool.length)];
          userB = pick.user as typeof userA;
        }
        if (!userB) userB = userA; // absolute last resort
      }

      const targetB = userB ?? userA; // guarantee non-null for subsequent rendering

      const size = { w: 700, h: 250 };
      const canvas = createCanvas(size.w, size.h);
      const ctx = canvas.getContext('2d');

      // Solid background to guarantee contrast
      ctx.fillStyle = '#2f3136';
      ctx.fillRect(0, 0, size.w, size.h);

      // Load avatars
      const aUrl = userA.displayAvatarURL({ extension: 'png', size: 256 });
      const bUrl = targetB.displayAvatarURL({ extension: 'png', size: 256 });
      const [aImg, bImg] = await Promise.all([loadImage(aUrl), loadImage(bUrl)]);

      // Draw square avatars (no border), flush to left/right edges
      const box = size.h; // full height square
      const y = 0;
      const leftX = 0;
      const rightX = size.w - box;
      ctx.drawImage(aImg, leftX, y, box, box);
      ctx.drawImage(bImg, rightX, y, box, box);

      // Heart and percentage (centered)
      let love = Math.floor(Math.random() * 101);
      try {
        const m = loveOverrides.get(msg.guildId!);
        const v = m?.get(loveKey(userA.id, targetB.id));
        if (typeof v === 'number') love = v;
      } catch {}
      console.log(`[.ll] rendering with love=%s, fontAvailable=%s`, love, ssdFontAvailable);
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      const cx = Math.floor(size.w / 2);
      const cy = Math.floor(size.h / 2);

      // Draw a glossy heart path with gradient
      const heartW = 230;
      const heartH = 210;
      const hw = heartW / 2;
      const hh = heartH / 2;
      ctx.save();
      ctx.translate(cx, cy);
      ctx.beginPath();
      // Parametric heart using Bezier curves
      ctx.moveTo(0, hh * 0.2);
      ctx.bezierCurveTo(hw, -hh * 0.6, hw * 1.2, hh * 0.8, 0, hh);
      ctx.bezierCurveTo(-hw * 1.2, hh * 0.8, -hw, -hh * 0.6, 0, hh * 0.2);
      const grad = ctx.createRadialGradient(-hw * 0.2, -hh * 0.4, hw * 0.1, 0, 0, Math.max(hw, hh));
      grad.addColorStop(0, '#ff88a3');
      grad.addColorStop(0.4, '#ff5e7a');
      grad.addColorStop(1, '#d61e41');
      ctx.fillStyle = grad;
      ctx.fill();

      // subtle highlight
      ctx.globalAlpha = 0.25;
      ctx.beginPath();
      ctx.ellipse(-hw * 0.25, -hh * 0.35, hw * 0.45, hh * 0.25, 0, 0, Math.PI * 2);
      ctx.fillStyle = '#ffffff';
      ctx.fill();
      ctx.globalAlpha = 1;
      ctx.restore();

      // Percentage text inside heart (outlined for visibility)
      ctx.font = ssdFontAvailable ? `bold 40px "${ssdFontFamily}"` : 'bold 40px Arial';
      ctx.lineWidth = 6;
      ctx.strokeStyle = 'rgba(0,0,0,0.7)';
      ctx.strokeText(`${love}%`, cx, cy);
      ctx.fillStyle = '#ffffff';
      ctx.fillText(`${love}%`, cx, cy);

      // Names
      const aMember = await msg.guild?.members.fetch(userA.id).catch(() => null);
      const bMember = await msg.guild?.members.fetch(targetB.id).catch(() => null);
      const aName = aMember?.displayName ?? userA.username;
      const bName = bMember?.displayName ?? targetB.username;
      ctx.fillStyle = '#ffffff';
      ctx.font = 'bold 20px sans-serif';
      ctx.fillText(aName, leftX + box / 2, box + 18);
      ctx.fillText(bName, rightX + box / 2, box + 18);

      const buffer = canvas.toBuffer('image/png');
      const attachment = new AttachmentBuilder(buffer, { name: 'love.png' });
      await msg.reply({ files: [attachment] });
      return;
    } catch (err) {
      console.error('Error in .ll command:', err);
      await msg.reply({ content: 'خطا در ساخت تصویر عشق. لطفاً کمی بعد دوباره تلاش کنید.' });
      return;
    } finally {
      llInFlight.delete(msg.id);
    }
  }

  // .e command
  if (content.startsWith('.e')) {
    const arg = content.slice(2).trim();
    if (!arg || !/^\d+$/.test(arg)) {
      await msg.reply({ content: 'استفاده: `.e 30` (افزودن ثانیه به آخرین تایمر شما)' });
      return;
    }
    const sec = Number(arg);
    if (sec <= 0) {
      await msg.reply({ content: 'عدد معتبر وارد کنید (بزرگتر از 0).' });
      return;
    }
    const t = await timerManager.extendLast(msg.guildId!, msg.author.id, sec * 1000);
    if (!t) {
      await msg.reply({ content: 'تایمر فعالی برای شما یافت نشد.' });
      return;
    }
    return;
  }

  if (!content.startsWith('.t')) return;

  const args = content.slice(2).trim();
  if (!args) {
    await msg.reply({ content: 'استفاده: `.t 10m [دلیل]` یا `.t 60 [دلیل]` (عدد = ثانیه)' });
    return;
  }

  const [first, ...rest] = args.split(/\s+/);
  const reason = rest.join(' ').trim() || null;
  const durationMs = parseDuration(first);
  if (!durationMs || durationMs < 1000) {
    await msg.reply({ content: 'مدت زمان نامعتبر. نمونه: 10m یا 2h یا 60 (ثانیه)' });
    return;
  }

  const at = timerManager.setTimer({
    guildId: msg.guildId!,
    channelId: msg.channel.id,
    userId: msg.author.id,
    durationMs,
    reason,
  });

  const embed = makeTimerSetEmbed(at);
  const sent = await msg.reply({ embeds: [embed] });
  at.messageId = sent.id;
});

client.login(token);
