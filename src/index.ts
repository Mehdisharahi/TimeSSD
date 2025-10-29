import 'dotenv/config';
import { Client, GatewayIntentBits, Interaction, Message, EmbedBuilder, VoiceState, Collection, AttachmentBuilder, PermissionsBitField, ActionRowBuilder, ButtonBuilder, ButtonStyle, GuildMember } from 'discord.js';
import { createCanvas, loadImage, GlobalFonts } from '@napi-rs/canvas';
import fs from 'fs';
import path from 'path';
import { PgFriendStore } from './storage/pgFriendStore';
import { handleTimerInteraction, TimerManager, parseDuration, makeTimerSetEmbed } from './modules/timerManager';

const token = process.env.BOT_TOKEN;

// ===== Hokm Phase 1 state =====
type Suit = 'S' | 'H' | 'D' | 'C';
const SUIT_EMOJI: Record<Suit, string> = { S: '♠️', H: '♥️', D: '♦️', C: '♣️' };
const EMOJI_TO_SUIT: Record<string, Suit> = {
  '♠': 'S','♠️': 'S',':spades:': 'S','🂡': 'S',
  '♥': 'H','♥️': 'H',':hearts:': 'H',
  '♦': 'D','♦️': 'D',':diamonds:': 'D',
  '♣': 'C','♣️': 'C',':clubs:': 'C',
  'پیک': 'S','دل': 'H','خشت': 'D','گیشنیز': 'C','گشنیز': 'C'
};
const RANKS = [2,3,4,5,6,7,8,9,10,11,12,13,14]; // 11:J 12:Q 13:K 14:A
interface Card { s: Suit; r: number }
interface HokmSession {
  channelId: string;
  guildId: string;
  ownerId?: string;
  team1: string[]; // userIds
  team2: string[];
  order: string[]; // play order: [t1[0], t2[0], t1[1], t2[1]]
  hakim?: string; // userId
  hokm?: Suit;
  targetTricks?: number; // 1..7, default 7
  deck: Card[];
  hands: Map<string, Card[]>; // userId -> 0..13
  state: 'waiting'|'choosing_hokm'|'playing'|'finished';
  controlMsgId?: string; // message with join buttons
  tableMsgId?: string; // live table embed message id
  playerDMMsgIds?: Map<string, string>; // userId -> DM message id
  // Phase 2
  leaderIndex?: number; // index into order for current trick leader
  turnIndex?: number; // index into order whose turn it is now
  table?: { userId: string; card: Card }[];
  leadSuit?: Suit | null;
  tricksTeam1?: number;
  tricksTeam2?: number;
}
const hokmSessions = new Map<string, HokmSession>(); // key: guildId:channelId
function keyGC(g: string, c: string){ return `${g}:${c}`; }
function makeDeck(): Card[] { const d: Card[] = []; (['S','H','D','C'] as Suit[]).forEach(s=>RANKS.forEach(r=>d.push({s, r}))); return d; }
function shuffle<T>(a: T[]): T[] { for(let i=a.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1)); [a[i],a[j]]=[a[j],a[i]];} return a; }
function rankStr(r:number){ if(r===14) return 'A'; if(r===13) return 'K'; if(r===12) return 'Q'; if(r===11) return 'J'; return String(r); }
function cardStr(c: Card){ return `${rankStr(c.r)}${SUIT_EMOJI[c.s]}`; }
function parseCardToken(tok: string): Card | null {
  const t = tok.trim().toLowerCase();
  // suit detection
  let s: Suit | null = null;
  if (t.includes('♠') || t.includes(':spades:') || t.endsWith('s')) s = 'S';
  else if (t.includes('♥') || t.includes(':hearts:') || t.endsWith('h')) s = 'H';
  else if (t.includes('♦') || t.includes(':diamonds:') || t.endsWith('d')) s = 'D';
  else if (t.includes('♣') || t.includes(':clubs:') || t.endsWith('c')) s = 'C';
  if (!s) return null;
  // rank
  const rt = t.replace(/[^a-z0-9]/g,'');
  let rStr = rt;
  // allow forms like A, K, Q, J, 10,9..2 possibly followed by suit letter which we removed
  if (rStr.endsWith('s')||rStr.endsWith('h')||rStr.endsWith('d')||rStr.endsWith('c')) rStr = rStr.slice(0,-1);
  let r: number | null = null;
  if (rStr === 'a') r = 14;
  else if (rStr === 'k') r = 13;
  else if (rStr === 'q') r = 12;
  else if (rStr === 'j') r = 11;
  else if (/^\d+$/.test(rStr)) { const n = parseInt(rStr,10); if (n>=2 && n<=10) r = n; }
  if (!r) return null;
  return { s, r };
}
function sameCard(a: Card, b: Card){ return a.s===b.s && a.r===b.r; }

// ====== UI helpers for interactive Hokm ======
function sortHand(hand: Card[]): Card[] { return [...hand].sort((a,b)=> a.s===b.s ? b.r-a.r : ['S','H','D','C'].indexOf(a.s)-['S','H','D','C'].indexOf(b.s)); }
function suitName(s: Suit){ return s==='S'?'♠️ پیک':s==='H'?'♥️ دل':s==='D'?'♦️ خشت':'♣️ گیشنیز'; }

function buildHandButtons(s: HokmSession, userId: string, opts?: { filter?: Suit|'ALL'; page?: number }): { rows: ActionRowBuilder<ButtonBuilder>[]; meta: { filter: string; page: number; totalPages: number } } {
  const filter = (opts?.filter ?? 'ALL') as Suit|'ALL';
  const page = opts?.page ?? 0;
  const hand = sortHand(s.hands.get(userId) || []);
  const filtered = filter==='ALL' ? hand : hand.filter(c=>c.s===filter);
  const perPage = 10; // 2 rows of 5 buttons
  const totalPages = Math.max(1, Math.ceil(filtered.length / perPage));
  const start = Math.min(page, totalPages-1) * perPage;
  const items = filtered.slice(start, start + perPage);
  const rows: ActionRowBuilder<ButtonBuilder>[] = [];
  // card buttons (max 2 rows)
  for (let r=0; r<2; r++) {
    const slice = items.slice(r*5, r*5+5);
    if (!slice.length) break;
    const row = new ActionRowBuilder<ButtonBuilder>();
    for (const c of slice) {
      row.addComponents(new ButtonBuilder().setCustomId(`hokm-play-${userId}-${c.s}-${c.r}`).setLabel(cardStr(c)).setStyle(ButtonStyle.Secondary));
    }
    rows.push(row);
  }
  // filter row
  const rowFilter = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${userId}-ALL`).setLabel('همه').setStyle(filter==='ALL'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${userId}-S`).setLabel('♠️').setStyle(filter==='S'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${userId}-H`).setLabel('♥️').setStyle(filter==='H'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${userId}-D`).setLabel('♦️').setStyle(filter==='D'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${userId}-C`).setLabel('♣️').setStyle(filter==='C'?ButtonStyle.Primary:ButtonStyle.Secondary),
  );
  rows.push(rowFilter);
  // pagination row (if needed)
  if (totalPages > 1) {
    const rowPage = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId(`hokm-hand-page-${userId}-${Math.max(0, page-1)}`).setLabel('قبلی').setStyle(ButtonStyle.Secondary).setDisabled(page<=0),
      new ButtonBuilder().setCustomId(`hokm-hand-page-${userId}-${Math.min(totalPages-1, page+1)}`).setLabel('بعدی').setStyle(ButtonStyle.Secondary).setDisabled(page>=totalPages-1),
    );
    rows.push(rowPage);
  }
  return { rows, meta: { filter, page, totalPages } };
}

async function refreshPlayerDM(ctx: { client: Client }, s: HokmSession, userId: string) {
  try {
    const user = await ctx.client.users.fetch(userId);
    const dm = await user.createDM(true);
    const stateKey = `__hokm_dm_state_${s.guildId}:${s.channelId}:${userId}` as any;
    const prev = (global as any)[stateKey] as { filter?: string; page?: number } | undefined;
    const filter = (prev?.filter as any) || 'ALL';
    const page = prev?.page || 0;
    const { rows, meta } = buildHandButtons(s, userId, { filter: filter as any, page });
    (global as any)[stateKey] = { filter: meta.filter, page: meta.page };
    const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${userId===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}\nدست شما:\n${handToString(s.hands.get(userId) || [])}`;
    const msgId = s.playerDMMsgIds?.get(userId);
    if (msgId) {
      const m = await dm.messages.fetch(msgId).catch(()=>null);
      if (m) { await m.edit({ content, components: rows }); return; }
    }
    const sent = await dm.send({ content, components: rows });
    s.playerDMMsgIds = s.playerDMMsgIds || new Map<string,string>();
    s.playerDMMsgIds.set(userId, sent.id);
  } catch {}
}

async function refreshAllDMs(ctx: { client: Client }, s: HokmSession) {
  for (const uid of s.order) await refreshPlayerDM(ctx, s, uid);
}

async function renderTableCanvas(s: HokmSession): Promise<Buffer> {
  const width = 900, height = 500;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');
  // table bg
  ctx.fillStyle = '#0a6a3b'; ctx.fillRect(0,0,width,height);
  // header
  ctx.fillStyle = '#ffffff'; ctx.font = '28px Sans';
  const hokmText = s.hokm ? `حکم: ${SUIT_EMOJI[s.hokm]}` : 'حکم: —';
  const turnUser = s.turnIndex!=null ? s.order[s.turnIndex] : undefined;
  ctx.fillText(`${hokmText} | نوبت: ${turnUser?`@${turnUser}`:'—'}`, 20, 40);
  ctx.fillText(`دست‌ها — تیم1: ${s.tricksTeam1??0} | تیم2: ${s.tricksTeam2??0}`, 20, 75);
  // draw 4 slots around center
  const cx = width/2, cy = height/2; const cardW=120, cardH=170;
  const positions = [ {x: cx-cardW/2, y: cy-cardH-30}, {x: cx+cardW+30, y: cy-cardH/2}, {x: cx-cardW/2, y: cy+30}, {x: cx-cardW-30, y: cy-cardH/2} ];
  // determine order relative to leader
  const table = s.table || [];
  for (let i=0;i<table.length;i++) {
    const p = table[i];
    const pos = positions[i] || positions[0];
    ctx.fillStyle = '#ffffff'; ctx.strokeStyle = '#222'; ctx.lineWidth = 3;
    ctx.fillRect(pos.x, pos.y, cardW, cardH); ctx.strokeRect(pos.x, pos.y, cardW, cardH);
    ctx.fillStyle = '#111'; ctx.font = 'bold 36px Sans';
    ctx.fillText(rankStr(p.card.r), pos.x+10, pos.y+50);
    ctx.fillText(SUIT_EMOJI[p.card.s], pos.x+10, pos.y+100);
    ctx.font = '14px Sans'; ctx.fillStyle = '#333';
    ctx.fillText(`<@${p.userId}>`, pos.x+10, pos.y+cardH-10);
  }
  return canvas.toBuffer('image/png');
}

async function refreshTableEmbed(ctx: { channel: any }, s: HokmSession) {
  const buf = await renderTableCanvas(s);
  const att = new AttachmentBuilder(buf, { name: 'table.png' });
  const desc = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:'—'} — نوبت: ${s.turnIndex!=null?`<@${s.order[s.turnIndex]}>`:'—'}\nتیم1 دست‌ها: ${s.tricksTeam1??0} | تیم2 دست‌ها: ${s.tricksTeam2??0}`;
  const embed = new EmbedBuilder().setTitle('Hokm — میز بازی').setDescription(desc).setColor(0x2f3136).setImage('attachment://table.png');
  if (s.tableMsgId) {
    const m = await ctx.channel.messages.fetch(s.tableMsgId).catch(()=>null);
    if (m) { await m.edit({ embeds: [embed], files: [att] }); return; }
  }
  const sent = await ctx.channel.send({ embeds: [embed], files: [att] });
  s.tableMsgId = sent.id;
}

async function resolveTrickAndContinue(interaction: Interaction, s: HokmSession) {
  // determine winner with same logic as text command
  const lead = s.leadSuit!; const trump = s.hokm!;
  let winnerIdxInTrick = 0; let winnerCard = s.table![0].card;
  for (let i=1;i<4;i++) {
    const c = s.table![i].card;
    const isWinnerTrump = winnerCard.s===trump; const isCurrentTrump = c.s===trump;
    if (isCurrentTrump && !isWinnerTrump) { winnerIdxInTrick = i; winnerCard = c; continue; }
    if (isCurrentTrump && isWinnerTrump) { if (c.r>winnerCard.r) { winnerIdxInTrick=i; winnerCard=c; } continue; }
    if (!isWinnerTrump && !isCurrentTrump) {
      const winnerIsLead = winnerCard.s===lead; const currentIsLead = c.s===lead;
      if (currentIsLead && !winnerIsLead) { winnerIdxInTrick=i; winnerCard=c; continue; }
      if (currentIsLead && winnerIsLead && c.r>winnerCard.r) { winnerIdxInTrick=i; winnerCard=c; continue; }
    }
  }
  const trickStartIndex = s.leaderIndex!;
  const winnerTurnIndex = (trickStartIndex + winnerIdxInTrick) % 4;
  const winnerUserId = s.order[winnerTurnIndex];
  const team = s.team1.includes(winnerUserId) ? 't1' : 't2';
  if (team==='t1') s.tricksTeam1 = (s.tricksTeam1||0)+1; else s.tricksTeam2 = (s.tricksTeam2||0)+1;
  // next trick
  s.leaderIndex = winnerTurnIndex; s.turnIndex = winnerTurnIndex; s.table = []; s.leadSuit = null;
  const target = s.targetTricks ?? 7;
  if ((s.tricksTeam1||0) >= target || (s.tricksTeam2||0) >= target) {
    s.state = 'finished';
    await refreshTableEmbed({ channel: interaction.channel }, s);
    await (interaction.channel as any)?.send({ content: `بازی تمام شد! تیم ${(s.tricksTeam1||0)>=target?1:2} برنده شد. نتیجه — تیم1: ${s.tricksTeam1} | تیم2: ${s.tricksTeam2}` });
    return;
  }
  await refreshTableEmbed({ channel: interaction.channel }, s);
  await refreshAllDMs({ client: (interaction.client as Client) }, s);
  await (interaction.channel as any)?.send({ content: `این دست را برد: <@${winnerUserId}> (تیم ${team==='t1'?1:2}). نوبت شروع: <@${s.order[s.leaderIndex!]}>` });
}

function handToString(hand: Card[]){ const bySuit: Record<Suit, Card[]> = {S:[],H:[],D:[],C:[]}; hand.forEach(c=>bySuit[c.s].push(c)); (Object.keys(bySuit) as Suit[]).forEach(s=>bySuit[s].sort((a,b)=>b.r-a.r));
  const parts: string[] = [];
  (['S','H','D','C'] as Suit[]).forEach(s=>{ if(bySuit[s].length){ parts.push(`${SUIT_EMOJI[s]} ${bySuit[s].map(cardStr).join(' ')}`); }});
  return parts.join('\n');
}
function parseSuit(input: string): Suit | null {
  const t = input.trim().toLowerCase();
  for (const [k,v] of Object.entries(EMOJI_TO_SUIT)) { if (t.includes(k)) return v; }
  if (t==='s' || t==='spade' || t==='spades') return 'S';
  if (t==='h' || t==='heart' || t==='hearts') return 'H';
  if (t==='d' || t==='diamond' || t==='diamonds') return 'D';
  if (t==='c' || t==='club' || t==='clubs') return 'C';
  return null;
}
function ensureSession(gId: string, cId: string): HokmSession {
  const k = keyGC(gId, cId);
  let s = hokmSessions.get(k);
  if (!s) {
    s = { guildId: gId, channelId: cId, team1: [], team2: [], order: [], deck: [], hands: new Map(), state: 'waiting' };
    hokmSessions.set(k, s);
  }
  return s;
}

async function resolveTargetIds(msg: Message, raw: string, cmd: string): Promise<string[]> {
  const ids = new Set<string>();
  for (const u of msg.mentions.users.values()) ids.add(u.id);
  const ref = await msg.fetchReference().catch(()=>null);
  if (ref?.author?.id) ids.add(ref.author.id);
  const rest = raw.replace(cmd, '').trim();
  if (rest) {
    for (const tk of rest.split(/\s+/).filter(Boolean)) {
      if (/^\d+$/.test(tk)) ids.add(tk);
    }

  }
  return Array.from(ids);
}
if (!token) {
  console.error('Missing BOT_TOKEN in .env');
  process.exit(1);
}

// Sticky random love values per guild so results stay consistent unless overridden
const loveRandoms: Map<string, Map<string, number>> = new Map();
const loveRandomFile = path.join(process.cwd(), 'data', 'love-randoms.json');
function loadLoveRandoms() {
  try {
    fs.mkdirSync(path.dirname(loveRandomFile), { recursive: true });
    const raw = fs.existsSync(loveRandomFile) ? fs.readFileSync(loveRandomFile, 'utf8') : '';
    if (raw) {
      const obj = JSON.parse(raw) as Record<string, Record<string, number>>;
      loveRandoms.clear();
      for (const [g, pairs] of Object.entries(obj)) {
        const m = new Map<string, number>();
        for (const [k, v] of Object.entries(pairs)) m.set(k, v);
        loveRandoms.set(g, m);
      }
    }
  } catch {}
}
function saveLoveRandoms() {
  try {
    fs.mkdirSync(path.dirname(loveRandomFile), { recursive: true });
    const obj: Record<string, Record<string, number>> = {};
    for (const [g, m] of loveRandoms) obj[g] = Object.fromEntries(m.entries());
    fs.writeFileSync(loveRandomFile, JSON.stringify(obj, null, 2), 'utf8');
  } catch {}
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
loadLoveRandoms();

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

// Minimal HTTPS downloader for environments without global fetch
async function fetchBuffer(url: string): Promise<Buffer> {
  return await new Promise<Buffer>((resolve, reject) => {
    try {
      const https = require('https');
      const req = https.get(url, (res: any) => {
        if (res.statusCode !== 200) {
          const code = res.statusCode;
          res.resume();
          return reject(new Error(`HTTP ${code}`));
        }
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => resolve(Buffer.concat(chunks)));
      });
      req.on('error', (err: any) => reject(err));
    } catch (e) {
      reject(e);
    }
  });
}

client.once('ready', async () => {
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
  // Hokm buttons
  if (interaction.isButton()) {
    const id = interaction.customId;
    // Join/Leave
    if (id === 'hokm-join-t1' || id === 'hokm-join-t2' || id === 'hokm-leave') {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      const uid = interaction.user.id;
      // Remove from both teams first
      s.team1 = s.team1.filter(x=>x!==uid);
      s.team2 = s.team2.filter(x=>x!==uid);
      if (id === 'hokm-leave') {
        s.team1 = s.team1.filter(x=>x!==uid);
        s.team2 = s.team2.filter(x=>x!==uid);
        await interaction.reply({ content: 'از اتاق خارج شدی.', ephemeral: true });
      } else {
        const target = id === 'hokm-join-t1' ? s.team1 : s.team2;
      if (target.length >= 2) { await interaction.reply({ content: 'این تیم پر است.', ephemeral: true }); return; }
      target.push(uid);
        await interaction.reply({ content: `به تیم ${id.endsWith('t1')? '1':'2'} پیوستی.`, ephemeral: true });
      }
      // Update control message embed
      const embed = new EmbedBuilder().setTitle('Hokm — اتاق فعال')
        .setDescription(`تیم 1: ${s.team1.map(u=>`<@${u}>`).join(' , ') || '—'}\nتیم 2: ${s.team2.map(u=>`<@${u}>`).join(' , ') || '—'}`)
        .setColor(0x2f3136);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId('hokm-leave').setLabel('خروج').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('hokm-start').setLabel('شروع بازی').setStyle(ButtonStyle.Danger),
      );
      try {
        if (s.controlMsgId) {
          const m = await (interaction.channel as any).messages.fetch(s.controlMsgId).catch(()=>null);
          if (m) await m.edit({ embeds: [embed], components: [row] });
        }
      } catch {}
      return;
    }

    // Start game button (owner only, default target 7)
    if (id === 'hokm-start') {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (!s.ownerId || interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'فقط سازنده اتاق می‌تواند شروع کند.', ephemeral: true }); return; }
      if (s.state !== 'waiting') { await interaction.reply({ content: 'اتاق در وضعیت شروع نیست.', ephemeral: true }); return; }
      if (s.team1.length !== 2 || s.team2.length !== 2) { await interaction.reply({ content: 'هر دو تیم باید ۲ نفر داشته باشند.', ephemeral: true }); return; }
      s.targetTricks = s.targetTricks ?? 7;
      s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
      s.hakim = s.team1[0];
      s.deck = shuffle(makeDeck());
      s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
      const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
      give(s.hakim, 5);
      s.state = 'choosing_hokm';
      try { const user = await interaction.client.users.fetch(s.hakim); await user.send({ content: `دست اولیه شما (۵ کارت):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
      // create or update table message with suit buttons
      const embed = new EmbedBuilder().setTitle('Hokm — انتخاب حکم')
        .setDescription(`تیم 1: ${s.team1.map(u=>`<@${u}>`).join(' , ')}\nتیم 2: ${s.team2.map(u=>`<@${u}>`).join(' , ')}\nحاکم: <@${s.hakim}> — لطفاً حکم را انتخاب کن.`)
        .setColor(0x5865F2);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId('hokm-choose-S').setLabel('♠️ پیک').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId('hokm-choose-H').setLabel('♥️ دل').setStyle(ButtonStyle.Danger),
        new ButtonBuilder().setCustomId('hokm-choose-D').setLabel('♦️ خشت').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('hokm-choose-C').setLabel('♣️ گیشنیز').setStyle(ButtonStyle.Success),
      );
      let msgObj = null as any;
      try {
        if (s.tableMsgId) {
          const m = await (interaction.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
          if (m) { await m.edit({ embeds: [embed], components: [row] }); msgObj = m; }
        }
      } catch {}
      if (!msgObj) {
        msgObj = await (interaction.channel as any).send({ embeds: [embed], components: [row] });
        s.tableMsgId = msgObj.id;
      }
      await interaction.reply({ content: 'بازی با موفقیت شروع شد. منتظر انتخاب حکم از حاکم باشید.', ephemeral: true });
      return;
    }

    // Suit choice buttons
    if (id.startsWith('hokm-choose-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (s.state !== 'choosing_hokm' || !s.hakim) { await interaction.reply({ content: 'الان وقت انتخاب حکم نیست.', ephemeral: true }); return; }
      if (interaction.user.id !== s.hakim) { await interaction.reply({ content: 'فقط حاکم می‌تواند حکم را انتخاب کند.', ephemeral: true }); return; }
      const suitKey = id.split('hokm-choose-')[1] as Suit;
      const suit: Suit | undefined = (['S','H','D','C'] as Suit[]).find(x=>x===suitKey);
      if (!suit) { await interaction.reply({ content: 'خال نامعتبر.', ephemeral: true }); return; }
      s.hokm = suit;
      // deal remaining to all to reach 13
      const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
      for (const uid of s.order) {
        const need = 13 - (s.hands.get(uid)?.length || 0);
        give(uid, need);
      }
      // init phase2
      s.state = 'playing';
      s.leaderIndex = s.order.indexOf(s.hakim); if (s.leaderIndex<0) s.leaderIndex=0;
      s.turnIndex = s.leaderIndex; s.table = []; s.leadSuit = null; s.tricksTeam1 = 0; s.tricksTeam2 = 0;
      // update table message
      const tableEmbed = new EmbedBuilder().setTitle('Hokm — میز بازی')
        .setDescription(`حکم: ${SUIT_EMOJI[s.hokm]} — نوبت: <@${s.order[s.turnIndex]}>\nتیم1 دست‌ها: 0 | تیم2 دست‌ها: 0`)
      try { if (s.tableMsgId) { const m = await (interaction.channel as any).messages.fetch(s.tableMsgId).catch(()=>null); if (m) await m.edit({ embeds: [tableEmbed], components: [] }); } } catch {}
      // send DM hands with card buttons
      s.playerDMMsgIds = s.playerDMMsgIds || new Map<string,string>();
      for (const uid of s.order) {
        await refreshPlayerDM({ client: interaction.client as Client }, s, uid);
      }
      await interaction.reply({ content: `حکم انتخاب شد: ${SUIT_EMOJI[s.hokm]}. بازی شروع شد.`, ephemeral: true });
      return;
    }

    // ...

    // Suit choice buttons
    if (id.startsWith('hokm-choose-')) {
      // ...
    }

    // Play card button: hokm-play-<uid>-<S|H|D|C>-<rank>
    if (id.startsWith('hokm-play-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (s.state !== 'playing' || s.turnIndex==null) { await interaction.reply({ content: 'بازی در جریان نیست.', ephemeral: true }); return; }
      const parts = id.split('-');
      const uid = parts[2]; const suit = parts[3] as Suit; const rank = parseInt(parts[4], 10);
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'این دکمه برای دست شما نیست.', ephemeral: true }); return; }
      if (s.order[s.turnIndex] !== uid) { await interaction.reply({ content: 'الان نوبت شما نیست.', ephemeral: true }); return; }
      const hand = s.hands.get(uid) || [];
      const card: Card = { s: suit, r: rank };
      const idx = hand.findIndex(c=>sameCard(c, card));
      if (idx === -1) { await interaction.reply({ content: 'این کارت در دست شما نیست.', ephemeral: true }); return; }
      // follow-suit
      if (!s.table || s.table.length === 0) {
        s.leadSuit = card.s;
      } else {
        const lead = s.leadSuit!;
        const hasLead = hand.some(c=>c.s===lead);
        if (hasLead && card.s !== lead) { await interaction.reply({ content: `باید خال شروع (${SUIT_EMOJI[lead]}) را دنبال کنید.`, ephemeral: true }); return; }
      }
      // play
      hand.splice(idx,1); s.hands.set(uid, hand);
      s.table = s.table || []; s.table.push({ userId: uid, card });
      s.turnIndex = (s.turnIndex + 1) % s.order.length;
      await interaction.reply({ content: `کارت ${cardStr(card)} بازی شد.`, ephemeral: true });
      // update player DM message for this user
      await refreshPlayerDM(interaction, s, uid);
      // update table embed in channel
      await refreshTableEmbed({ channel: interaction.channel }, s);
      // check trick resolve
      if (s.table.length === 4) {
        await resolveTrickAndContinue(interaction, s);
      }
      return;
    }
  }
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

  // .friend [@user|userId]
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
    const rawEntries = Array.from(map.entries()).filter(([pid]) => pid !== target!.id);
    const entries: Array<[string, number]> = [];
    for (const [pid, ms] of rawEntries) {
      try {
        const member = await msg.guild?.members.fetch(pid).catch(() => null);
        if (member && !member.user.bot) entries.push([pid, ms]);
      } catch {}
    }
    if (entries.length === 0) {
      await msg.reply({ content: 'هیچ دوست غیر باتی پیدا نشد.' });
      return;
    }
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

  // .topfriend — list top 10 pairs with most co-voice time (exclude bots)
  if (content.startsWith('.topfriend')) {
    const gId = msg.guildId!;

    // Aggregate persisted totals per unordered pair (a<b)
    const agg = new Map<string, { a: string; b: string; ms: number }>();
    const baseGuild = partnerTotals.get(gId);
    if (baseGuild) {
      for (const [a, mp] of baseGuild) {
        for (const [b, ms] of mp) {
          const [x, y] = a < b ? [a, b] : [b, a];
          const key = `${x}:${y}`;
          const cur = agg.get(key) || { a: x, b: y, ms: 0 };
          cur.ms += ms;
          agg.set(key, cur);
        }
      }
    }

    // Add ongoing sessions from pairStarts (per channel) up to now
    const pMap = pairStarts.get(gId);
    if (pMap && pMap.size) {
      const now = Date.now();
      for (const [key, start] of pMap) {
        const parts = key.split(':');
        if (parts.length < 3) continue;
        const [a, b] = [parts[0], parts[1]];
        const [x, y] = a < b ? [a, b] : [b, a];
        const k2 = `${x}:${y}`;
        const cur = agg.get(k2) || { a: x, b: y, ms: 0 };
        const delta = Math.max(0, now - start);
        cur.ms += delta;
        agg.set(k2, cur);
      }
    }

    // Nothing to report
    if (agg.size === 0) {
      await msg.reply({ content: 'هیچ زوجی یافت نشد.' });
      return;
    }

    // Sort by ms desc
    const allPairs = Array.from(agg.values()).sort((p, q) => q.ms - p.ms);

    // Build top 10 non-bot pairs (lazy fetch members)
    const lines: string[] = [];
    const fmt = (ms: number) => {
      let s = Math.floor(ms / 1000);
      const h = Math.floor(s / 3600); s -= h * 3600;
      const m = Math.floor(s / 60); s -= m * 60;
      if (h > 0) return `${h}h ${m}m`;
      if (m > 0) return `${m}m ${s}s`;
      return `${s}s`;
    };

    for (const p of allPairs) {
      if (lines.length >= 10) break;
      let m1 = msg.guild?.members.cache.get(p.a) || null;
      let m2 = msg.guild?.members.cache.get(p.b) || null;
      try { if (!m1) m1 = await msg.guild?.members.fetch(p.a).catch(() => null) || null; } catch {}
      try { if (!m2) m2 = await msg.guild?.members.fetch(p.b).catch(() => null) || null; } catch {}
      if (!m1 || !m2) continue;
      if (m1.user.bot || m2.user.bot) continue;
      lines.push(`${lines.length + 1}. <@${p.a}> + <@${p.b}> — ${fmt(p.ms)}`);
    }

    if (lines.length === 0) {
      await msg.reply({ content: 'هیچ زوج غیر باتی یافت نشد.' });
      return;
    }

    const embed = new EmbedBuilder()
      .setTitle('top friends')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .hokm new — create room with join buttons (now includes Start button)
  if (content.startsWith('.hokm new')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    // reset session
    s.team1 = []; s.team2 = []; s.order = []; s.hakim = undefined; s.hokm = undefined; s.deck = []; s.hands.clear(); s.state = 'waiting'; s.ownerId = msg.author.id;
    const embed = new EmbedBuilder().setTitle('Hokm — اتاق جدید')
      .setDescription('با دکمه‌ها تیم خود را انتخاب کنید. هر تیم ۲ نفر. سپس `.hokm start` (یا `.hokm start 1..7`) را بزنید.')
      .setColor(0x2f3136);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('hokm-leave').setLabel('خروج').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('hokm-start').setLabel('شروع بازی').setStyle(ButtonStyle.Danger),
    );
    const sent = await msg.reply({ embeds: [embed], components: [row] });
    s.controlMsgId = sent.id;
    return;
  }

  // .a1 @user — owner assigns user to Team 1
  if (content.startsWith('.a1')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را اضافه کند.'); return; }
    const targets = await resolveTargetIds(msg, content, '.a1');
    if (targets.length === 0) { await msg.reply('استفاده: `.a1 @user1 @user2` یا ریپلای/آیدی'); return; }
    const added: string[] = []; const skipped: string[] = [];
    for (const uid of targets) {
      try { const u = await msg.client.users.fetch(uid); if (u.bot) { skipped.push(`<@${uid}> (bot)`); continue; } } catch { skipped.push(`<@${uid}> (نامعتبر)`); continue; }
      if (s.team1.includes(uid)) { skipped.push(`<@${uid}> (قبلاً تیم 1)`); continue; }
      s.team1 = s.team1.filter(x=>x!==uid); s.team2 = s.team2.filter(x=>x!==uid);
      if (s.team1.length >= 2) { skipped.push(`<@${uid}> (تیم 1 پر است)`); continue; }
      s.team1.push(uid); added.push(`<@${uid}>`);
    }
    const embed = new EmbedBuilder().setTitle('Hokm — اتاق فعال')
      .setDescription(`تیم 1: ${s.team1.map(u=>`<@${u}>`).join(' , ') || '—'}\nتیم 2: ${s.team2.map(u=>`<@${u}>`).join(' , ') || '—'}`)
      .setColor(0x2f3136);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success),
    );
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ embeds: [embed], components: [row] }); } } catch {}
    await msg.reply({ content: `افزوده شد: ${added.join(' , ') || '—'}\nنادیده: ${skipped.join(' , ') || '—'}` });
    return;
  }

  // .a2 @user — owner assigns user to Team 2
  if (content.startsWith('.a2')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را اضافه کند.'); return; }
    const targets = await resolveTargetIds(msg, content, '.a2');
    if (targets.length === 0) { await msg.reply('استفاده: `.a2 @user1 @user2` یا ریپلای/آیدی'); return; }
    const added: string[] = []; const skipped: string[] = [];
    for (const uid of targets) {
      try { const u = await msg.client.users.fetch(uid); if (u.bot) { skipped.push(`<@${uid}> (bot)`); continue; } } catch { skipped.push(`<@${uid}> (نامعتبر)`); continue; }
      if (s.team2.includes(uid)) { skipped.push(`<@${uid}> (قبلاً تیم 2)`); continue; }
      s.team1 = s.team1.filter(x=>x!==uid); s.team2 = s.team2.filter(x=>x!==uid);
      if (s.team2.length >= 2) { skipped.push(`<@${uid}> (تیم 2 پر است)`); continue; }
      s.team2.push(uid); added.push(`<@${uid}>`);
    }
    const embed = new EmbedBuilder().setTitle('Hokm — اتاق فعال')
      .setDescription(`تیم 1: ${s.team1.map(u=>`<@${u}>`).join(' , ') || '—'}\nتیم 2: ${s.team2.map(u=>`<@${u}>`).join(' , ') || '—'}`)
      .setColor(0x2f3136);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success),
    );
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ embeds: [embed], components: [row] }); } } catch {}
    await msg.reply({ content: `افزوده شد: ${added.join(' , ') || '—'}\nنادیده: ${skipped.join(' , ') || '—'}` });
    return;
  }

  // .r — owner removes a user from teams
  if (content.startsWith('.r')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را حذف کند.'); return; }
    const targets = await resolveTargetIds(msg, content, '.r');
    if (targets.length === 0) { await msg.reply('استفاده: `.r @user1 @user2` یا ریپلای/آیدی'); return; }
    const removed: string[] = []; const notIn: string[] = [];
    for (const uid of targets) {
      const inAny = s.team1.includes(uid) || s.team2.includes(uid);
      s.team1 = s.team1.filter(x=>x!==uid);
      s.team2 = s.team2.filter(x=>x!==uid);
      if (inAny) removed.push(`<@${uid}>`); else notIn.push(`<@${uid}>`);
    }
    const embed = new EmbedBuilder().setTitle('Hokm — اتاق فعال')
      .setDescription(`تیم 1: ${s.team1.map(u=>`<@${u}>`).join(' , ') || '—'}\nتیم 2: ${s.team2.map(u=>`<@${u}>`).join(' , ') || '—'}`)
      .setColor(0x2f3136);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success),
    );
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ embeds: [embed], components: [row] }); } } catch {}
    await msg.reply({ content: `حذف شد: ${removed.join(' , ') || '—'}\nناموجود: ${notIn.join(' , ') || '—'}` });
    return;
  }

  // .end — owner ends the room and disables controls
  if (content.startsWith('.end')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (!s.ownerId || msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند پایان دهد.'); return; }
    // disable buttons if control exists
    if (s.controlMsgId) {
      try {
        const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null);
        if (m) {
          const disabledRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
            new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary).setDisabled(true),
            new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success).setDisabled(true),
            new ButtonBuilder().setCustomId('hokm-leave').setLabel('خروج').setStyle(ButtonStyle.Secondary).setDisabled(true),
            new ButtonBuilder().setCustomId('hokm-start').setLabel('شروع بازی').setStyle(ButtonStyle.Danger).setDisabled(true),
          );
          await m.edit({ components: [disabledRow] });
        }
      } catch {}
    }
    // clear session
    s.team1 = []; s.team2 = []; s.order = []; s.hakim = undefined; s.hokm = undefined; s.deck = []; s.hands.clear(); s.state = 'finished'; s.controlMsgId = undefined;
    await msg.reply('اتاق پایان یافت.');
    return;
  }

  // .reset — owner resets the room and redeals (like fresh start with current teams)
  if (content.startsWith('.reset')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (!s.ownerId || msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند ریست کند.'); return; }
    if (s.team1.length !== 2 || s.team2.length !== 2) { await msg.reply('برای ریست، هر دو تیم باید ۲ نفر داشته باشند.'); return; }
    // reinitialize game state
    s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
    s.hakim = s.team1[0];
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.hokm = undefined;
    s.state = 'choosing_hokm';
    try { const user = await msg.client.users.fetch(s.hakim); await user.send({ content: `بازی ریست شد. دست اولیه شما (۵ کارت):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    // update control embed if exists
    if (s.controlMsgId) {
      const embed = new EmbedBuilder().setTitle('Hokm — اتاق فعال')
        .setDescription(`تیم 1: ${s.team1.map(u=>`<@${u}>`).join(' , ') || '—'}\nتیم 2: ${s.team2.map(u=>`<@${u}>`).join(' , ') || '—'}`)
        .setColor(0x2f3136);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId('hokm-leave').setLabel('خروج').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('hokm-start').setLabel('شروع بازی').setStyle(ButtonStyle.Danger),
      );
      try { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ embeds: [embed], components: [row] }); } catch {}
    }
    await msg.reply({ content: `ریست شد. حاکم: <@${s.hakim}> — لطفاً با ".hokm hokm <خال>" حکم را انتخاب کن.` });
    return;
  }

  // .hokm start — start game, deal first 5 to hakim (seat = team1[0]) and ask for hokm
  if (content.startsWith('.hokm start')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند بازی را شروع کند.'); return; }
    if (s.state !== 'waiting') { await msg.reply('اتاق در وضعیت شروع نیست.'); return; }
    if (s.team1.length !== 2 || s.team2.length !== 2) { await msg.reply('هر دو تیم باید ۲ نفر داشته باشند.'); return; }
    // parse optional target tricks
    const m = content.match(/^\.hokm start(?:\s+(\d+))?/);
    let target = 7;
    if (m && m[1]) {
      const n = parseInt(m[1], 10);
      if (Number.isNaN(n) || n < 1 || n > 7) { await msg.reply('عدد معتبر بین 1 تا 7 وارد کنید. مثال: `.hokm start 5`'); return; }
      target = n;
    }
    s.targetTricks = target;
    s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
    s.hakim = s.team1[0];
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    // deal 5 to hakim
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.state = 'choosing_hokm';
    // DM hakim hand
    try { const user = await msg.client.users.fetch(s.hakim); await user.send({ content: `دست اولیه شما (۵ کارت):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    await msg.reply({ content: `بازی آغاز شد. هدف برد دست‌ها: ${s.targetTricks}. حاکم: <@${s.hakim}> — لطفاً با دستور ".hokm hokm <خال>" حکم را انتخاب کن. (مثال: .hokm hokm ${SUIT_EMOJI['S']} یا پیک)` });
    return;
  }

  // .hokm hokm <suit> — hakim chooses trump; then deal remaining to all and DM hands
  if (content.startsWith('.hokm hokm')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'choosing_hokm' || !s.hakim) { await msg.reply('الان وقت انتخاب حکم نیست.'); return; }
    if (msg.author.id !== s.hakim) { await msg.reply('فقط حاکم می‌تواند حکم را انتخاب کند.'); return; }
    const arg = content.replace('.hokm hokm', '').trim();
    const suit = parseSuit(arg);
    if (!suit) { await msg.reply('خال نامعتبر. گزینه‌ها: ♠️ پیک، ♥️ دل، ♦️ خشت، ♣️ گیشنیز'); return; }
    s.hokm = suit;
    // deal remaining to all to reach 13
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    for (const uid of s.order) {
      const need = 13 - (s.hands.get(uid)?.length || 0);
      give(uid, need);
    }
    // init Phase 2 state
    s.state = 'playing';
    s.leaderIndex = s.order.indexOf(s.hakim);
    if (s.leaderIndex < 0) s.leaderIndex = 0;
    s.turnIndex = s.leaderIndex;
    s.table = [];
    s.leadSuit = null;
    s.tricksTeam1 = 0; s.tricksTeam2 = 0;
    // DM all hands
    for (const uid of s.order) {
      try { const user = await msg.client.users.fetch(uid); await user.send({ content: `حکم: ${SUIT_EMOJI[s.hokm]}\nدست شما:\n${handToString(s.hands.get(uid)!)}\nنوبت آغاز با حاکم <@${s.hakim}>` }); } catch {}
    }
    await msg.reply({ content: `حکم انتخاب شد: ${SUIT_EMOJI[s.hokm]} — نوبت آغاز با حاکم <@${s.hakim}>. با ".hokm play <کارت>" بازی کنید. مثال: .hokm play A${SUIT_EMOJI['S']}` });
    return;
  }

  // .hokm hand — DM your hand
  if (content.startsWith('.hokm hand')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state === 'waiting') { await msg.reply('بازی شروع نشده است.'); return; }
    const hand = s.hands.get(msg.author.id);
    if (!hand) { await msg.reply('شما در این بازی نیستید.'); return; }
    try { await msg.author.send({ content: `دست شما:\n${handToString(hand)}` }); await msg.reply({ content: 'به پیام‌های خصوصی‌ات ارسال شد.' }); } catch {
      await msg.reply('امکان ارسال پیام خصوصی به شما وجود ندارد.');
    }
    return;
  }

  // .hokm table — show teams and current state (with table/tricks)
  if (content.startsWith('.hokm table')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    const parts: string[] = [];
    parts.push(`تیم 1: ${s.team1.map(u=>`<@${u}>`).join(' , ') || '—'}`);
    parts.push(`تیم 2: ${s.team2.map(u=>`<@${u}>`).join(' , ') || '—'}`);
    parts.push(`حاکم: ${s.hakim?`<@${s.hakim}>`:'—'}`);
    parts.push(`حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:'—'}`);
    parts.push(`هدف دست‌ها: ${s.targetTricks ?? 7}`);
    if (s.state === 'playing') {
      parts.push(`برد دست‌ها — تیم1: ${s.tricksTeam1 ?? 0} | تیم2: ${s.tricksTeam2 ?? 0}`);
      const tableLines: string[] = [];
      if (s.table && s.table.length) {
        for (const p of s.table) tableLines.push(`<@${p.userId}>: ${cardStr(p.card)}`);
        parts.push(`میز:
${tableLines.join('\n')}`);
      } else {
        parts.push('میز: —');
      }
      const next = s.turnIndex!=null ? s.order[s.turnIndex] : undefined;
      if (next) parts.push(`نوبت: <@${next}>`);
    }
    parts.push(`وضعیت: ${s.state}`);
    const embed = new EmbedBuilder().setTitle('Hokm — وضعیت میز').setDescription(parts.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .komak — help
  if (content.startsWith('.komak')) {
    const lines: string[] = [
      '• .t <مدت> [دلیل] — تنظیم تایمر. نمونه: `.t 10m` یا `.t 60 [دلیل]`',
      '• .e <ثانیه> — افزودن چند ثانیه به آخرین تایمر خودت. نمونه: `.e 30`',
      '• .friend [@کاربر|آیدی] — نمایش ۱۰ نفرِ برتر که بیشترین هم‌حضوری ویس با کاربر هدف را داشته‌اند (بدون ربات‌ها).',
      '• .topfriend — نمایش ۱۰ زوج برتر با بیشترین هم‌حضوری در ویس (بدون ربات‌ها).',
      '• .ll [@کاربر|آیدی] — محاسبه و ساخت تصویر درصد عشق بین شما و کاربر هدف.',
      '• .llset @user1 @user2 <0..100> — فقط مدیران: تنظیم درصد ثابت عشق برای دو کاربر.',
      '• .llunset @user1 @user2 — فقط مدیران: حذف تنظیم ثابت درصد عشق.',
      '• .av [@کاربر|آیدی] — نمایش آواتار کاربر (با لینک).',
      '• .ba [@کاربر|آیدی] — نمایش بنر کاربر (اگر داشته باشد).',
      '• Slash: /timer set|list|cancel — تایمر با اینترفیس اسلش‌کامند (ثبت با `npm run register:commands`).',
    ];
    const embed = new EmbedBuilder()
      .setTitle('راهنمای دستورات')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .av [@user|userId]
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


  // .llset — admin only
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

  // .llunset — admin only
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
      let love: number;
      const gIdForLove = msg.guildId!;
      const pair = loveKey(userA.id, targetB.id);
      try {
        // 1) Admin override wins
        const mOverride = loveOverrides.get(gIdForLove);
        const vOverride = mOverride?.get(pair);
        if (typeof vOverride === 'number') {
          love = vOverride;
        } else {
          // 2) Sticky random: reuse if exists, otherwise create and persist
          let mRand = loveRandoms.get(gIdForLove);
          if (!mRand) { mRand = new Map<string, number>(); loveRandoms.set(gIdForLove, mRand); }
          const vRand = mRand.get(pair);
          if (typeof vRand === 'number') {
            love = vRand;
          } else {
            love = Math.floor(Math.random() * 101);
            mRand.set(pair, love);
            saveLoveRandoms();
          }
        }
      } catch {
        // Fallback if anything goes wrong
        love = Math.floor(Math.random() * 101);
      }
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
