import 'dotenv/config';
import { Client, GatewayIntentBits, Interaction, Message, EmbedBuilder, VoiceState, Collection, PermissionsBitField, ActionRowBuilder, ButtonBuilder, ButtonStyle, GuildMember, AttachmentBuilder } from 'discord.js';
import { createCanvas, GlobalFonts, loadImage } from '@napi-rs/canvas';
import fs from 'fs';
import path from 'path';
import { PgFriendStore } from './storage/pgFriendStore';
import { handleTimerInteraction, TimerManager, parseDuration, makeTimerSetEmbed } from './modules/timerManager';

const token = process.env.BOT_TOKEN;

// Persian font registration
try {
  const persianFontPath = path.join(process.cwd(), 'assets', 'fonts', 'Sarbaz.ttf');
  if (fs.existsSync(persianFontPath)) {
    const buf = fs.readFileSync(persianFontPath);
    GlobalFonts.register(buf, 'Sarbaz');
    console.log('[Font] Persian font (Sarbaz) registered successfully');
  }
} catch (e) {
  console.warn('[Font] Failed to register Persian font:', e);
}

// Emoji font registration (optional)
let emojiFontAvailable = false;
try {
  const candidates = [
    path.join(process.cwd(), 'fonts', 'NotoColorEmoji.ttf'),
    path.join(process.cwd(), 'fonts', 'NotoColorEmoji-Regular.ttf'),
    path.join(process.cwd(), 'fonts', 'NotoColorEmojiCompat.ttf'),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      const buf = fs.readFileSync(p);
      GlobalFonts.register(buf, 'Noto Color Emoji');
      emojiFontAvailable = true;
      break;
    }
  }
} catch {}

// ===== Hokm Phase 1 state =====
type Suit = 'S' | 'H' | 'D' | 'C';
const SUIT_EMOJI: Record<Suit, string> = { S: 'â ď¸', H: 'âĽď¸', D: 'âŚď¸', C: 'âŁď¸' };
const EMOJI_TO_SUIT: Record<string, Suit> = {
  'â ': 'S','â ď¸': 'S',':spades:': 'S','đĄ': 'S',
  'âĽ': 'H','âĽď¸': 'H',':hearts:': 'H',
  'âŚ': 'D','âŚď¸': 'D',':diamonds:': 'D',
  'âŁ': 'C','âŁď¸': 'C',':clubs:': 'C',
  'ŮžŰÚŠ': 'S','ŘŻŮ': 'H','ŘŽŘ´ŘŞ': 'D','ÚŻŰŘ´ŮŰŘ˛': 'C','ÚŻŘ´ŮŰŘ˛': 'C'
};
const RANKS = [2,3,4,5,6,7,8,9,10,11,12,13,14]; // 11:J 12:Q 13:K 14:A
interface Card { s: Suit; r: number }

// ===== Virtual Bots =====
function isVirtualBot(id: string) { return /^BOT[1-3]$/.test(id); }
function nextAvailableBotId(s: HokmSession): string | null {
  const allBots = [...s.team1, ...s.team2].filter(isVirtualBot);
  const count = allBots.length;
  if (count >= 3) return null;
  return `BOT${count + 1}`;
}
function addBotToTeam(s: HokmSession, team: 1|2): { id: string } | null {
  const id = nextAvailableBotId(s); if (!id) return null;
  const teamArr = team===1 ? s.team1 : s.team2;
  if (teamArr.length >= 2) return null;
  teamArr.push(id);
  return { id };
}

function controlListText(s: HokmSession): string {
  const name = (u: string)=> isVirtualBot(u) ? u.replace('BOT','Bot') : `<@${u}>`;
  const t1 = s.team1.map((u,i)=>`${i+1}- ${name(u)}`).join('\n') || 'â';
  const t2 = s.team2.map((u,i)=>`${i+1}- ${name(u)}`).join('\n') || 'â';
  const sep = 'ââŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâ';
  return [
    sep,
    'ŘŞŰŮ 1:',
    t1,
    sep,
    'ŘŞŰŮ 2:',
    t2,
    sep,
  ].join('\n');
}
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
  hokmChooseMessageId?: string; // message asking hakim to choose hokm
  // Phase 2
  leaderIndex?: number; // index into order for current trick leader
  turnIndex?: number; // index into order whose turn it is now
  table?: { userId: string; card: Card }[];
  leadSuit?: Suit | null;
  tricksTeam1?: number;
  tricksTeam2?: number;
  tricksByPlayer?: Map<string, number>;
  lastTrick?: { userId: string; card: Card }[];
  // match-level (sets)
  targetSets?: number; // how many won hands (sets) to win the match
  setsTeam1?: number;
  setsTeam2?: number;
}
// ===== Hokm Stats =====
type HokmUserStat = {
  games: number;
  wins: number;
  teammateWins: Record<string, number>;
  hokmPicks: Partial<Record<Suit, number>>;
};
const hokmStats: Map<string, Map<string, HokmUserStat>> = new Map();
const hokmStatsFile = path.join(process.cwd(), 'data', 'hokm-stats.json');
function loadHokmStats() {
  try {
    fs.mkdirSync(path.dirname(hokmStatsFile), { recursive: true });
    const raw = fs.existsSync(hokmStatsFile) ? fs.readFileSync(hokmStatsFile, 'utf8') : '';
    if (!raw) return;
    const obj = JSON.parse(raw) as Record<string, Record<string, HokmUserStat>>;
    hokmStats.clear();
    for (const [g, users] of Object.entries(obj)) {
      const m = new Map<string, HokmUserStat>();
      for (const [uid, st] of Object.entries(users)) m.set(uid, st as HokmUserStat);
      hokmStats.set(g, m);
    }
  } catch {}
}
function saveHokmStats() {
  try {
    fs.mkdirSync(path.dirname(hokmStatsFile), { recursive: true });
    const obj: Record<string, Record<string, HokmUserStat>> = {};
    for (const [g, m] of hokmStats) obj[g] = Object.fromEntries(m.entries());
    fs.writeFileSync(hokmStatsFile, JSON.stringify(obj, null, 2), 'utf8');
  } catch {}
}
function ensureUserStat(gId: string, uid: string): HokmUserStat {
  let g = hokmStats.get(gId);
  if (!g) { g = new Map(); hokmStats.set(gId, g); }
  let st = g.get(uid);
  if (!st) { st = { games: 0, wins: 0, teammateWins: {}, hokmPicks: {} }; g.set(uid, st); }
  return st;
}
function addHokmPick(gId: string, uid: string, suit: Suit) {
  const st = ensureUserStat(gId, uid);
  st.hokmPicks[suit] = (st.hokmPicks[suit] || 0) + 1;
}
loadHokmStats();
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
  if (t.includes('â ') || t.includes(':spades:') || t.endsWith('s')) s = 'S';
  else if (t.includes('âĽ') || t.includes(':hearts:') || t.endsWith('h')) s = 'H';
  else if (t.includes('âŚ') || t.includes(':diamonds:') || t.endsWith('d')) s = 'D';
  else if (t.includes('âŁ') || t.includes(':clubs:') || t.endsWith('c')) s = 'C';
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
function suitName(s: Suit){ return s==='S'?'â ď¸ ŮžŰÚŠ':s==='H'?'âĽď¸ ŘŻŮ':s==='D'?'âŚď¸ ŘŽŘ´ŘŞ':'âŁď¸ ÚŻŰŘ´ŮŰŘ˛'; }

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
      row.addComponents(new ButtonBuilder().setCustomId(`hokm-play-${s.guildId}-${s.channelId}-${userId}-${c.s}-${c.r}`).setLabel(cardStr(c)).setStyle(ButtonStyle.Secondary));
    }
    rows.push(row);
  }
  // filter row
  const rowFilter = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-ALL`).setLabel('ŮŮŮ').setStyle(filter==='ALL'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-S`).setLabel('â ď¸').setStyle(filter==='S'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-H`).setLabel('âĽď¸').setStyle(filter==='H'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-D`).setLabel('âŚď¸').setStyle(filter==='D'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-C`).setLabel('âŁď¸').setStyle(filter==='C'?ButtonStyle.Primary:ButtonStyle.Secondary),
  );
  rows.push(rowFilter);
  // pagination row (if needed)
  if (totalPages > 1) {
    const rowPage = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId(`hokm-hand-page-${s.guildId}-${s.channelId}-${userId}-${Math.max(0, page-1)}`).setLabel('ŮŘ¨ŮŰ').setStyle(ButtonStyle.Secondary).setDisabled(page<=0),
      new ButtonBuilder().setCustomId(`hokm-hand-page-${s.guildId}-${s.channelId}-${userId}-${Math.min(totalPages-1, page+1)}`).setLabel('Ř¨ŘšŘŻŰ').setStyle(ButtonStyle.Secondary).setDisabled(page>=totalPages-1),
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
    const content = `Ř­ÚŠŮ: ${s.hokm?SUIT_EMOJI[s.hokm]:''} â ${userId===s.order[s.turnIndex??0]?'ŮŮŘ¨ŘŞ Ř´ŮŘ§ŘłŘŞ.':'ŮŮŘŞŘ¸Řą ŮŮŘ¨ŘŞ Ř¨ŮŘ§ŮŰŘŻ.'}\nŘŻŘłŘŞ Ř´ŮŘ§:\n${handToString(s.hands.get(userId) || [])}`;
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

function buildHandRowsSimple(hand: Card[], userId: string, gId: string, cId: string): ActionRowBuilder<ButtonBuilder>[] {
  const rows: ActionRowBuilder<ButtonBuilder>[] = [];
  const items = [...hand].sort((a,b)=> a.s===b.s ? b.r-a.r : ['S','H','D','C'].indexOf(a.s)-['S','H','D','C'].indexOf(b.s));
  for (let r=0; r<3; r++) {
    const slice = items.slice(r*5, r*5+5);
    if (!slice.length) break;
    const row = new ActionRowBuilder<ButtonBuilder>();
    for (const c of slice) {
      row.addComponents(new ButtonBuilder().setCustomId(`hokm-play-${gId}-${cId}-${userId}-${c.s}-${c.r}`).setLabel(cardStr(c)).setStyle(ButtonStyle.Secondary));
    }
    rows.push(row);
  }
  return rows;
}

async function refreshPlayerChannelHand(ctx: { channel: any }, s: HokmSession, userId: string) {
  const hand = s.hands.get(userId) || [];
  const rows = buildHandRowsSimple(hand, userId, s.guildId, s.channelId);
  const content = `<@${userId}> â ${userId===s.order[s.turnIndex??0] ? 'ŮŮŘ¨ŘŞ Ř´ŮŘ§ŘłŘŞ.' : 'ŮŮŘŞŘ¸Řą ŮŮŘ¨ŘŞ Ř¨ŮŘ§ŮŰŘŻ.'}`;
  s.playerDMMsgIds = s.playerDMMsgIds || new Map<string,string>();
  const prevId = s.playerDMMsgIds.get(userId);
  if (prevId) {
    const m = await ctx.channel.messages.fetch(prevId).catch(()=>null);
    if (m) { await m.edit({ content, components: rows }); return; }
  }
  const msg = await ctx.channel.send({ content, components: rows });
  s.playerDMMsgIds.set(userId, msg.id);
}

type AvatarEntry = { img: any; tag: string; at: number };
const avatarCache: Map<string, AvatarEntry> = new Map();
const AVATAR_TTL_MS = 10 * 60 * 1000;

async function getMemberVisual(guildId: string, userId: string): Promise<{ tag: string; img: any|null }> {
  try {
    const g = await client.guilds.fetch(guildId).catch(()=>null);
    if (!g) return { tag: userId, img: null };
    const m = await g.members.fetch(userId).catch(()=>null as any);
    // prefer stable username to avoid font tofu for fancy display names
    const tag = m?.user?.username || m?.displayName || userId;
    const now = Date.now();
    const cached = avatarCache.get(userId);
    if (cached && now - cached.at < AVATAR_TTL_MS) {
      return { tag, img: cached.img };
    }
    const url = m?.displayAvatarURL({ extension: 'png', size: 64 }) as string | undefined;
    if (url) {
      const img = await loadImage(url).catch(()=>null);
      if (img) { avatarCache.set(userId, { img, tag, at: now }); return { tag, img }; }
    }
    return { tag, img: null };
  } catch { return { tag: userId, img: null }; }
}

function drawSuit(ctx: any, s: Suit, x: number, y: number, size: number) {
  ctx.save();
  ctx.translate(x, y);
  const red = (s==='H'||s==='D');
  ctx.fillStyle = red ? '#dc2626' : '#111827';
  ctx.beginPath();
  const r = size;
  if (s==='H') { // heart
    ctx.moveTo(0, r*0.6);
    ctx.bezierCurveTo(-r, -r*0.3, -r*0.6, -r, 0, -r*0.4);
    ctx.bezierCurveTo(r*0.6, -r, r, -r*0.3, 0, r*0.6);
  } else if (s==='D') { // diamond
    ctx.moveTo(0, -r);
    ctx.lineTo(r*0.8, 0);
    ctx.lineTo(0, r);
    ctx.lineTo(-r*0.8, 0);
    ctx.closePath();
  } else if (s==='S') { // spade
    ctx.moveTo(0, -r);
    ctx.bezierCurveTo(r, -r*0.2, r*0.7, r*0.6, 0, r*0.2);
    ctx.bezierCurveTo(-r*0.7, r*0.6, -r, -r*0.2, 0, -r);
    // stem
    ctx.moveTo(-r*0.25, r*0.4);
    ctx.lineTo(r*0.25, r*0.4);
    ctx.lineTo(0, r*0.9);
    ctx.closePath();
  } else { // club
    ctx.arc(-r*0.35, 0, r*0.35, 0, Math.PI*2);
    ctx.arc(r*0.35, 0, r*0.35, 0, Math.PI*2);
    ctx.arc(0, -r*0.45, r*0.35, 0, Math.PI*2);
    // stem
    ctx.moveTo(-r*0.2, r*0.2);
    ctx.lineTo(r*0.2, r*0.2);
    ctx.lineTo(0, r*0.9);
    ctx.closePath();
  }
  ctx.fill();
  ctx.restore();
}

async function renderTableImage(s: HokmSession): Promise<Buffer> {
  // square canvas to avoid layout overlap on edges
  const width = 1000, height = 1000;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');
  // background felt
  ctx.fillStyle = '#0f5132';
  ctx.fillRect(0, 0, width, height);
  // border frame
  ctx.strokeStyle = '#e5e7eb';
  ctx.lineWidth = 6;
  ctx.strokeRect(10, 10, width-20, height-20);
  // top bar (dark strip)
  ctx.fillStyle = 'rgba(0,0,0,0.25)';
  ctx.fillRect(10, 10, width-20, 54);
  // center hokm and sets at the top (always visible)
  const cx = Math.floor(width/2);
  const cy = 10 + 27; // vertical center of the top strip
  ctx.textBaseline = 'middle';
  const setsTxt = `Sets: ${s.targetSets ?? 1}`;
  ctx.font = `${ssdFontAvailable? 'bold 22px '+ssdFontFamily : 'bold 22px Arial'}`;
  const setsWidth = ctx.measureText(setsTxt).width;
  const gap = 10;
  if (s.hokm) {
    const suitFill = (s.hokm==='H' || s.hokm==='D') ? '#f87171' : '#ffffff';
    const totalWidth = 36 + gap + setsWidth; // suit ~36px
    const startX = cx - totalWidth/2;
    if (emojiFontAvailable) {
      ctx.textAlign = 'center';
      ctx.fillStyle = '#ffffff';
      ctx.font = `${ssdFontAvailable? 'bold 22px '+ssdFontFamily : 'bold 22px Arial'}, 'Noto Color Emoji', 'Segoe UI Emoji', 'Apple Color Emoji'`;
      ctx.fillText(SUIT_EMOJI[s.hokm], startX + 18, cy + 1);
      ctx.textAlign = 'left';
    } else {
      ctx.fillStyle = suitFill;
      drawSuit(ctx, s.hokm, startX + 18, cy, 18);
    }
    ctx.textAlign = 'left';
    ctx.fillStyle = '#ffffff';
    ctx.fillText(setsTxt, startX + 36 + gap, cy + 1);
  } else {
    const hokmTxt = 'Ř­ÚŠŮŘ';
    ctx.fillStyle = '#ffffff';
    const hokmWidth = ctx.measureText(hokmTxt).width;
    const totalWidth = hokmWidth + gap + setsWidth;
    const startX = cx - totalWidth/2;
    ctx.textAlign = 'left';
    ctx.fillText(hokmTxt, startX, cy + 1);
    ctx.fillText(setsTxt, startX + hokmWidth + gap, cy + 1);
  }

  // positions for seats and cards (square layout, generous margins)
  const margin = 220;
  const seats = [
    { x: width/2, y: margin },                // N
    { x: width - margin, y: height/2 },       // E
    { x: width/2, y: height - margin },       // S
    { x: margin, y: height/2 },               // W
  ];
  const avatarRadius = 64; // bigger avatars
  const nameFont = `${ssdFontAvailable? '22px '+ssdFontFamily : '22px Arial'}`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  function drawSeatLabel(i: number, uid?: string, name?: string, avatar?: any, isTurn?: boolean, playerTricks?: number) {
    const seat = seats[i];
    // avatar only (bigger)
    const avR = avatarRadius;
    const avX = seat.x;
    const avY = seat.y; // center on seat
    if (avatar) {
      ctx.save();
      ctx.beginPath();
      ctx.arc(avX, avY, avR, 0, Math.PI*2);
      ctx.closePath();
      ctx.clip();
      ctx.drawImage(avatar, avX-avR, avY-avR, avR*2, avR*2);
      ctx.restore();
    }
    // team-colored ring
    const isT1 = uid ? s.team1.includes(uid) : false;
    const teamColor = isT1 ? '#3b82f6' : '#ef4444';
    ctx.save();
    ctx.strokeStyle = teamColor;
    ctx.lineWidth = 5;
    ctx.beginPath();
    ctx.arc(avX, avY, avR + 4, 0, Math.PI * 2);
    ctx.stroke();
    // yellow outer ring if player's turn (brighter + glow)
    if (isTurn) {
      // glow
      ctx.save();
      ctx.strokeStyle = '#fde047'; // bright yellow
      ctx.lineWidth = 8;
      (ctx as any).shadowColor = '#fde047';
      (ctx as any).shadowBlur = 18;
      ctx.beginPath();
      ctx.arc(avX, avY, avR + 12, 0, Math.PI * 2);
      ctx.stroke();
      ctx.restore();
      // solid highlight ring
      ctx.strokeStyle = '#facc15';
      ctx.lineWidth = 4;
      ctx.beginPath();
      ctx.arc(avX, avY, avR + 12, 0, Math.PI * 2);
      ctx.stroke();
    }
    ctx.restore();
    // tricks badge (square) centered below avatar
    if (typeof playerTricks === 'number') {
      const bx = avX; // center x under avatar
      const by = avY + avR + 26; // below avatar
      ctx.save();
      ctx.fillStyle = 'rgba(0,0,0,0.45)';
      ctx.beginPath();
      ctx.roundRect(bx-18, by-14, 36, 28, 8);
      ctx.fill();
      ctx.fillStyle = '#f9fafb';
      ctx.textAlign = 'center';
      ctx.font = `${ssdFontAvailable? '18px '+ssdFontFamily : '18px Arial'}`;
      ctx.fillText(String(playerTricks), bx, by+3);
      ctx.restore();
    }
    ctx.textAlign = 'center';
  }
  function drawCard(x: number, y: number, c: Card) {
    const w = 110, h = 155, r = 12;
    // shadow
    ctx.fillStyle = 'rgba(0,0,0,0.35)';
    ctx.beginPath();
    ctx.roundRect(x+4, y+6, w, h, r);
    ctx.fill();
    // body
    ctx.fillStyle = '#ffffff';
    ctx.beginPath();
    ctx.roundRect(x, y, w, h, r);
    ctx.fill();
    ctx.strokeStyle = '#e5e7eb';
    ctx.lineWidth = 2;
    ctx.stroke();
    // rank + suit
    const red = (c.s === 'H' || c.s === 'D');
    ctx.fillStyle = red ? '#dc2626' : '#111827';
    ctx.font = `${ssdFontAvailable? 'bold 36px '+ssdFontFamily : 'bold 36px Arial'}`;
    const rtxt = rankStr(c.r);
    ctx.textAlign = 'left';
    ctx.fillText(rtxt, x + 10, y + 28);
    // center suit: prefer emoji if emoji font is available; fallback to vector
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    if (emojiFontAvailable) {
      ctx.fillStyle = '#ffffff';
      ctx.font = `bold 56px 'Noto Color Emoji'`;
      ctx.fillText(SUIT_EMOJI[c.s], x + w/2, y + h/2 + 6);
    } else {
      ctx.fillStyle = red ? '#dc2626' : '#111827';
      drawSuit(ctx, c.s, x + w/2, y + h/2 + 6, 28);
    }
  }
  // draw seats and played cards
  for (let i=0;i<4;i++) {
    const uid = s.order[i];
    if (!uid) continue;
    const mv = await getMemberVisual(s.guildId, uid);
    const isTurn = s.turnIndex!=null && s.order[s.turnIndex] === uid;
    const pTricks = s.tricksByPlayer?.get(uid) ?? 0;
    drawSeatLabel(i, uid, mv.tag, mv.img, isTurn, pTricks);
    const play = (s.table||[]).find(t=>t.userId===uid);
    if (play) {
      // offset from seat for card placement
      const cardPos = [
        {x: seats[i].x - 55, y: seats[i].y + (avatarRadius + 70)},     // N: ŮžŘ§ŰŰŮ Ř˘ŮŘ§ŘŞŘ§Řą
        {x: seats[i].x - (avatarRadius + 170), y: seats[i].y - 75},    // E: ÚŮž Ř˘ŮŘ§ŘŞŘ§Řą
        {x: seats[i].x - 55, y: seats[i].y - (avatarRadius + 210)},    // S: Ř¨Ř§ŮŘ§Ű Ř˘ŮŘ§ŘŞŘ§Řą
        {x: seats[i].x + (avatarRadius + 170), y: seats[i].y - 75},    // W: ŘąŘ§ŘłŘŞ Ř˘ŮŘ§ŘŞŘ§Řą
      ][i];
      drawCard(cardPos.x, cardPos.y, play.card);
    }
  }

  // Show previous trick (lastTrick) bottom-right
  if (s.lastTrick && s.lastTrick.length === 4) {
    const baseX = width - 480; const baseY = height - 260;
    ctx.save();
    ctx.globalAlpha = 0.95;
    for (let i=0;i<4;i++) {
      const dx = baseX + (i%2)*140;
      const dy = baseY + Math.floor(i/2)*90;
      drawCard(dx, dy, s.lastTrick[i].card);
    }
    ctx.restore();
    ctx.fillStyle = 'rgba(0,0,0,0.4)';
    ctx.font = `${ssdFontAvailable? '18px '+ssdFontFamily : '18px Arial'}`;
    ctx.textAlign = 'right';
    ctx.fillText('Last Trick', width - 24, baseY - 12);
  }

  // Team labels and scores with colors (bold, placed below top bar)
  const team1Color = '#3b82f6';
  const team2Color = '#ef4444';
  const numColor = '#ffffff';
  // Left side (Team 1)
  ctx.textAlign = 'left';
  ctx.font = `${ssdFontAvailable? 'bold 44px '+ssdFontFamily : 'bold 44px Arial'}`;
  ctx.fillStyle = team1Color;
  ctx.fillText('Team 1', 28, 96);
  // Tricks/Sets line with white numbers; add more vertical spacing
  ctx.font = `${ssdFontAvailable? 'bold 40px '+ssdFontFamily : 'bold 40px Arial'}`;
  let xL = 28; const yL = 146;
  const tLabel = 'Tricks: ';
  const sLabel = '  Sets: ';
  ctx.fillStyle = team1Color; ctx.fillText(tLabel, xL, yL); xL += ctx.measureText(tLabel).width;
  ctx.fillStyle = numColor; ctx.fillText(String(s.tricksTeam1 ?? 0), xL, yL); xL += ctx.measureText(String(s.tricksTeam1 ?? 0)).width;
  ctx.fillStyle = team1Color; ctx.fillText(sLabel, xL, yL); xL += ctx.measureText(sLabel).width;
  ctx.fillStyle = numColor; ctx.fillText(String(s.setsTeam1 ?? 0), xL, yL);
  // Right side (Team 2)
  ctx.textAlign = 'right';
  ctx.fillStyle = team2Color;
  ctx.font = `${ssdFontAvailable? 'bold 44px '+ssdFontFamily : 'bold 44px Arial'}`;
  ctx.fillText('Team 2', width-28, 96);
  ctx.font = `${ssdFontAvailable? 'bold 40px '+ssdFontFamily : 'bold 40px Arial'}`;
  const yR = 146; let xR = width - 28;
  const t2 = String(s.tricksTeam2 ?? 0); const s2 = String(s.setsTeam2 ?? 0);
  // draw from right to left
  ctx.fillStyle = numColor; ctx.fillText(s2, xR, yR); xR -= ctx.measureText(s2).width;
  ctx.fillStyle = team2Color; ctx.fillText(sLabel, xR, yR); xR -= ctx.measureText(sLabel).width;
  ctx.fillStyle = numColor; ctx.fillText(t2, xR, yR); xR -= ctx.measureText(t2).width;
  ctx.fillStyle = team2Color; ctx.fillText(tLabel, xR, yR);
  return canvas.toBuffer('image/png');
}

async function refreshTableEmbed(ctx: { channel: any }, s: HokmSession) {
  const img = await renderTableImage(s);
  const attachment = new AttachmentBuilder(img, { name: 'table.png' });
  const embed = new EmbedBuilder()
    .setTitle('Hokm â ŮŰŘ˛ Ř¨Ř§Ř˛Ű')
    .setColor(0x2f3136)
    .setImage('attachment://table.png');
  const openRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`hokm-open-hand-${s.guildId}-${s.channelId}`).setLabel('ŘŻŘłŘŞ ŮŮ').setStyle(ButtonStyle.Secondary)
  );
  // add hokm choose buttons when waiting for hakim to pick
  const rows: any[] = [openRow];
  if (s.state === 'choosing_hokm' && s.hakim) {
    const chooseRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-choose-S').setLabel('â ď¸ ŮžŰÚŠ').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-choose-H').setLabel('âĽď¸ ŘŻŮ').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('hokm-choose-D').setLabel('âŚď¸ ŘŽŘ´ŘŞ').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('hokm-choose-C').setLabel('âŁď¸ ÚŻŰŘ´ŮŰŘ˛').setStyle(ButtonStyle.Success),
    );
    embed.setDescription(`Ř­Ř§ÚŠŮ: <@${s.hakim}> â ŮŘˇŮŘ§Ů Ř­ÚŠŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮ.`);
    rows.push(chooseRow);
  }
  if (s.tableMsgId) {
    const m = await ctx.channel.messages.fetch(s.tableMsgId).catch(()=>null);
    if (m) { await m.edit({ embeds: [embed], components: rows, files: [attachment] }); return; }
  }
  const sent = await ctx.channel.send({ embeds: [embed], components: rows, files: [attachment] });
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
  // per-player trick counter
  if (!s.tricksByPlayer) s.tricksByPlayer = new Map();
  s.tricksByPlayer.set(winnerUserId, (s.tricksByPlayer.get(winnerUserId) || 0) + 1);
  // store last trick and set next trick
  s.lastTrick = (s.table || []).slice(0, 4);
  s.leaderIndex = winnerTurnIndex; s.turnIndex = winnerTurnIndex; s.table = []; s.leadSuit = null;
  const target = s.targetTricks ?? 7;
  // Always operate on the game text channel, not the DM channel
  let gameChannel: any = null;
  try { gameChannel = await (interaction.client as Client).channels.fetch(s.channelId).catch(()=>null); } catch {}
  if ((s.tricksTeam1||0) >= target || (s.tricksTeam2||0) >= target) {
    // hand complete -> award a set
    const winnerTeam = (s.tricksTeam1||0) >= target ? 't1' : 't2';
    s.setsTeam1 = s.setsTeam1 || 0; s.setsTeam2 = s.setsTeam2 || 0;
    if (winnerTeam==='t1') s.setsTeam1++; else s.setsTeam2++;
    const targetSets = s.targetSets ?? 1;
    if ((s.setsTeam1>=targetSets) || (s.setsTeam2>=targetSets)) {
      s.state = 'finished';
      // update stats
      try {
        const gId = s.guildId;
        const t1 = s.team1; const t2 = s.team2;
        const winners = (s.setsTeam1 ?? 0) >= targetSets ? t1 : t2;
        for (const uid of [...t1, ...t2]) ensureUserStat(gId, uid).games += 1;
        for (const uid of winners) ensureUserStat(gId, uid).wins += 1;
        // teammate wins (per team, +1 for both teammates)
        if (winners.length === 2) {
          const [a,b] = winners;
          ensureUserStat(gId, a).teammateWins[b] = (ensureUserStat(gId, a).teammateWins[b] || 0) + 1;
          ensureUserStat(gId, b).teammateWins[a] = (ensureUserStat(gId, b).teammateWins[a] || 0) + 1;
        }
        saveHokmStats();
      } catch {}
      if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s);
      // result embed
      if (gameChannel) {
        const t1Set = s.setsTeam1 ?? 0; const t2Set = s.setsTeam2 ?? 0;
        const starter = s.ownerId ? `<@${s.ownerId}>` : 'â';
        const lines: string[] = [];
        lines.push(`âšStarter: ${starter}`);
        lines.push(`âšSets: [${s.targetSets ?? 1}]`);
        lines.push('ââŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâ');
        lines.push(`âšTeam 1: ${s.team1.map(u=>`<@${u}>`).join(' , ')} â¤ [${t1Set}]`);
        lines.push('ââââââââââââââââââââ');
        lines.push(`âšTeam 2: ${s.team2.map(u=>`<@${u}>`).join(' , ')} â¤ [${t2Set}]`);
        lines.push('ââŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâ');
        lines.push(`âšWinner: Team ${t1Set>t2Set?1:2} â`);
        const emb = new EmbedBuilder().setDescription(lines.join('\n')).setColor(t1Set>t2Set?0x3b82f6:0xef4444);
        await gameChannel.send({ embeds: [emb] });
      }
      return;
    }
    // prepare next hand in same match
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    s.hokm = undefined; s.table = []; s.leadSuit = null; s.tricksTeam1 = 0; s.tricksTeam2 = 0; s.tricksByPlayer = new Map(); s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
    // choose next hakim randomly (can be improved to winner-led)
    s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.state = 'choosing_hokm';
    try { const user = await (interaction.client as Client).users.fetch(s.hakim); await user.send({ content: `ŘłŘŞ ŘŹŘŻŰŘŻ Ř´ŘąŮŘš Ř´ŘŻ. ŘŻŘłŘŞ Ř§ŮŮŰŮ Ř´ŮŘ§ (Űľ ÚŠŘ§ŘąŘŞ):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    if (gameChannel) await gameChannel.send({ content: `ŘłŘŞ ŘŹŘŻŰŘŻ Ř˘ŘşŘ§Ř˛ Ř´ŘŻ. Ř­Ř§ÚŠŮ: <@${s.hakim}> â ŮŘˇŮŘ§Ů Ř­ÚŠŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮ.` });
    if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s);
    await refreshAllDMs({ client: (interaction.client as Client) }, s);
    return;
  }

  if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s);
  await refreshAllDMs({ client: (interaction.client as Client) }, s);
}

function handToString(hand: Card[]){ const bySuit: Record<Suit, Card[]> = {S:[],H:[],D:[],C:[]}; hand.forEach(c=>bySuit[c.s].push(c)); (Object.keys(bySuit) as Suit[]).forEach(s=>bySuit[s].sort((a,b)=>b.r-a.r));
  const parts: string[] = [];
  (['S','H','D','C'] as Suit[]).forEach(s=>{ if(bySuit[s].length){ parts.push(`${SUIT_EMOJI[s]} ${bySuit[s].map(c=>rankStr(c.r)).join(' ')}`); }});
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
    s = { guildId: gId, channelId: cId, team1: [], team2: [], order: [], deck: [], hands: new Map(), state: 'waiting', tricksByPlayer: new Map() };
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
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'ŘŽŘˇŘ§Ű ŘłŘąŮŘą.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      const uid = interaction.user.id;
      // Remove from both teams first
      s.team1 = s.team1.filter(x=>x!==uid);
      s.team2 = s.team2.filter(x=>x!==uid);
      if (id === 'hokm-leave') {
        s.team1 = s.team1.filter(x=>x!==uid);
        s.team2 = s.team2.filter(x=>x!==uid);
        await interaction.reply({ content: 'Ř§Ř˛ Ř§ŘŞŘ§Ů ŘŽŘ§ŘąŘŹ Ř´ŘŻŰ.', ephemeral: true });
      } else {
        const target = id === 'hokm-join-t1' ? s.team1 : s.team2;
      if (target.length >= 2) { await interaction.reply({ content: 'Ř§ŰŮ ŘŞŰŮ ŮžŘą Ř§ŘłŘŞ.', ephemeral: true }); return; }
      target.push(uid);
        await interaction.reply({ content: `Ř¨Ů ŘŞŰŮ ${id.endsWith('t1')? '1':'2'} ŮžŰŮŘłŘŞŰ.`, ephemeral: true });
      }
      // Update control message as plain text (no embed)
      const contentText = controlListText(s);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('ŘŞŰŮ 1').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('ŘŞŰŮ 2').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId('hokm-leave').setLabel('ŘŽŘąŮŘŹ').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('hokm-start').setLabel('Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű').setStyle(ButtonStyle.Danger),
      );
      try {
        if (s.controlMsgId) {
          const m = await (interaction.channel as any).messages.fetch(s.controlMsgId).catch(()=>null);
          if (m) { await m.edit({ content: contentText, components: [row] }); return; }
        }
      } catch {}
      // If missing, create new control message
      try {
        const sent = await (interaction.channel as any).send({ content: contentText, components: [row] });
        s.controlMsgId = sent.id;
      } catch {}
      return;
    }

    // Start game button (owner only): let owner choose targetSets before starting
    if (id === 'hokm-start') {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'ŘŽŘˇŘ§Ű ŘłŘąŮŘą.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (!s.ownerId || interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ Ř´ŘąŮŘš ÚŠŮŘŻ.', ephemeral: true }); return; }
      if (s.state !== 'waiting') { await interaction.reply({ content: 'Ř§ŘŞŘ§Ů ŘŻŘą ŮŘśŘšŰŘŞ Ř´ŘąŮŘš ŮŰŘłŘŞ.', ephemeral: true }); return; }
      if (s.team1.length !== 2 || s.team2.length !== 2) { await interaction.reply({ content: 'ŮŘą ŘŻŮ ŘŞŰŮ Ř¨Ř§ŰŘŻ Ű˛ ŮŮŘą ŘŻŘ§Ř´ŘŞŮ Ř¨Ř§Ř´ŮŘŻ.', ephemeral: true }); return; }
      // show ephemeral config for targetSets selection
      const current = s.targetSets ?? 1;
      const rowSets1 = new ActionRowBuilder<ButtonBuilder>();
      for (let n=1;n<=4;n++) rowSets1.addComponents(new ButtonBuilder().setCustomId(`hokm-sets-${n}`).setLabel(String(n)).setStyle(current===n?ButtonStyle.Primary:ButtonStyle.Secondary));
      const rowSets2 = new ActionRowBuilder<ButtonBuilder>();
      for (let n=5;n<=7;n++) rowSets2.addComponents(new ButtonBuilder().setCustomId(`hokm-sets-${n}`).setLabel(String(n)).setStyle(current===n?ButtonStyle.Primary:ButtonStyle.Secondary));
      const rowGo = new ActionRowBuilder<ButtonBuilder>().addComponents(new ButtonBuilder().setCustomId('hokm-start-go').setLabel('Ř´ŘąŮŘš').setStyle(ButtonStyle.Danger));
      await interaction.reply({ content: `ŘŞŘšŘŻŘ§ŘŻ ŘłŘŞâŮŘ§Ű ŮŘ§Ř˛Ů Ř¨ŘąŘ§Ű Ř¨ŘąŘŻ ÚŠŘ§ŮŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮ (ŮžŰŘ´âŮŘąŘś: ${current}). ŘłŮžŘł ÂŤŘ´ŘąŮŘšÂť ŘąŘ§ Ř¨Ř˛Ů.`, components: [rowSets1, rowSets2, rowGo], ephemeral: true });
      return;
    }

    // Sets selection buttons
    if (id.startsWith('hokm-sets-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'ŘŽŘˇŘ§Ű ŘłŘąŮŘą.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (!s.ownerId || interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ ŘŞŮŘ¸ŰŮ ÚŠŮŘŻ.', ephemeral: true }); return; }
      const n = parseInt(id.split('hokm-sets-')[1], 10);
      if (!(n>=1 && n<=7)) { await interaction.reply({ content: 'Ř¨ŰŮ 1 ŘŞŘ§ 7 Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮ.', ephemeral: true }); return; }
      s.targetSets = n;
      // re-render ephemeral rows with active selection
      const rowSets1 = new ActionRowBuilder<ButtonBuilder>();
      for (let k=1;k<=4;k++) rowSets1.addComponents(new ButtonBuilder().setCustomId(`hokm-sets-${k}`).setLabel(String(k)).setStyle(n===k?ButtonStyle.Primary:ButtonStyle.Secondary));
      const rowSets2 = new ActionRowBuilder<ButtonBuilder>();
      for (let k=5;k<=7;k++) rowSets2.addComponents(new ButtonBuilder().setCustomId(`hokm-sets-${k}`).setLabel(String(k)).setStyle(n===k?ButtonStyle.Primary:ButtonStyle.Secondary));
      const rowGo = new ActionRowBuilder<ButtonBuilder>().addComponents(new ButtonBuilder().setCustomId('hokm-start-go').setLabel('Ř´ŘąŮŘš').setStyle(ButtonStyle.Danger));
      await interaction.update({ content: `ŘŞŘšŘŻŘ§ŘŻ ŘłŘŞâŮŘ§: ${n}. Ř¨ŘąŘ§Ű Ř´ŘąŮŘš ÂŤŘ´ŘąŮŘšÂť ŘąŘ§ Ř¨Ř˛Ů.`, components: [rowSets1, rowSets2, rowGo] });
      return;
    }

    // Start after sets selection
    if (id === 'hokm-start-go') {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'ŘŽŘˇŘ§Ű ŘłŘąŮŘą.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (!s.ownerId || interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ Ř´ŘąŮŘš ÚŠŮŘŻ.', ephemeral: true }); return; }
      if (s.state !== 'waiting') { await interaction.reply({ content: 'Ř§ŘŞŘ§Ů ŘŻŘą ŮŘśŘšŰŘŞ Ř´ŘąŮŘš ŮŰŘłŘŞ.', ephemeral: true }); return; }
      if (s.team1.length !== 2 || s.team2.length !== 2) { await interaction.reply({ content: 'ŮŘą ŘŻŮ ŘŞŰŮ Ř¨Ř§ŰŘŻ Ű˛ ŮŮŘą ŘŻŘ§Ř´ŘŞŮ Ř¨Ř§Ř´ŮŘŻ.', ephemeral: true }); return; }
      s.targetSets = s.targetSets ?? 1;
      s.targetTricks = s.targetTricks ?? 7;
      s.setsTeam1 = 0; s.setsTeam2 = 0;
      s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
      s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
      s.deck = shuffle(makeDeck());
      s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
      const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
      give(s.hakim, 5);
      s.state = 'choosing_hokm';
      try { const user = await interaction.client.users.fetch(s.hakim); await user.send({ content: `ŘłŘŞ ŘŹŘŻŰŘŻ Ř´ŘąŮŘš Ř´ŘŻ. ŘŻŘłŘŞ Ř§ŮŮŰŮ Ř´ŮŘ§ (Űľ ÚŠŘ§ŘąŘŞ):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
      try { const chAny = interaction.channel as any; if (chAny && chAny.send) { await chAny.send({ content: `ŘłŘŞ ŘŹŘŻŰŘŻ Ř˘ŘşŘ§Ř˛ Ř´ŘŻ. Ř­Ř§ÚŠŮ: <@${s.hakim}> â ŮŘˇŮŘ§Ů Ř­ÚŠŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮ.` }); } } catch {}
      if (interaction.guild) await refreshTableEmbed({ channel: interaction.channel as any }, s);
      await refreshAllDMs({ client: interaction.client }, s);
      return;
    }

    // Suit choice buttons
    if (id.startsWith('hokm-choose-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'ŘŽŘˇŘ§Ű ŘłŘąŮŘą.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (s.state !== 'choosing_hokm' || !s.hakim) { await interaction.reply({ content: 'Ř§ŮŘ§Ů ŮŮŘŞ Ř§ŮŘŞŘŽŘ§Ř¨ Ř­ÚŠŮ ŮŰŘłŘŞ.', ephemeral: true }); return; }
      if (interaction.user.id !== s.hakim) { await interaction.reply({ content: 'ŮŮŘˇ Ř­Ř§ÚŠŮ ŮŰâŘŞŮŘ§ŮŘŻ Ř­ÚŠŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮŘŻ.', ephemeral: true }); return; }
      const suitKey = id.split('hokm-choose-')[1] as Suit;
      const suit: Suit | undefined = (['S','H','D','C'] as Suit[]).find(x=>x===suitKey);
      if (!suit) { await interaction.reply({ content: 'ŘŽŘ§Ů ŮŘ§ŮŘšŘŞŘ¨Řą.', ephemeral: true }); return; }
      s.hokm = suit;
      try { addHokmPick(s.guildId, s.hakim!, suit); saveHokmStats(); } catch {}
      // deal remaining to all to reach 13
      const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
      for (const uid of s.order) {
        const need = 13 - (s.hands.get(uid)?.length || 0);
        give(uid, need);
      }
      // init phase2
      s.state = 'playing';
      s.leaderIndex = s.order.indexOf(s.hakim); if (s.leaderIndex < 0) s.leaderIndex = 0;
      s.turnIndex = s.leaderIndex; s.table = []; s.leadSuit = null; s.tricksTeam1 = 0; s.tricksTeam2 = 0;
      s.tricksByPlayer = new Map(); s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
      // update or create table message
      const tableEmbed = new EmbedBuilder().setTitle('Hokm â ŮŰŘ˛ Ř¨Ř§Ř˛Ű')
        .setDescription(`Ř­ÚŠŮ: ${SUIT_EMOJI[s.hokm]} â ŮŮŘ¨ŘŞ: <@${s.order[s.turnIndex]}>`);
      try {
        if (s.tableMsgId) {
          const m = await (interaction.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
          if (m) await m.edit({ embeds: [tableEmbed] });
        }
      } catch {}
      await refreshTableEmbed({ channel: interaction.channel }, s);
      // no per-player channel hand messages; users open hand ephemerally via table button
      await interaction.reply({ content: `Ř­ÚŠŮ Ř§ŮŘŞŘŽŘ§Ř¨ Ř´ŘŻ: ${SUIT_EMOJI[s.hokm]}. Ř¨Ř§Ř˛Ű Ř´ŘąŮŘš Ř´ŘŻ. Ř¨ŘąŘ§Ű ŘŻŰŘŻŮ ŘŻŘłŘŞ ŘŽŮŘŻŘ ŘąŮŰ ŘŻÚŠŮŮ "ŘŻŘłŘŞ ŮŮ" Ř˛ŰŘą ŮŰŘ˛ Ř¨Ř˛Ů.`, ephemeral: true });
      return;
    }

    // Open Hand button (ephemeral per-user hand in channel)
    if (id.startsWith('hokm-open-hand-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'ŘŽŘˇŘ§Ű ŘłŘąŮŘą.', ephemeral: true }); return; }
      const parts = id.split('-'); // hokm-open-hand-gId-cId
      const gId = parts[3]; const cId = parts[4];
      const s = ensureSession(gId, cId);
      const uid = interaction.user.id;
      const hand = s.hands.get(uid) || [];
      const rows = buildHandRowsSimple(hand, uid, s.guildId, s.channelId);
      const content = `Ř­ÚŠŮ: ${s.hokm?SUIT_EMOJI[s.hokm]:''} â ${uid===s.order[s.turnIndex??0]?'ŮŮŘ¨ŘŞ Ř´ŮŘ§ŘłŘŞ.':'ŮŮŘŞŘ¸Řą ŮŮŘ¨ŘŞ Ř¨ŮŘ§ŮŰŘŻ.'}`;
      await interaction.reply({ content, components: rows, ephemeral: true });
      return;
    }

    // DM hand filter buttons
    if (id.startsWith('hokm-hand-filter-')) {
      const parts = id.split('-'); // hokm-hand-filter-gId-cId-uid-FL
      const gId = parts[3]; const cId = parts[4]; const uid = parts[5]; const fl = parts[6] as any;
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'Ř§ŰŮ ŘŻÚŠŮŮ Ř¨ŘąŘ§Ű ŘŻŘłŘŞ Ř´ŮŘ§ ŮŰŘłŘŞ.', ephemeral: true }); return; }
      const key = `__hokm_dm_state_${gId}:${cId}:${uid}`;
      (global as any)[key] = { filter: fl, page: 0 };
      const s = ensureSession(gId, cId);
      if (interaction.guild) {
        const { rows, meta } = buildHandButtons(s, uid, { filter: fl, page: 0 });
        (global as any)[key] = { filter: meta.filter, page: meta.page };
        const content = `Ř­ÚŠŮ: ${s.hokm?SUIT_EMOJI[s.hokm]:''} â ${uid===s.order[s.turnIndex??0]?'ŮŮŘ¨ŘŞ Ř´ŮŘ§ŘłŘŞ.':'ŮŮŘŞŘ¸Řą ŮŮŘ¨ŘŞ Ř¨ŮŘ§ŮŰŘŻ.'}\nŘŻŘłŘŞ Ř´ŮŘ§:\n${handToString(s.hands.get(uid) || [])}`;
        await interaction.update({ content, components: rows });
      } else {
        await refreshPlayerDM({ client: interaction.client as Client }, s, uid);
        await interaction.deferUpdate();
      }
      return;
    }
    // DM hand pagination buttons
    if (id.startsWith('hokm-hand-page-')) {
      const parts = id.split('-'); // hokm-hand-page-gId-cId-uid-page
      const gId = parts[3]; const cId = parts[4]; const uid = parts[5]; const page = parseInt(parts[6], 10) || 0;
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'Ř§ŰŮ ŘŻÚŠŮŮ Ř¨ŘąŘ§Ű ŘŻŘłŘŞ Ř´ŮŘ§ ŮŰŘłŘŞ.', ephemeral: true }); return; }
      const key = `__hokm_dm_state_${gId}:${cId}:${uid}`;
      const prev = (global as any)[key] || { filter: 'ALL', page: 0 };
      (global as any)[key] = { filter: prev.filter || 'ALL', page };
      const s = ensureSession(gId, cId);
      if (interaction.guild) {
        const { rows, meta } = buildHandButtons(s, uid, { filter: (prev.filter||'ALL') as any, page });
        (global as any)[key] = { filter: meta.filter, page: meta.page };
        const content = `Ř­ÚŠŮ: ${s.hokm?SUIT_EMOJI[s.hokm]:''} â ${uid===s.order[s.turnIndex??0]?'ŮŮŘ¨ŘŞ Ř´ŮŘ§ŘłŘŞ.':'ŮŮŘŞŘ¸Řą ŮŮŘ¨ŘŞ Ř¨ŮŘ§ŮŰŘŻ.'}\nŘŻŘłŘŞ Ř´ŮŘ§:\n${handToString(s.hands.get(uid) || [])}`;
        await interaction.update({ content, components: rows });
      } else {
        await refreshPlayerDM({ client: interaction.client as Client }, s, uid);
        await interaction.deferUpdate();
      }
      return;
    }

    // Play card button: supports both DM (with gId/cId) and channel (short)
    if (id.startsWith('hokm-play-')) {
      const parts = id.split('-');
      let gId = interaction.guild?.id || '';
      let cId = ((interaction.channel as any)?.id as string) || '';
      let uid = '';
      let suit: Suit; let rank: number;
      if (parts.length === 7) {
        // hokm-play-gId-cId-uid-suit-rank
        gId = parts[2]; cId = parts[3]; uid = parts[4]; suit = parts[5] as Suit; rank = parseInt(parts[6], 10);
      } else {
        // hokm-play-uid-suit-rank (clicked in channel)
        uid = parts[2]; suit = parts[3] as Suit; rank = parseInt(parts[4], 10);
        const chAny = interaction.channel as any;
        if (chAny?.isThread && chAny.parentId) { cId = chAny.parentId; }
        if (!gId && chAny?.guildId) { gId = chAny.guildId; }
      }
      if (!gId || !cId) { await interaction.reply({ content: 'ŘŽŘˇŘ§Ű ÚŠŘ§ŮŘ§Ů Ř¨Ř§Ř˛Ű.', ephemeral: true }); return; }
      const s = ensureSession(gId, cId);
      if (s.state !== 'playing' || s.turnIndex==null) { await interaction.reply({ content: 'Ř¨Ř§Ř˛Ű ŘŻŘą ŘŹŘąŰŘ§Ů ŮŰŘłŘŞ.', ephemeral: true }); return; }
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'Ř§ŰŮ ŘŻÚŠŮŮ Ř¨ŘąŘ§Ű ŘŻŘłŘŞ Ř´ŮŘ§ ŮŰŘłŘŞ.', ephemeral: true }); return; }
      if (s.order[s.turnIndex] !== uid) { await interaction.reply({ content: 'Ř§ŮŘ§Ů ŮŮŘ¨ŘŞ Ř´ŮŘ§ ŮŰŘłŘŞ.', ephemeral: true }); return; }
      const hand = s.hands.get(uid) || [];
      const card: Card = { s: suit, r: rank };
      const idx = hand.findIndex(c=>sameCard(c, card));
      if (idx === -1) { await interaction.reply({ content: 'Ř§ŰŮ ÚŠŘ§ŘąŘŞ ŘŻŘą ŘŻŘłŘŞ Ř´ŮŘ§ ŮŰŘłŘŞ.', ephemeral: true }); return; }
      // follow-suit
      if (!s.table || s.table.length === 0) {
        s.leadSuit = card.s;
      } else {
        const lead = s.leadSuit!;
        const hasLead = hand.some(c=>c.s===lead);
        if (hasLead && card.s !== lead) { await interaction.reply({ content: `Ř¨Ř§ŰŘŻ ŘŽŘ§Ů Ř´ŘąŮŘš (${SUIT_EMOJI[lead]}) ŘąŘ§ ŘŻŮŘ¨Ř§Ů ÚŠŮŰŘŻ.`, ephemeral: true }); return; }
      }
      // play
      hand.splice(idx,1); s.hands.set(uid, hand);
      s.table = s.table || []; s.table.push({ userId: uid, card });
      s.turnIndex = (s.turnIndex + 1) % s.order.length;
      // update the ephemeral hand panel dynamically
      {
        const rows = buildHandRowsSimple(hand, uid, s.guildId, s.channelId);
        const content = `Ř­ÚŠŮ: ${s.hokm?SUIT_EMOJI[s.hokm]:''} â ${uid===s.order[s.turnIndex??0]?'ŮŮŘ¨ŘŞ Ř´ŮŘ§ŘłŘŞ.':'ŮŮŘŞŘ¸Řą ŮŮŘ¨ŘŞ Ř¨ŮŘ§ŮŰŘŻ.'}`;
        try { await interaction.update({ content, components: rows }); } catch { await interaction.reply({ content, components: rows, ephemeral: true }); }
      }
      // update table only (hands are private via ephemeral)
      try {
        const ch = await interaction.client.channels.fetch(cId).catch(()=>null) as any;
        if (ch) await refreshTableEmbed({ channel: ch }, s);
      } catch {}
      
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
  const isCmd = (name: string) => new RegExp(`^\\.${name}(?:\\s|$)`).test(content);
  const isSubCmd = (head: string, tail: string) => new RegExp(`^\\.${head}\\s+${tail}(?:\\s|$)`).test(content);

  // .friend [@user|userId]
  if (isCmd('friend')) {
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
      await msg.reply({ content: 'ŘŻŘ§ŘŻŮâŘ§Ű Ř¨ŘąŘ§Ű Ř§ŰŮ ÚŠŘ§ŘąŘ¨Řą ŰŘ§ŮŘŞ ŮŘ´ŘŻ.' });
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
      await msg.reply({ content: 'ŮŰÚ ŘŻŮŘłŘŞ ŘşŰŘą Ř¨Ř§ŘŞŰ ŮžŰŘŻŘ§ ŮŘ´ŘŻ.' });
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
      lines.push(`${i + 1}. ${mention} â ${fmt(ms)}`);
    });
    const embed = new EmbedBuilder()
      .setTitle('ŘŻŮŘłŘŞŘ§Ů')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .best â top 20 Hokm winners (by wins)
  if (isCmd('best')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const gId = msg.guildId!;
    const stats = hokmStats.get(gId);
    if (!stats || stats.size === 0) { await msg.reply({ content: 'ŘŻŘą Ř§ŰŮ ŘłŘąŮŘą Ř¨Ř§Ř˛Ű Ř§ŮŘŹŘ§Ů ŮŘ´ŘŻŮ Ř§ŘłŘŞ.' }); return; }
    const entries = Array.from(stats.entries()) as Array<[string, HokmUserStat]>;
    const arr = entries
      .filter(([, st]) => ((st?.games)||0) > 0)
      .sort((a: [string, HokmUserStat], b: [string, HokmUserStat]) => ((b[1].wins||0) - (a[1].wins||0)) || ((b[1].games||0) - (a[1].games||0)))
      .slice(0, 20);
    if (arr.length === 0) { await msg.reply({ content: 'ŘŻŘą Ř§ŰŮ ŘłŘąŮŘą Ř¨Ř§Ř˛Ű Ř§ŮŘŹŘ§Ů ŮŘ´ŘŻŮ Ř§ŘłŘŞ.' }); return; }
    const server = msg.guild.name;
    const lines: string[] = [];
    lines.push(`âľ ${server} ŮŰŘłŘŞ Ř¨ŘąŮŘŻÚŻŘ§Ů:`);
    lines.push('ââŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâ');
    let idx = 0;
    for (const [uid, st] of arr) {
      idx++;
      const rank = String(idx).padStart(2, '0');
      lines.push(`âĄ ${rank} - <@${uid}> âśď¸Ř¨Ř§Ř˛ŰâŮŘ§: ${st.games||0} đŤŘ¨ŘąŘŻ: ${st.wins||0}`);
    }
    lines.push('ââŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâ');
    const embedBest = new EmbedBuilder().setDescription(lines.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embedBest] });
    return;
  }

  // .bazikon â show user's Hokm stats
  if (isCmd('bazikon')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const gId = msg.guildId!;
    const targetIds = await resolveTargetIds(msg, content, '.bazikon');
    const targetId = targetIds[0] || msg.author.id;
    const stMap = hokmStats.get(gId);
    const st: HokmUserStat = stMap?.get(targetId) || { games: 0, wins: 0, teammateWins: {}, hokmPicks: {} };
    if (!st.games) { await msg.reply({ content: 'Ř§ŰŮ ÚŠŘ§ŘąŘ¨Řą Ř¨Ř§Ř˛Ű Ř§ŮŘŹŘ§Ů ŮŘŻŘ§ŘŻŮ Ř§ŘłŘŞ.' }); return; }
    let bestMate: string | null = null; let bestWins = 0;
    for (const [uid, w] of Object.entries((st.teammateWins||{}) as Record<string, number>)) {
      const val = Number(w)||0;
      if (val > bestWins) { bestWins = val; bestMate = uid; }
    }
    const mateText = bestMate ? `<@${bestMate}> (${bestWins} WIN)` : 'â';
    const picks = (st.hokmPicks || {}) as Partial<Record<Suit, number>>;
    const suitOrder: Suit[] = ['C','S','D','H'];
    const sortedSuits = suitOrder.sort((a,b)=> (picks[b]||0) - (picks[a]||0));
    const favArray = sortedSuits.filter(su => (picks[su]||0) > 0).map(su => SUIT_EMOJI[su as Suit].replace('ď¸',''));
    const favText = `[${favArray.join(',')}]`;
    const lines: string[] = [];
    lines.push(`âľ <@${targetId}> Ř˘ŮŘ§Řą:`);
    lines.push('ââŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâ');
    lines.push(`âśď¸Ř¨Ř§Ř˛ŰâŮŘ§: ${st.games||0}`);
    lines.push(`đŤŘ¨ŘąŘŻ: ${st.wins||0}`);
    lines.push(`âĽď¸Ř¨ŮŘŞŘąŰŮ ŮŮŘŞŰŮŰ: ${mateText}`);
    lines.push(`đĄ Ř­ÚŠŮ ŮŘ­Ř¨ŮŘ¨: ${favText}`);
    lines.push('ââŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâŹâ');
    const embedBaz = new EmbedBuilder().setDescription(lines.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embedBaz] });
    return;
  }

  // .topfriend â list top 10 pairs with most co-voice time (exclude bots)
  if (isCmd('topfriend')) {
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
      for (const [key, start] of pMap.entries()) {
        const parts = key.split(':');
        if (parts.length < 3) continue;
        const [a, b] = [parts[0], parts[1]];
        const [x, y] = a < b ? [a, b] : [b, a];
        const k2 = `${x}:${y}`;
        const cur = agg.get(k2) || { a: x, b: y, ms: 0 };
        const delta = now - start;
        if (delta > 0) cur.ms += delta;
        agg.set(k2, cur);
      }
    }

    // Nothing to report
    if (agg.size === 0) {
      await msg.reply({ content: 'ŮŰÚ Ř˛ŮŘŹŰ ŰŘ§ŮŘŞ ŮŘ´ŘŻ.' });
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
      lines.push(`${lines.length + 1}. <@${p.a}> + <@${p.b}> â ${fmt(p.ms)}`);
    }

    if (lines.length === 0) {
      await msg.reply({ content: 'ŮŰÚ Ř˛ŮŘŹ ŘşŰŘą Ř¨Ř§ŘŞŰ ŰŘ§ŮŘŞ ŮŘ´ŘŻ.' });
      return;
    }

    const embed = new EmbedBuilder()
      .setTitle('Ř¨ŮŘŞŘąŰŮ ŘŻŮŘłŘŞŘ§Ů')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .new â create room with join buttons
  if (isCmd('new')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    // reset session
    s.team1 = []; s.team2 = []; s.order = []; s.hakim = undefined; s.hokm = undefined; s.deck = []; s.hands.clear(); s.state = 'waiting'; s.ownerId = msg.author.id; s.tableMsgId = undefined;
    const contentText = controlListText(s);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('ŘŞŰŮ 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('ŘŞŰŮ 2').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('hokm-leave').setLabel('ŘŽŘąŮŘŹ').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('hokm-start').setLabel('Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű').setStyle(ButtonStyle.Danger),
    );
    const sent = await msg.reply({ content: contentText, components: [row] });
    s.controlMsgId = sent.id;
    return;
  }

  // .a1 @user â owner assigns user to Team 1
  if (isCmd('a1')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('ŮŮŘˇ ŮŘ¨Ů Ř§Ř˛ Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű ŮŘ§Ř¨Ů Ř§ŮŘŹŘ§Ů Ř§ŘłŘŞ.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ Ř§ŘšŘśŘ§ ŘąŘ§ Ř§ŘśŘ§ŮŮ ÚŠŮŘŻ.'); return; }
    const targets = await resolveTargetIds(msg, content, '.a1');
    if (targets.length === 0) { await msg.reply('Ř§ŘłŘŞŮŘ§ŘŻŮ: `.a1 @user1 @user2` ŰŘ§ ŘąŰŮžŮŘ§Ű/Ř˘ŰŘŻŰ'); return; }
    const added: string[] = []; const skipped: string[] = [];
    for (const uid of targets) {
      try { const u = await msg.client.users.fetch(uid); if (u.bot) { skipped.push(`<@${uid}> (bot)`); continue; } } catch { skipped.push(`<@${uid}> (ŮŘ§ŮŘšŘŞŘ¨Řą)`); continue; }
      if (s.team1.includes(uid)) { skipped.push(`<@${uid}> (ŮŘ¨ŮŘ§Ů ŘŞŰŮ 1)`); continue; }
      s.team1 = s.team1.filter(x=>x!==uid); s.team2 = s.team2.filter(x=>x!==uid);
      if (s.team1.length >= 2) { skipped.push(`<@${uid}> (ŘŞŰŮ 1 ŮžŘą Ř§ŘłŘŞ)`); continue; }
      s.team1.push(uid); added.push(`<@${uid}>`);
    }
    const contentText = controlListText(s);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('ŘŞŰŮ 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('ŘŞŰŮ 2').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('hokm-leave').setLabel('ŘŽŘąŮŘŹ').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('hokm-start').setLabel('Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű').setStyle(ButtonStyle.Danger),
    );
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: [row] }); } } catch {}
    await msg.reply({ content: `Ř§ŮŘ˛ŮŘŻŮ Ř´ŘŻ: ${added.join(' , ') || 'â'}` });
    return;
  }

  // .a2 @user â owner assigns user to Team 2
  if (isCmd('a2')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('ŮŮŘˇ ŮŘ¨Ů Ř§Ř˛ Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű ŮŘ§Ř¨Ů Ř§ŮŘŹŘ§Ů Ř§ŘłŘŞ.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ Ř§ŘšŘśŘ§ ŘąŘ§ Ř§ŘśŘ§ŮŮ ÚŠŮŘŻ.'); return; }
    const targets = await resolveTargetIds(msg, content, '.a2');
    if (targets.length === 0) { await msg.reply('Ř§ŘłŘŞŮŘ§ŘŻŮ: `.a2 @user1 @user2` ŰŘ§ ŘąŰŮžŮŘ§Ű/Ř˘ŰŘŻŰ'); return; }
    const added: string[] = []; const skipped: string[] = [];
    for (const uid of targets) {
      try { const u = await msg.client.users.fetch(uid); if (u.bot) { skipped.push(`<@${uid}> (bot)`); continue; } } catch { skipped.push(`<@${uid}> (ŮŘ§ŮŘšŘŞŘ¨Řą)`); continue; }
      if (s.team2.includes(uid)) { skipped.push(`<@${uid}> (ŮŘ¨ŮŘ§Ů ŘŞŰŮ 2)`); continue; }
      s.team1 = s.team1.filter(x=>x!==uid); s.team2 = s.team2.filter(x=>x!==uid);
      if (s.team2.length >= 2) { skipped.push(`<@${uid}> (ŘŞŰŮ 2 ŮžŘą Ř§ŘłŘŞ)`); continue; }
      s.team2.push(uid); added.push(`<@${uid}>`);
    }
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('ŘŞŰŮ 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('ŘŞŰŮ 2').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('hokm-leave').setLabel('ŘŽŘąŮŘŹ').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('hokm-start').setLabel('Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű').setStyle(ButtonStyle.Danger),
    );
    const contentText = controlListText(s);
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: [row] }); } } catch {}
    {
      const lines: string[] = [];
      lines.push(`Ř§ŮŘ˛ŮŘŻŮ Ř´ŘŻ: ${added.join(' , ') || 'â'}`);
      if (skipped.length > 0) lines.push(`ŮŘ§ŘŻŰŘŻŮ: ${skipped.join(' , ')}`);
      await msg.reply({ content: lines.join('\n') });
    }
    return;
  }

  // .r â owner removes a user from teams
  if (isCmd('r')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('ŮŮŘˇ ŮŘ¨Ů Ř§Ř˛ Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű ŮŘ§Ř¨Ů Ř§ŮŘŹŘ§Ů Ř§ŘłŘŞ.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ Ř§ŘšŘśŘ§ ŘąŘ§ Ř­Ř°Ů ÚŠŮŘŻ.'); return; }
    const targets = await resolveTargetIds(msg, content, '.r');
    if (targets.length === 0) { await msg.reply('Ř§ŘłŘŞŮŘ§ŘŻŮ: `.r @user1 @user2` ŰŘ§ ŘąŰŮžŮŘ§Ű/Ř˘ŰŘŻŰ'); return; }
    const removed: string[] = []; const notIn: string[] = [];
    for (const uid of targets) {
      const inAny = s.team1.includes(uid) || s.team2.includes(uid);
      s.team1 = s.team1.filter(x=>x!==uid);
      s.team2 = s.team2.filter(x=>x!==uid);
      if (inAny) removed.push(`<@${uid}>`); else notIn.push(`<@${uid}>`);
    }
    const contentText = controlListText(s);
    const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('ŘŞŰŮ 1').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('ŘŞŰŮ 2').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('hokm-leave').setLabel('ŘŽŘąŮŘŹ').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('hokm-start').setLabel('Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű').setStyle(ButtonStyle.Danger),
    );
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: [row] }); } } catch {}
    {
      const lines: string[] = [];
      lines.push(`Ř­Ř°Ů Ř´ŘŻ: ${removed.join(' , ') || 'â'}`);
      if (notIn.length > 0) lines.push(`ŮŘ§ŮŮŘŹŮŘŻ: ${notIn.join(' , ')}`);
      await msg.reply({ content: lines.join('\n') });
    }
    return;
  }

  // .end â owner ends the room and deletes control/table messages
  if (isCmd('end')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (!s.ownerId || msg.author.id !== s.ownerId) { await msg.reply('ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ ŮžŘ§ŰŘ§Ů ŘŻŮŘŻ.'); return; }
    // delete control and table messages if exist
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.delete().catch(()=>{}); } } catch {}
    try { if (s.tableMsgId) { const m2 = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null); if (m2) await m2.delete().catch(()=>{}); } } catch {}
    // clear session
    s.team1 = []; s.team2 = []; s.order = []; s.hakim = undefined; s.hokm = undefined; s.deck = []; s.hands.clear(); s.state = 'finished'; s.controlMsgId = undefined; s.tableMsgId = undefined;
    await msg.reply('Ř§ŘŞŘ§Ů ŮžŘ§ŰŘ§Ů ŰŘ§ŮŘŞ.');
    return;
  }

  // .reset â owner resets the room and redeals (like fresh start with current teams)
  if (isCmd('reset')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (!s.ownerId || msg.author.id !== s.ownerId) { await msg.reply('ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ ŘąŰŘłŘŞ ÚŠŮŘŻ.'); return; }
    if (s.team1.length !== 2 || s.team2.length !== 2) { await msg.reply('Ř¨ŘąŘ§Ű ŘąŰŘłŘŞŘ ŮŘą ŘŻŮ ŘŞŰŮ Ř¨Ř§ŰŘŻ Ű˛ ŮŮŘą ŘŻŘ§Ř´ŘŞŮ Ř¨Ř§Ř´ŮŘŻ.'); return; }
    // reinitialize game state
    s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
    s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.hokm = undefined; s.tableMsgId = undefined;
    s.state = 'choosing_hokm';
    try { const user = await msg.client.users.fetch(s.hakim); await user.send({ content: `Ř¨Ř§Ř˛Ű ŘąŰŘłŘŞ Ř´ŘŻ. ŘŻŘłŘŞ Ř§ŮŮŰŮ Ř´ŮŘ§ (Űľ ÚŠŘ§ŘąŘŞ):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    // update control list if exists
    if (s.controlMsgId) {
      const contentText = controlListText(s);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('ŘŞŰŮ 1').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('ŘŞŰŮ 2').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId('hokm-leave').setLabel('ŘŽŘąŮŘŹ').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('hokm-start').setLabel('Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű').setStyle(ButtonStyle.Danger),
      );
      try { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: [row] }); } catch {}
    }
    await msg.reply({ content: `ŘąŰŘłŘŞ Ř´ŘŻ. Ř­Ř§ÚŠŮ: <@${s.hakim}> â ŮŘˇŮŘ§Ů Ř¨Ř§ ".hokm hokm <ŘŽŘ§Ů>" Ř­ÚŠŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮ.` });
    return;
  }

  // .list â recreate control list if waiting; otherwise re-render table
  if (isCmd('list')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state === 'waiting') {
      // delete previous control message if exists
      if (s.controlMsgId) {
        try { const prev = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (prev) await prev.delete().catch(()=>{}); } catch {}
        s.controlMsgId = undefined;
      }
      const contentText = controlListText(s);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('ŘŞŰŮ 1').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('ŘŞŰŮ 2').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId('hokm-leave').setLabel('ŘŽŘąŮŘŹ').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('hokm-start').setLabel('Ř´ŘąŮŘš Ř¨Ř§Ř˛Ű').setStyle(ButtonStyle.Danger),
      );
      const sent = await msg.reply({ content: contentText, components: [row] });
      s.controlMsgId = sent.id;
    } else {
      try { await refreshTableEmbed({ channel: msg.channel }, s); } catch {}
    }
    return;
  }

  // .miz â ŮžŘ§ÚŠâŘłŘ§Ř˛Ű ŮžŰŘ§Ů ŮŰŘ˛ ŮŘšŮŰ Ů ŮŮŘ§ŰŘ´ ŘŻŮŘ¨Ř§ŘąŮ ŮŰŘ˛ ŘŻŘą ÚŮŮ
  if (isCmd('miz')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.tableMsgId) {
      try {
        const prev = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
        if (prev) await prev.delete().catch(()=>{});
      } catch {}
      s.tableMsgId = undefined;
    }
    try { await refreshTableEmbed({ channel: msg.channel }, s); } catch {}
    return;
  }

  // .hokm start â start game; optional N sets to win match
  if (isSubCmd('hokm','start')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('ŮŮŘˇ ŘłŘ§Ř˛ŮŘŻŮ Ř§ŘŞŘ§Ů ŮŰâŘŞŮŘ§ŮŘŻ Ř¨Ř§Ř˛Ű ŘąŘ§ Ř´ŘąŮŘš ÚŠŮŘŻ.'); return; }
    if (s.state !== 'waiting') { await msg.reply('Ř§ŘŞŘ§Ů ŘŻŘą ŮŘśŘšŰŘŞ Ř´ŘąŮŘš ŮŰŘłŘŞ.'); return; }
    if (s.team1.length !== 2 || s.team2.length !== 2) { await msg.reply('ŮŘą ŘŻŮ ŘŞŰŮ Ř¨Ř§ŰŘŻ Ű˛ ŮŮŘą ŘŻŘ§Ř´ŘŞŮ Ř¨Ř§Ř´ŮŘŻ.'); return; }
    // parse optional target sets (full hands)
    const m = content.match(/^\.hokm start(?:\s+(\d+))?/);
    let targetSets = 1;
    if (m && m[1]) {
      const n = parseInt(m[1], 10);
      if (Number.isNaN(n) || n < 1 || n > 7) { await msg.reply('ŘšŘŻŘŻ ŮŘšŘŞŘ¨Řą Ř¨ŰŮ 1 ŘŞŘ§ 7 ŮŘ§ŘąŘŻ ÚŠŮŰŘŻ. ŮŘŤŘ§Ů: `.hokm start 3`'); return; }
      targetSets = n;
    }
    s.targetSets = targetSets; // number of sets to win
    s.targetTricks = s.targetTricks ?? 7; // tricks to win a set (always 7)
    s.setsTeam1 = 0; s.setsTeam2 = 0;
    s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
    s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    // deal 5 to hakim
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.state = 'choosing_hokm';
    // DM hakim hand
    try { const user = await msg.client.users.fetch(s.hakim); await user.send({ content: `ŘŻŘłŘŞ Ř§ŮŮŰŮ Ř´ŮŘ§ (Űľ ÚŠŘ§ŘąŘŞ):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    // Show suit selection panel in channel
    {
      const embed = new EmbedBuilder().setTitle('Hokm â Ř§ŮŘŞŘŽŘ§Ř¨ Ř­ÚŠŮ')
        .setDescription(`ŘŞŰŮ 1: ${s.team1.map(u=>`<@${u}>`).join(' , ')}\nŘŞŰŮ 2: ${s.team2.map(u=>`<@${u}>`).join(' , ')}\nŘ­Ř§ÚŠŮ: <@${s.hakim}> â ŮŘˇŮŘ§Ů Ř­ÚŠŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮ.`)
        .setColor(0x5865F2);
      const row = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId('hokm-choose-S').setLabel('â ď¸ ŮžŰÚŠ').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId('hokm-choose-H').setLabel('âĽď¸ ŘŻŮ').setStyle(ButtonStyle.Danger),
        new ButtonBuilder().setCustomId('hokm-choose-D').setLabel('âŚď¸ ŘŽŘ´ŘŞ').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId('hokm-choose-C').setLabel('âŁď¸ ÚŻŰŘ´ŮŰŘ˛').setStyle(ButtonStyle.Success),
      );
      let msgObj = null as any;
      try {
        if (s.tableMsgId) {
          const m = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
          if (m) { await m.edit({ embeds: [embed], components: [row] }); msgObj = m; }
        }
      } catch {}
      if (!msgObj) {
        msgObj = await (msg.channel as any).send({ embeds: [embed], components: [row] });
        s.tableMsgId = msgObj.id;
      }
    }
    await msg.reply({ content: `Ř¨Ř§Ř˛Ű Ř˘ŘşŘ§Ř˛ Ř´ŘŻ. ŮŘŻŮ ŘłŘŞâŮŘ§: ${s.targetSets} (ŮŘą ŘłŘŞ = Űˇ ŘŻŘłŘŞ). Ř­Ř§ÚŠŮ: <@${s.hakim}> â Ř§Ř˛ ŘŻÚŠŮŮâŮŘ§Ű ŮŰŘ˛ Ř¨ŘąŘ§Ű Ř§ŮŘŞŘŽŘ§Ř¨ Ř­ÚŠŮ Ř§ŘłŘŞŮŘ§ŘŻŮ ÚŠŮ.` });
    return;
  }

  // .hokm hokm <suit> â hakim chooses trump; then deal remaining to all and DM hands
  if (isSubCmd('hokm','hokm')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'choosing_hokm' || !s.hakim) { await msg.reply('Ř§ŮŘ§Ů ŮŮŘŞ Ř§ŮŘŞŘŽŘ§Ř¨ Ř­ÚŠŮ ŮŰŘłŘŞ.'); return; }
    if (msg.author.id !== s.hakim) { await msg.reply('ŮŮŘˇ Ř­Ř§ÚŠŮ ŮŰâŘŞŮŘ§ŮŘŻ Ř­ÚŠŮ ŘąŘ§ Ř§ŮŘŞŘŽŘ§Ř¨ ÚŠŮŘŻ.'); return; }
    const arg = content.replace('.hokm hokm', '').trim();
    const suit = parseSuit(arg);
    if (!suit) { await msg.reply('ŘŽŘ§Ů ŮŘ§ŮŘšŘŞŘ¨Řą. ÚŻŘ˛ŰŮŮâŮŘ§: â ď¸ ŮžŰÚŠŘ âĽď¸ ŘŻŮŘ âŚď¸ ŘŽŘ´ŘŞŘ âŁď¸ ÚŻŰŘ´ŮŰŘ˛'); return; }
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
    s.tricksByPlayer = new Map(); s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
    // DM all hands
    for (const uid of s.order) {
      try { const user = await msg.client.users.fetch(uid); await user.send({ content: `Ř­ÚŠŮ: ${SUIT_EMOJI[s.hokm]}\nŘŻŘłŘŞ Ř´ŮŘ§:\n${handToString(s.hands.get(uid)!)}\nŮŮŘ¨ŘŞ Ř˘ŘşŘ§Ř˛ Ř¨Ř§ Ř­Ř§ÚŠŮ <@${s.hakim}>` }); } catch {}
    }
    await msg.reply({ content: `Ř­ÚŠŮ Ř§ŮŘŞŘŽŘ§Ř¨ Ř´ŘŻ: ${SUIT_EMOJI[s.hokm]} â ŮŮŘ¨ŘŞ Ř˘ŘşŘ§Ř˛ Ř¨Ř§ Ř­Ř§ÚŠŮ <@${s.hakim}>. Ř¨Ř§ ".hokm play <ÚŠŘ§ŘąŘŞ>" Ř¨Ř§Ř˛Ű ÚŠŮŰŘŻ. ŮŘŤŘ§Ů: .hokm play A${SUIT_EMOJI['S']}` });
    return;
  }

  // .hokm hand â DM your hand
  if (isSubCmd('hokm','hand')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state === 'waiting') { await msg.reply('Ř¨Ř§Ř˛Ű Ř´ŘąŮŘš ŮŘ´ŘŻŮ Ř§ŘłŘŞ.'); return; }
    const hand = s.hands.get(msg.author.id);
    if (!hand) { await msg.reply('Ř´ŮŘ§ ŘŻŘą Ř§ŰŮ Ř¨Ř§Ř˛Ű ŮŰŘłŘŞŰŘŻ.'); return; }
    try { await msg.author.send({ content: `ŘŻŘłŘŞ Ř´ŮŘ§:\n${handToString(hand)}` }); await msg.reply({ content: 'Ř¨Ů ŮžŰŘ§ŮâŮŘ§Ű ŘŽŘľŮŘľŰâŘ§ŘŞ Ř§ŘąŘłŘ§Ů Ř´ŘŻ.' }); } catch {
      await msg.reply('Ř§ŮÚŠŘ§Ů Ř§ŘąŘłŘ§Ů ŮžŰŘ§Ů ŘŽŘľŮŘľŰ Ř¨Ů Ř´ŮŘ§ ŮŘŹŮŘŻ ŮŘŻŘ§ŘąŘŻ.');
    }
    return;
  }

  // .hokm table â show teams and current state (with table/tricks)
  if (isSubCmd('hokm','table')) {
    if (!msg.guild) { await msg.reply('ŮŮŘˇ ŘŻŘ§ŘŽŮ ŘłŘąŮŘą.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    const parts: string[] = [];
    parts.push(`ŘŞŰŮ 1: ${s.team1.map(u=>`<@${u}>`).join(' , ') || 'â'}`);
    parts.push(`ŘŞŰŮ 2: ${s.team2.map(u=>`<@${u}>`).join(' , ') || 'â'}`);
    parts.push(`Ř­Ř§ÚŠŮ: ${s.hakim?`<@${s.hakim}>`:'â'}`);
    parts.push(`Ř­ÚŠŮ: ${s.hokm?SUIT_EMOJI[s.hokm]:'â'}`);
    parts.push(`ŮŘŻŮ ŘŻŘłŘŞâŮŘ§: ${s.targetTricks ?? 7}`);
    if (s.state === 'playing') {
      parts.push(`Ř¨ŘąŘŻ ŘŻŘłŘŞâŮŘ§ â ŘŞŰŮ1: ${s.tricksTeam1 ?? 0} | ŘŞŰŮ2: ${s.tricksTeam2 ?? 0}`);
      const tableLines: string[] = [];
      if (s.table && s.table.length) {
        for (const p of s.table) tableLines.push(`<@${p.userId}>: ${cardStr(p.card)}`);
        parts.push(`ŮŰŘ˛:
${tableLines.join('\n')}`);
      } else {
        parts.push('ŮŰŘ˛: â');
      }
      const next = s.turnIndex!=null ? s.order[s.turnIndex] : undefined;
      if (next) parts.push(`ŮŮŘ¨ŘŞ: <@${next}>`);
    }
    parts.push(`ŮŘśŘšŰŘŞ: ${s.state}`);
    const embed = new EmbedBuilder().setTitle('Hokm â ŮŘśŘšŰŘŞ ŮŰŘ˛').setDescription(parts.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .komak â help
  if (isCmd('komak')) {
    const lines: string[] = [
      'â˘ .t <ŮŘŻŘŞ> [ŘŻŮŰŮ] â ŘŞŮŘ¸ŰŮ ŘŞŘ§ŰŮŘą. ŮŮŮŮŮ: `.t 10m` ŰŘ§ `.t 60 [ŘŻŮŰŮ]`',
      'â˘ .e <ŘŤŘ§ŮŰŮ> â Ř§ŮŘ˛ŮŘŻŮ ÚŮŘŻ ŘŤŘ§ŮŰŮ Ř¨Ů Ř˘ŘŽŘąŰŮ ŘŞŘ§ŰŮŘą ŘŽŮŘŻŘŞ. ŮŮŮŮŮ: `.e 30`',
      'â˘ .friend [@ÚŠŘ§ŘąŘ¨Řą|Ř˘ŰŘŻŰ] â ŮŮŘ§ŰŘ´ ŰąŰ° ŮŮŘąŮ Ř¨ŘąŘŞŘą ÚŠŮ Ř¨ŰŘ´ŘŞŘąŰŮ ŮŮâŘ­ŘśŮŘąŰ ŮŰŘł Ř¨Ř§ ÚŠŘ§ŘąŘ¨Řą ŮŘŻŮ ŘąŘ§ ŘŻŘ§Ř´ŘŞŮâŘ§ŮŘŻ (Ř¨ŘŻŮŮ ŘąŘ¨Ř§ŘŞâŮŘ§).',
      'â˘ .topfriend â ŮŮŘ§ŰŘ´ ŰąŰ° Ř˛ŮŘŹ Ř¨ŘąŘŞŘą Ř¨Ř§ Ř¨ŰŘ´ŘŞŘąŰŮ ŮŮâŘ­ŘśŮŘąŰ ŘŻŘą ŮŰŘł (Ř¨ŘŻŮŮ ŘąŘ¨Ř§ŘŞâŮŘ§).',
      'â˘ .ll [@ÚŠŘ§ŘąŘ¨Řą|Ř˘ŰŘŻŰ] â ŮŘ­Ř§ŘłŘ¨Ů Ů ŘłŘ§ŘŽŘŞ ŘŞŘľŮŰŘą ŘŻŘąŘľŘŻ ŘšŘ´Ů Ř¨ŰŮ Ř´ŮŘ§ Ů ÚŠŘ§ŘąŘ¨Řą ŮŘŻŮ.',
      'â˘ .llset @user1 @user2 <0..100> â ŮŮŘˇ ŮŘŻŰŘąŘ§Ů: ŘŞŮŘ¸ŰŮ ŘŻŘąŘľŘŻ ŘŤŘ§Ř¨ŘŞ ŘšŘ´Ů Ř¨ŘąŘ§Ű ŘŻŮ ÚŠŘ§ŘąŘ¨Řą.',
      'â˘ .llunset @user1 @user2 â ŮŮŘˇ ŮŘŻŰŘąŘ§Ů: Ř­Ř°Ů ŘŞŮŘ¸ŰŮ ŘŤŘ§Ř¨ŘŞ ŘŻŘąŘľŘŻ ŘšŘ´Ů.',
      'â˘ .av [@ÚŠŘ§ŘąŘ¨Řą|Ř˘ŰŘŻŰ] â ŮŮŘ§ŰŘ´ Ř˘ŮŘ§ŘŞŘ§Řą ÚŠŘ§ŘąŘ¨Řą (Ř¨Ř§ ŮŰŮÚŠ).',
      'â˘ .ba [@ÚŠŘ§ŘąŘ¨Řą|Ř˘ŰŘŻŰ] â ŮŮŘ§ŰŘ´ Ř¨ŮŘą ÚŠŘ§ŘąŘ¨Řą (Ř§ÚŻŘą ŘŻŘ§Ř´ŘŞŮ Ř¨Ř§Ř´ŘŻ).',
      'â˘ Slash: /timer set|list|cancel â ŘŞŘ§ŰŮŘą Ř¨Ř§ Ř§ŰŮŘŞŘąŮŰŘł Ř§ŘłŮŘ´âÚŠŘ§ŮŮŘŻ (ŘŤŘ¨ŘŞ Ř¨Ř§ `npm run register:commands`).',
    ];
    const embed = new EmbedBuilder()
      .setTitle('ŘąŘ§ŮŮŮŘ§Ű ŘŻŘłŘŞŮŘąŘ§ŘŞ')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .av [@user|userId]
  if (isCmd('av')) {
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

  if (isCmd('ba')) {
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
      await msg.reply({ content: 'Ř§ŰŮ ÚŠŘ§ŘąŘ¨Řą Ř¨ŮŘąŰ ŘŞŮŘ¸ŰŮ ŮÚŠŘąŘŻŮ Ř§ŘłŘŞ.' });
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


  // .llset â admin only
  if (isCmd('llset')) {
    const isAdmin = !!msg.member?.permissions.has(PermissionsBitField.Flags.Administrator);
    if (!isAdmin) {
      await msg.reply({ content: 'ŮŮŘˇ ŮŘŻŰŘąŘ§Ů ŮŰâŘŞŮŘ§ŮŮŘŻ Ř§Ř˛ Ř§ŰŮ ŘŻŘłŘŞŮŘą Ř§ŘłŘŞŮŘ§ŘŻŮ ÚŠŮŮŘŻ.' });
      return;
    }
    const arg = content.slice(6).trim();
    const parts = arg.split(/\s+/).filter(Boolean);
    if (parts.length < 3 && msg.mentions.users.size < 2) {
      await msg.reply({ content: 'Ř§ŘłŘŞŮŘ§ŘŻŮ: `.llset @user1 @user2 89` ŰŘ§ Ř¨Ř§ Ř˘ŰŘŻŰ ŘŻŮ ÚŠŘ§ŘąŘ¨Řą Ů ŘšŘŻŘŻ Ř¨ŰŮ 0 ŘŞŘ§ 100.' });
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
      await msg.reply({ content: 'ŮŘąŮŘŻŰ ŮŘ§ŮŘšŘŞŘ¨Řą. ŘšŘŻŘŻ Ř¨Ř§ŰŘŻ Ř¨ŰŮ 0 ŘŞŘ§ 100 Ř¨Ř§Ř´ŘŻ Ů ŘŻŮ ÚŠŘ§ŘąŘ¨Řą ŮŘ´ŘŽŘľ Ř´ŮŮŘŻ.' });
      return;
    }
    const gId = msg.guildId!;
    const m = loveOverrides.get(gId) || new Map<string, number>();
    m.set(loveKey(u1.id, u2.id), p);
    loveOverrides.set(gId, m);
    saveLoveOverrides();
    await msg.reply({ content: `ŘŻŘąŘľŘŻ ŘšŘ´Ů Ř¨ŰŮ <@${u1.id}> Ů <@${u2.id}> ŘąŮŰ ${p}% ŘŞŮŘ¸ŰŮ Ř´ŘŻ.` });
    return;
  }

  // .llunset â admin only
  if (isCmd('llunset')) {
    const isAdmin = !!msg.member?.permissions.has(PermissionsBitField.Flags.Administrator);
    if (!isAdmin) {
      await msg.reply({ content: 'ŮŮŘˇ ŮŘŻŰŘąŘ§Ů ŮŰâŘŞŮŘ§ŮŮŘŻ Ř§Ř˛ Ř§ŰŮ ŘŻŘłŘŞŮŘą Ř§ŘłŘŞŮŘ§ŘŻŮ ÚŠŮŮŘŻ.' });
      return;
    }
    const arg = content.slice(8).trim();
    const parts = arg.split(/\s+/).filter(Boolean);
    if (parts.length < 2 && msg.mentions.users.size < 2) {
      await msg.reply({ content: 'Ř§ŘłŘŞŮŘ§ŘŻŮ: `.llunset @user1 @user2` ŰŘ§ Ř¨Ř§ Ř˘ŰŘŻŰ ŘŻŮ ÚŠŘ§ŘąŘ¨Řą.' });
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
      await msg.reply({ content: 'ŘŻŮ ÚŠŘ§ŘąŘ¨Řą ŘąŘ§ ŮŘ´ŘŽŘľ ÚŠŮŰŘŻ.' });
      return;
    }
    const gId = msg.guildId!;
    const m = loveOverrides.get(gId);
    if (m) {
      m.delete(loveKey(u1.id, u2.id));
      if (m.size === 0) loveOverrides.delete(gId); else loveOverrides.set(gId, m);
      saveLoveOverrides();
    }
    await msg.reply({ content: `ŘŞŮŘ¸ŰŮ ŘŤŘ§Ř¨ŘŞ Ř¨ŰŮ <@${u1.id}> Ů <@${u2.id}> Ř­Ř°Ů Ř´ŘŻ.` });
    return;
  }

  // .ll command
  if (isCmd('ll')) {
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
      await msg.reply({ content: 'ŘŽŘˇŘ§ ŘŻŘą ŘłŘ§ŘŽŘŞ ŘŞŘľŮŰŘą ŘšŘ´Ů. ŮŘˇŮŘ§Ů ÚŠŮŰ Ř¨ŘšŘŻ ŘŻŮŘ¨Ř§ŘąŮ ŘŞŮŘ§Ř´ ÚŠŮŰŘŻ.' });
      return;
    } finally {
      llInFlight.delete(msg.id);
    }
  }

  // .e command
  if (isCmd('e')) {
    const arg = content.slice(2).trim();
    if (!arg || !/^\d+$/.test(arg)) {
      await msg.reply({ content: 'Ř§ŘłŘŞŮŘ§ŘŻŮ: `.e 30` (Ř§ŮŘ˛ŮŘŻŮ ŘŤŘ§ŮŰŮ Ř¨Ů Ř˘ŘŽŘąŰŮ ŘŞŘ§ŰŮŘą Ř´ŮŘ§)' });
      return;
    }
    const sec = Number(arg);
    if (sec <= 0) {
      await msg.reply({ content: 'ŘšŘŻŘŻ ŮŘšŘŞŘ¨Řą ŮŘ§ŘąŘŻ ÚŠŮŰŘŻ (Ř¨Ř˛ŘąÚŻŘŞŘą Ř§Ř˛ 0).' });
      return;
    }
    const t = await timerManager.extendLast(msg.guildId!, msg.author.id, sec * 1000);
    if (!t) {
      await msg.reply({ content: 'ŘŞŘ§ŰŮŘą ŮŘšŘ§ŮŰ Ř¨ŘąŘ§Ű Ř´ŮŘ§ ŰŘ§ŮŘŞ ŮŘ´ŘŻ.' });
      return;
    }
    return;
  }

  if (!isCmd('t')) return;

  const args = content.slice(2).trim();
  if (!args) {
    await msg.reply({ content: 'Ř§ŘłŘŞŮŘ§ŘŻŮ: `.t 10m [ŘŻŮŰŮ]` ŰŘ§ `.t 60 [ŘŻŮŰŮ]` (ŘšŘŻŘŻ = ŘŤŘ§ŮŰŮ)' });
    return;
  }

  const [first, ...rest] = args.split(/\s+/);
  const reason = rest.join(' ').trim() || null;
  const durationMs = parseDuration(first);
  if (!durationMs || durationMs < 1000) {
    await msg.reply({ content: 'ŮŘŻŘŞ Ř˛ŮŘ§Ů ŮŘ§ŮŘšŘŞŘ¨Řą. ŮŮŮŮŮ: 10m ŰŘ§ 2h ŰŘ§ 60 (ŘŤŘ§ŮŰŮ)' });
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
