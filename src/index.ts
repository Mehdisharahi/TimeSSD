import 'dotenv/config';
import { Client, GatewayIntentBits, Interaction, Message, EmbedBuilder, VoiceState, Collection, PermissionsBitField, ActionRowBuilder, ButtonBuilder, ButtonStyle, GuildMember, AttachmentBuilder } from 'discord.js';
import { createCanvas, GlobalFonts, loadImage, Canvas } from '@napi-rs/canvas';
import fs from 'fs';
import path from 'path';
import { PgFriendStore } from './storage/pgFriendStore';
import { handleTimerInteraction, TimerManager, parseDuration, makeTimerSetEmbed } from './modules/timerManager';

const token = process.env.BOT_TOKEN;

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

// Bot hakim chooses hokm (trump) based on the initial 5-card hand.
function botPickHokm(s: HokmSession): Suit {
  const hand = s.hands.get(s.hakim!) || [];
  const score: Record<Suit, number> = { S:0, H:0, D:0, C:0 };
  const count: Record<Suit, number> = { S:0, H:0, D:0, C:0 };
  for (const c of hand) {
    count[c.s] += 1;
    const rw = c.r>=14?5 : c.r===13?4 : c.r===12?3 : c.r===11?2 : c.r/14;
    score[c.s] += rw;
  }
  const suits: Suit[] = ['S','H','D','C'];
  suits.sort((a,b)=> count[b]-count[a] || score[b]-score[a]);
  return suits[0];
}

async function botChooseHokmAndStart(client: Client, channel: any, s: HokmSession) {
  if (!s.hakim) return;
  const suit = botPickHokm(s);
  s.hokm = suit;
  try { addHokmPick(s.guildId, s.hakim!, suit); saveHokmStats(); } catch {}
  // delete the announce message if present (bot hakim chose immediately)
  try {
    if (s.newSetAnnounceMsgId && channel?.messages?.fetch) {
      const am = await channel.messages.fetch(s.newSetAnnounceMsgId).catch(()=>null);
      if (am) await am.delete().catch(()=>{});
      s.newSetAnnounceMsgId = undefined;
    }
  } catch {}
  const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
  for (const uid of s.order) {
    const need = 13 - (s.hands.get(uid)?.length || 0);
    give(uid, need);
  }
  s.state = 'playing';
  s.leaderIndex = s.order.indexOf(s.hakim); if (s.leaderIndex < 0) s.leaderIndex = 0;
  s.turnIndex = s.leaderIndex; s.table = []; s.leadSuit = null; s.tricksTeam1 = 0; s.tricksTeam2 = 0;
  s.tricksByPlayer = new Map(); s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
  const tableEmbed = new EmbedBuilder().setTitle('Hokm — میز بازی')
    .setDescription(`حکم: ${SUIT_EMOJI[s.hokm]} — نوبت: <@${s.order[s.turnIndex]}>`);
  try {
    if (s.tableMsgId && channel) {
      const m = await channel.messages.fetch(s.tableMsgId).catch(()=>null);
      if (m) await m.edit({ embeds: [tableEmbed] });
    }
  } catch {}
  if (channel) await refreshTableEmbed({ channel }, s);
  await maybeBotAutoPlay(client, s);
}
  }
} catch {}

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

// Cached prerendered card bitmaps to speed up table rendering
const cardBitmapCache = new Map<string, Canvas>();
function cardKey(c: Card, scale: number) { return `${c.s}-${c.r}-${scale}`; }

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
  const t1 = s.team1.map((u,i)=>`${i+1}- ${name(u)}`).join('\n') || '—';
  const t2 = s.team2.map((u,i)=>`${i+1}- ${name(u)}`).join('\n') || '—';
  const sep = '●▬▬▬▬▬▬▬▬▬▬▬●';
  return [
    sep,
    'Team 1:',
    t1,
    sep,
    'Team 2:',
    t2,
    sep,
  ].join('\n');
}

function buildControlButtons(): ActionRowBuilder<ButtonBuilder>[] {
  const row1 = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId('hokm-join-t1').setLabel('تیم 1').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('hokm-join-t2').setLabel('تیم 2').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('hokm-leave').setLabel('خروج').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('hokm-start').setLabel('شروع بازی').setStyle(ButtonStyle.Danger),
  );
  const row2 = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId('hokm-bot-add-t1').setLabel('🤖 بات 1').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId('hokm-bot-add-t2').setLabel('🤖 بات 2').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId('hokm-bot-remove-t1').setLabel('❌ حذف بات 1').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId('hokm-bot-remove-t2').setLabel('❌ حذف بات 2').setStyle(ButtonStyle.Secondary),
  );
  return [row1, row2];
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
  newSetAnnounceMsgId?: string; // message ID of "ست جدید آغاز شد" to delete after hokm chosen
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

// ===== Bot Auto-Play =====
function legalMovesFor(hand: Card[], s: HokmSession): Card[] {
  const lead = s.leadSuit;
  if (!lead) return hand.slice();
  const follow = hand.filter(c=>c.s===lead);
  return follow.length? follow : hand.slice();
}
function chooseBotCard(hand: Card[], s: HokmSession): Card {
  const trump = s.hokm!; const table = s.table || []; const legal = legalMovesFor(hand, s);
  const botId = s.order[s.turnIndex!]; const pos = table.length;
  const botTeam = s.team1.includes(botId)?1:2;
  const beatCard = (a: Card, b: Card)=>{ if(a.s===b.s) return a.r>b.r; if(a.s===trump&&b.s!==trump) return true; return false; };
  const minCard = (arr: Card[])=> [...arr].sort((a,b)=>a.r-b.r)[0];
  const maxCard = (arr: Card[])=> [...arr].sort((a,b)=>b.r-a.r)[0];
  const countBySuit = (h: Card[])=>{ const m=new Map<Suit,number>(); h.forEach(c=>m.set(c.s,(m.get(c.s)||0)+1)); return m; };
  const minNonTrump = (h: Card[])=>{ const nt=h.filter(c=>c.s!==trump); if(!nt.length) return minCard(h); const cnt=countBySuit(nt); return minCard([...nt].sort((a,b)=>(cnt.get(a.s)||0)-(cnt.get(b.s)||0)||a.r-b.r)); };
  const teammate = (tableIdx: number)=>{ const seatIdx=(s.leaderIndex!+tableIdx)%4; const uid=s.order[seatIdx]; return (s.team1.includes(uid)?1:2)===botTeam; };
  const getWinner = ()=>{ if(!table.length) return -1; let w=0,wc=table[0].card; for(let i=1;i<table.length;i++){ if(beatCard(table[i].card,wc)){ w=i; wc=table[i].card; }} return w; };
  const isPlayed = (su: Suit, rk: number)=> s.lastTrick?.some(t=>t.card.s===su&&t.card.r===rk) || table.some(t=>t.card.s===su&&t.card.r===rk);
  const isAcePlayed = (su: Suit)=> isPlayed(su,14);
  const isKingPlayed = (su: Suit)=> isPlayed(su,13);
  
  if (pos===0) {
    const aces = legal.filter(c=>c.r===14&&c.s!==trump);
    if (aces.length) return minCard(aces);
    const kings = legal.filter(c=>c.r===13&&c.s!==trump&&isAcePlayed(c.s));
    if (kings.length) return minCard(kings);
    const queens = legal.filter(c=>c.r===12&&c.s!==trump&&isAcePlayed(c.s)&&isKingPlayed(c.s));
    if (queens.length) return minCard(queens);
    return minNonTrump(legal);
  }
  
  const lead = s.leadSuit!; const follow = legal.filter(c=>c.s===lead); const canFollow = follow.length>0;
  
  if (pos===3) {
    const w = getWinner(); const mateWins = teammate(w);
    if (canFollow) {
      if (mateWins) return minCard(follow);
      const wc = table[w].card;
      const winners = follow.filter(c=>beatCard(c,wc));
      if (winners.length) return minCard(winners);
      return minCard(follow);
    } else {
      if (mateWins) return minNonTrump(legal);
      const trumps = legal.filter(c=>c.s===trump);
      if (!trumps.length) return minNonTrump(legal);
      const wc = table[w].card;
      if (wc.s===trump) {
        const better = trumps.filter(c=>c.r>wc.r);
        return better.length? minCard(better) : minNonTrump(legal);
      }
      return minCard(trumps);
    }
  }
  
  if (pos===1) {
    const c0 = table[0].card;
    if (canFollow) {
      if (c0.r===14) return minCard(follow);
      if (c0.r===13) {
        const ace = follow.find(c=>c.r===14);
        return ace || minCard(follow);
      }
      const ace = follow.find(c=>c.r===14);
      if (ace) return ace;
      const king = follow.find(c=>c.r===13);
      if (king && (c0.r===12 || isAcePlayed(lead))) return king;
      const better = follow.filter(c=>c.r>c0.r);
      if (better.length) {
        const noK = better.filter(c=>c.r!==13);
        return noK.length? maxCard(noK) : minCard(follow);
      }
      return minCard(follow);
    } else {
      const trumps = legal.filter(c=>c.s===trump);
      return trumps.length? minCard(trumps) : minNonTrump(legal);
    }
  }
  
  if (pos===2) {
    const c0=table[0].card, c1=table[1].card;
    const mate0=teammate(0);
    const mateCard = mate0?c0:c1;
    const oppCard = mate0?c1:c0;
    const w = getWinner(); const mateWins = teammate(w);
    
    if (canFollow) {
      if (mateCard.r===14) return minCard(follow);
      if (mateCard.r===13 && mateWins) return minCard(follow);
      if (mateWins) {
        // فقط اگر یار A یا K یا Q (12+) بازی کرده برش نزن
        if (mateCard.r >= 12) return minCard(follow);
        // در غیر این صورت برش بزن اگر می‌تونی
        const wc = table[w].card;
        const better = follow.filter(c=>beatCard(c,wc));
        if (better.length) {
          const noK = better.filter(c=>c.r!==13);
          return noK.length? maxCard(noK) : minCard(follow);
        }
        return minCard(follow);
      }
      const ace = follow.find(c=>c.r===14);
      if (ace) return ace;
      const king = follow.find(c=>c.r===13);
      const wc = table[w].card;
      const better = follow.filter(c=>beatCard(c,wc));
      if (better.length) {
        if (king && isAcePlayed(lead)) return king;
        const noK = better.filter(c=>c.r!==13);
        return noK.length? maxCard(noK) : minCard(follow);
      }
      return minCard(follow);
    } else {
      if (mateCard.r===14 || (mateCard.r===13&&isAcePlayed(lead))) return minNonTrump(legal);
      if (mateWins && oppCard.s!==trump) return minNonTrump(legal);
      const trumps = legal.filter(c=>c.s===trump);
      if (!trumps.length) return minNonTrump(legal);
      if (oppCard.s===trump) {
        const better = trumps.filter(c=>c.r>oppCard.r);
        return better.length? minCard(better) : minNonTrump(legal);
      }
      if (mateCard.r>=12 && !isAcePlayed(lead)) return minNonTrump(legal);
      return minCard(trumps);
    }
  }
  
  return minCard(legal);
}
async function maybeBotAutoPlay(client: Client, s: HokmSession) {
  if (s.state!=='playing' || s.turnIndex==null) return;
  const uid = s.order[s.turnIndex];
  if (!isVirtualBot(uid)) return;
  const hand = s.hands.get(uid) || [];
  if (hand.length===0) return;
  // schedule a short delay
  setTimeout(async () => {
    try {
      // compute legal card
      const card = chooseBotCard(hand, s);
      // play
      const idx = hand.findIndex(c=>c.s===card.s && c.r===card.r);
      if (idx<0) return;
      hand.splice(idx,1); s.hands.set(uid, hand);
      s.table = s.table || []; s.table.push({ userId: uid, card });
      if (!s.leadSuit) s.leadSuit = card.s;
      const nextTurn = ((s.turnIndex ?? 0) + 1) % s.order.length;
      s.turnIndex = nextTurn;
      // render table
      let ch: any = null; try { ch = await client.channels.fetch(s.channelId).catch(()=>null); } catch {}
      if (ch) await refreshTableEmbed({ channel: ch }, s);
      // resolve trick if needed
      if (s.table.length === 4) {
        // fabricate a minimal Interaction-like for resolveTrickAndContinue
        await resolveTrickAndContinue({ client } as any, s);
      } else {
        await maybeBotAutoPlay(client, s);
      }
    } catch {}
  }, 500);
}

// Bot hakim chooses hokm (trump) based on the initial 5-card hand).
function botPickHokm(s: HokmSession): Suit {
  const hand = s.hands.get(s.hakim!) || [];
  const score: Record<Suit, number> = { S:0, H:0, D:0, C:0 };
  const count: Record<Suit, number> = { S:0, H:0, D:0, C:0 };
  for (const c of hand) {
    count[c.s] += 1;
    const rw = c.r>=14?5 : c.r===13?4 : c.r===12?3 : c.r===11?2 : c.r/14;
    score[c.s] += rw;
  }
  const suits: Suit[] = ['S','H','D','C'];
  suits.sort((a,b)=> count[b]-count[a] || score[b]-score[a]);
  return suits[0];
}

async function botChooseHokmAndStart(client: Client, channel: any, s: HokmSession) {
  if (!s.hakim) return;
  const suit = botPickHokm(s);
  s.hokm = suit;
  try { addHokmPick(s.guildId, s.hakim!, suit); saveHokmStats(); } catch {}
  const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
  for (const uid of s.order) {
    const need = 13 - (s.hands.get(uid)?.length || 0);
    give(uid, need);
  }
  s.state = 'playing';
  s.leaderIndex = s.order.indexOf(s.hakim); if (s.leaderIndex < 0) s.leaderIndex = 0;
  s.turnIndex = s.leaderIndex; s.table = []; s.leadSuit = null; s.tricksTeam1 = 0; s.tricksTeam2 = 0;
  s.tricksByPlayer = new Map(); s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
  const tableEmbed = new EmbedBuilder().setTitle('Hokm — میز بازی')
    .setDescription(`حکم: ${SUIT_EMOJI[s.hokm]} — نوبت: <@${s.order[s.turnIndex]}>`);
  try {
    if (s.tableMsgId && channel?.messages?.fetch) {
      const m = await channel.messages.fetch(s.tableMsgId).catch(()=>null);
      if (m) await m.edit({ embeds: [tableEmbed] });
    }
  } catch {}
  if (channel) await refreshTableEmbed({ channel }, s);
  await maybeBotAutoPlay(client, s);
}

// ===== Hokm Stats =====
type HokmUserStat = {
  games: number;
  wins: number;
  teammateWins: Record<string, number>;
  hokmPicks: Partial<Record<Suit, number>>;
};
const hokmStats: Map<string, Map<string, HokmUserStat>> = new Map();
// Prefer env override, then Railway volume (/data), else project data/
function getHokmDataDir() {
  const envDir = process.env.HOKM_DATA_DIR;
  if (envDir && envDir.trim().length) return envDir;
  if (process.platform !== 'win32' && fs.existsSync('/data')) return '/data';
  return path.join(process.cwd(), 'data');
}
const hokmDataDir = getHokmDataDir();
const hokmStatsFile = path.join(hokmDataDir, 'hokm-stats.json');
try { fs.mkdirSync(hokmDataDir, { recursive: true }); } catch {}
console.log(`[HOKM] Stats path: ${hokmStatsFile}`);
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
function sortHand(hand: Card[], hokm?: Suit): Card[] {
  if (!hokm) return [...hand].sort((a,b)=> a.s===b.s ? b.r-a.r : ['S','H','D','C'].indexOf(a.s)-['S','H','D','C'].indexOf(b.s));
  const sameColor = (s: Suit)=>{
    if (s===hokm) return 0;
    if ((s==='H'&&hokm==='D')||(s==='D'&&hokm==='H')) return 1;
    if ((s==='S'&&hokm==='C')||(s==='C'&&hokm==='S')) return 1;
    return 2;
  };
  const countMap = new Map<Suit,number>();
  hand.forEach(c=>countMap.set(c.s, (countMap.get(c.s)||0)+1));
  return [...hand].sort((a,b)=>{
    const colorA = sameColor(a.s); const colorB = sameColor(b.s);
    if (colorA!==colorB) return colorA-colorB;
    if (colorA===0) return b.r-a.r;
    const cA = countMap.get(a.s)||0; const cB = countMap.get(b.s)||0;
    if (cA!==cB) return cB-cA;
    if (a.s===b.s) return b.r-a.r;
    return ['S','H','D','C'].indexOf(a.s)-['S','H','D','C'].indexOf(b.s);
  });
}
function suitName(s: Suit){ return s==='S'?'♠️ پیک':s==='H'?'♥️ دل':s==='D'?'♦️ خشت':'♣️ گیشنیز'; }

function buildHandButtons(s: HokmSession, userId: string, opts?: { filter?: Suit|'ALL'; page?: number }): { rows: ActionRowBuilder<ButtonBuilder>[]; meta: { filter: string; page: number; totalPages: number } } {
  const filter = (opts?.filter ?? 'ALL') as Suit|'ALL';
  const page = opts?.page ?? 0;
  const hand = sortHand(s.hands.get(userId) || [], s.hokm);
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
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-ALL`).setLabel('همه').setStyle(filter==='ALL'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-S`).setLabel('♠️').setStyle(filter==='S'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-H`).setLabel('♥️').setStyle(filter==='H'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-D`).setLabel('♦️').setStyle(filter==='D'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${userId}-C`).setLabel('♣️').setStyle(filter==='C'?ButtonStyle.Primary:ButtonStyle.Secondary),
  );
  rows.push(rowFilter);
  // pagination row (if needed)
  if (totalPages > 1) {
    const rowPage = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId(`hokm-hand-page-${s.guildId}-${s.channelId}-${userId}-${Math.max(0, page-1)}`).setLabel('قبلی').setStyle(ButtonStyle.Secondary).setDisabled(page<=0),
      new ButtonBuilder().setCustomId(`hokm-hand-page-${s.guildId}-${s.channelId}-${userId}-${Math.min(totalPages-1, page+1)}`).setLabel('بعدی').setStyle(ButtonStyle.Secondary).setDisabled(page>=totalPages-1),
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
  for (const uid of s.order) { if (!isVirtualBot(uid)) await refreshPlayerDM(ctx, s, uid); }
}

function clearHandOrderCache(s: HokmSession) {
  // Clear cached suit order for all players when starting a new set/game
  for (const uid of s.order) {
    const orderKey = `__hokm_suit_order_${s.guildId}:${s.channelId}:${uid}`;
    delete (global as any)[orderKey];
  }
}

function buildHandRowsSimple(hand: Card[], userId: string, gId: string, cId: string, hokm?: Suit, initialOrder?: Suit[]): ActionRowBuilder<ButtonBuilder>[] {
  const rows: ActionRowBuilder<ButtonBuilder>[] = [];
  // Group by suit
  const bySuit: Record<Suit, Card[]> = { S: [], H: [], D: [], C: [] };
  hand.forEach(c => bySuit[c.s].push(c));
  // Sort each suit descending
  (Object.keys(bySuit) as Suit[]).forEach(s => {
    bySuit[s].sort((a, b) => b.r - a.r);
  });
  
  // Determine suit order: if initialOrder provided, use it; else use suits with cards in S,H,D,C order
  let suitOrder: Suit[];
  if (initialOrder && initialOrder.length > 0) {
    suitOrder = initialOrder;
  } else {
    // First time: only include suits that have cards
    suitOrder = (['S', 'H', 'D', 'C'] as Suit[]).filter(s => bySuit[s].length > 0);
  }
  
  // Create one row per suit in the fixed order (even if empty now)
  for (const s of suitOrder) {
    const cards = bySuit[s];
    if (cards.length === 0) continue; // skip empty suits but keep order
    const row = new ActionRowBuilder<ButtonBuilder>();
    for (const c of cards) {
      row.addComponents(new ButtonBuilder().setCustomId(`hokm-play-${gId}-${cId}-${userId}-${c.s}-${c.r}`).setLabel(cardStr(c)).setStyle(ButtonStyle.Secondary));
    }
    rows.push(row);
  }
  return rows;
}

async function refreshPlayerChannelHand(ctx: { channel: any }, s: HokmSession, userId: string) {
  if (isVirtualBot(userId)) return; // bots don't need channel hand controls
  const hand = s.hands.get(userId) || [];
  const rows = buildHandRowsSimple(hand, userId, s.guildId, s.channelId, s.hokm);
  const content = `<@${userId}> — ${userId===s.order[s.turnIndex??0] ? 'نوبت شماست.' : 'منتظر نوبت بمانید.'}`;
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
  // virtual bot visuals
  if (isVirtualBot(userId)) {
    const tag = userId.replace('BOT', 'Bot');
    return { tag, img: null };
  }
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
    const hokmTxt = 'حکم؟';
    ctx.fillStyle = '#ffffff';
    const hokmWidth = ctx.measureText(hokmTxt).width;
    const totalWidth = hokmWidth + gap + setsWidth;
    const startX = cx - totalWidth/2;
    ctx.textAlign = 'left';
    ctx.fillText(hokmTxt, startX, cy + 1);
    ctx.fillText(setsTxt, startX + hokmWidth + gap, cy + 1);
  }

  // seats: use exact SVG coordinates and radii for proportional match
  const seats = [
    { x: 500,    y: 201.13, r: 75.75 }, // N (top)
    { x: 861.04, y: 504.00, r: 74.22 }, // E (right)
    { x: 500,    y: 845.00, r: 84.10 }, // S (bottom)
    { x: 144.00, y: 500.00, r: 76.63 }, // W (left)
  ];
  const nameFont = `${ssdFontAvailable? '22px '+ssdFontFamily : '22px Arial'}`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  function drawSeatLabel(i: number, uid?: string, name?: string, avatar?: any, isTurn?: boolean, playerTricks?: number) {
    const seat = seats[i];
    // avatar radius from SVG seat circle to keep proportions
    const avR = seat.r;
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
    } else if (uid && isVirtualBot(uid)) {
      // draw a simple bot avatar placeholder tinted toward team color
      const isT1 = s.team1.includes(uid);
      const tint = isT1 ? '#3b82f6' : '#ef4444';
      ctx.save();
      ctx.beginPath(); ctx.arc(avX, avY, avR, 0, Math.PI*2); ctx.clip();
      // background
      ctx.fillStyle = '#e5e7eb';
      ctx.fillRect(avX-avR, avY-avR, avR*2, avR*2);
      // silhouette
      ctx.fillStyle = tint;
      // head
      ctx.beginPath(); ctx.arc(avX, avY-avR*0.25, avR*0.32, 0, Math.PI*2); ctx.fill();
      // body
      ctx.beginPath(); ctx.roundRect(avX-avR*0.45, avY-avR*0.05, avR*0.9, avR*0.9, avR*0.2); ctx.fill();
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
    // yellow outer ring if player's turn (fast, no heavy blur)
    if (isTurn) {
      ctx.strokeStyle = '#facc15';
      ctx.lineWidth = 6;
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
  function getCardBitmap(c: Card, scale: number) {
    const key = cardKey(c, scale);
    const existing = cardBitmapCache.get(key);
    if (existing) return existing;
    const w = Math.round(110 * scale), h = Math.round(155 * scale), r = Math.round(12 * scale);
    const off = createCanvas(w + Math.ceil(6*scale), h + Math.ceil(8*scale));
    const c2 = off.getContext('2d');
    // shadow
    c2.fillStyle = 'rgba(0,0,0,0.30)';
    c2.beginPath();
    c2.roundRect(4*scale, 6*scale, w, h, r);
    c2.fill();
    // body
    c2.fillStyle = '#ffffff';
    c2.beginPath();
    c2.roundRect(0, 0, w, h, r);
    c2.fill();
    c2.strokeStyle = '#e5e7eb';
    c2.lineWidth = Math.max(1, 2*scale);
    c2.stroke();
    // rank + suit
    const red = (c.s === 'H' || c.s === 'D');
    c2.fillStyle = red ? '#dc2626' : '#111827';
    c2.font = `${ssdFontAvailable? 'bold '+Math.round(36*scale)+'px '+ssdFontFamily : 'bold '+Math.round(36*scale)+'px Arial'}`;
    const rtxt = rankStr(c.r);
    c2.textAlign = 'left';
    c2.textBaseline = 'alphabetic';
    c2.fillText(rtxt, 10*scale, 28*scale);
    // center suit
    c2.textAlign = 'center';
    c2.textBaseline = 'middle';
    if (emojiFontAvailable) {
      c2.fillStyle = '#ffffff';
      c2.font = `bold ${Math.round(56*scale)}px 'Noto Color Emoji'`;
      c2.fillText(SUIT_EMOJI[c.s], w/2, h/2 + 6*scale);
    } else {
      c2.fillStyle = red ? '#dc2626' : '#111827';
      drawSuit(c2 as any, c.s, w/2, h/2 + 6*scale, 28*scale);
    }
    cardBitmapCache.set(key, off);
    return off;
  }
  function drawCard(x: number, y: number, c: Card) {
    const bmp = getCardBitmap(c, 1);
    ctx.drawImage(bmp as any, x, y);
  }
  function drawCardScaled(x: number, y: number, c: Card, scale: number) {
    const bmp = getCardBitmap(c, scale);
    ctx.drawImage(bmp as any, x, y);
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
      // fixed positions to match SVG
      const playPos = [
        { x: 445,    y: 303.58 }, // from top
        { x: 590.29, y: 430.58 }, // from right
        { x: 445,    y: 576.00 }, // from bottom
        { x: 304.02, y: 430.58 }, // from left
      ];
      const pos = playPos[i];
      drawCard(pos.x, pos.y, play.card);
    }
  }

  // Show previous trick (lastTrick) bottom-right — align by seat positions
  if (s.lastTrick && s.lastTrick.length === 4) {
    // mini positions mapped to seats: [Top, Right, Bottom, Left]
    const miniPos = [
      { x: 782.19, y: 693.43 }, // Top
      { x: 860.59, y: 761.96 }, // Right
      { x: 782.19, y: 840.43 }, // Bottom
      { x: 706.12, y: 761.96 }, // Left
    ];
    const scale = 0.54;
    ctx.save();
    ctx.globalAlpha = 0.98;
    // for each seat index, find that user's card in lastTrick
    for (let i=0;i<4;i++) {
      const uid = s.order[i];
      const entry = s.lastTrick.find(t=>t.userId===uid);
      if (entry) drawCardScaled(miniPos[i].x, miniPos[i].y, entry.card, scale);
    }
    ctx.restore();
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

async function renderTableSVG(s: HokmSession) {
  const width = 1000, height = 1000;
  const cx = 500, cy = 500;
  // positions and radii to match the user's Illustrator SVG
  const seatSpec = [
    { x: 500, y: 201.13, r: 75.75 }, // top
    { x: 861.04, y: 504.00, r: 74.22 }, // right
    { x: 500, y: 845.00, r: 84.10 }, // bottom
    { x: 144.00, y: 500.00, r: 76.63 }, // left
  ];
  const teamColor = (uid?: string)=> uid && s.team1.includes(uid) ? '#3b82f6' : '#ef4444';
  const isTurn = (i:number)=> s.turnIndex!=null && s.order[s.turnIndex]===s.order[i];
  const suitTxt = (su: Suit)=> SUIT_EMOJI[su];
  function esc(t: string){ return t.replace(/[&<>\"]/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;","\\\"":"&quot;"}[c] as string)); }
  function cardGroup(id:string, x:number, y:number, c:Card, scale=1){
    const w=110*scale,h=155*scale,r=12;
    const red = (c.s==='H'||c.s==='D');
    return `
    <g id="${id}">
      <rect class="st12" x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" ry="${r}"/>
      <text class="st6" transform="translate(${x+10} ${y+28})">${esc(rankStr(c.r))}</text>
      <text class="st7" transform="translate(${x+19.12*scale} ${y+83.5*scale})">${esc(suitTxt(c.s))}</text>
    </g>`;
  }
  // header content
  const hokmMark = s.hokm ? `${esc(suitTxt(s.hokm))}` : 'حکم؟';
  const setsTxt = `Sets: ${s.targetSets ?? 1}`;
  // table cards positions to match sample
  const playPos = [
    { x: 445, y: 303.58 }, // from top
    { x: 590.29, y: 430.58 }, // from right
    { x: 445, y: 576 }, // from bottom
    { x: 304.02, y: 430.58 }, // from left
  ];
  // last trick mini-cards bottom-right (if available)
  const miniPos = [
    { x: 782.19, y: 693.43 },
    { x: 706.12, y: 761.96 },
    { x: 860.59, y: 761.96 },
    { x: 782.19, y: 840.43 },
  ];
  const table = s.table||[];
  let playsSvg = '';
  for (let i=0;i<4;i++){
    const pl = table.find(t=>t.userId===s.order[i]);
    if (pl) playsSvg += cardGroup(`plays${i===0?'':i}`, playPos[i].x, playPos[i].y, pl.card, 1);
  }
  let lastSvg = '';
  if (s.lastTrick && s.lastTrick.length) {
    for (let i=0;i<Math.min(4, s.lastTrick.length); i++) {
      const c = s.lastTrick[i].card;
      lastSvg += cardGroup(`plays${i+4}`, miniPos[i].x, miniPos[i].y, c, 0.54);
    }
  }
  // build SVG with the user's class names and styles
  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg id="Layer_1" xmlns="http://www.w3.org/2000/svg" version="1.1" viewBox="0 0 1000 1000">
  <defs>
    <style>
      .st0,.st1,.st2,.st3,.st4,.st5,.st6,.st7,.st8,.st9,.st10,.st11{isolation:isolate}
      .st0,.st1,.st5,.st6,.st8,.st9,.st10,.st11{font-family:${ssdFontAvailable?esc(ssdFontFamily):'Arial'}, Arial}
      .st0,.st9{fill:#3b82f6}
      .st0,.st10{font-size:44px}
      .st1,.st3{font-size:22px}
      .st1,.st3,.st5,.st12{fill:#fff}
      .st2{font-size:30.22px}
      .st2,.st3,.st7{font-family:ArialMT, Arial}
      .st2,.st6,.st7,.st8{fill:#111827}
      .st13{stroke:#d1fae5}
      .st13,.st14,.st15,.st16{fill:none}
      .st13,.st15{stroke-width:4px}
      .st14{stroke:#3b82f6}
      .st14,.st16{stroke-width:5px}
      .st15{stroke:#facc15}
      .st5,.st9,.st11{font-size:40px}
      .st16{stroke:#ef4444}
      .st6{font-size:30px}
      .st7{font-size:56px}
      .st17{fill:#ccc}
      .st8{font-size:16.19px}
      .st10,.st11{fill:#ef4444}
      .st12{stroke:#e5e7eb;stroke-width:2px}
      .st18{fill:#0f5132}
    </style>
  </defs>
  <g id="background">
    <rect class="st18" width="1000" height="1000"/>
    <rect class="st13" x="8" y="8" width="984" height="984"/>
  </g>
  <g id="header">
    <rect x="10" y="10" width="980" height="54"/>
    <text class="st3" transform="translate(468.14 38)">${s.hokm?esc(suitTxt(s.hokm)):esc('حکم؟')}</text>
    <text class="st1" transform="translate(518 38)">${esc(setsTxt)}</text>
  </g>
  <g id="teams">
    <text class="st0" transform="translate(28 96)">Team 1</text>
    <g class="st4">
      <text class="st9" transform="translate(28 146)">Tricks: </text>
      <text class="st5" transform="translate(166.76 146)">${String(s.tricksTeam1 ?? 0)}</text>
      <text class="st9" transform="translate(191.48 146)" xml:space="preserve"> Sets: </text>
      <text class="st5" transform="translate(299.28 146)">${String(s.setsTeam1 ?? 0)}</text>
    </g>
    <text class="st10" transform="translate(818.93 96)">Team 2</text>
    <g class="st4">
      <text class="st11" transform="translate(676.01 146)">Tricks: </text>
      <text class="st5" transform="translate(814.76 146)">${String(s.tricksTeam2 ?? 0)}</text>
      <text class="st11" transform="translate(839.48 146)" xml:space="preserve"> Sets: </text>
      <text class="st5" transform="translate(947.28 146)">${String(s.setsTeam2 ?? 0)}</text>
    </g>
  </g>
  ${seatSpec.map((p,i)=>{
    const uid = s.order[i];
    const ring = teamColor(uid);
    const isT = isTurn(i);
    const ringClass = ring==='#3b82f6'?'st14':'st16';
    const base = `<circle class="st17" cx="${p.x}" cy="${p.y}" r="${p.r}"/>\n  <circle class="${ringClass}" cx="${p.x}" cy="${p.y}" r="${(p.r+4).toFixed(2)}"/>`;
    const turn = isT? `\n  <circle class=\"st15\" cx=\"${p.x}\" cy=\"${p.y}\" r=\"${(p.r+14.36).toFixed(2)}\"/>` : '';
    return base+turn;
  }).join('\n')}
  <g id="plays">${playsSvg}</g>
  ${lastSvg ? `<g id="last_trick">${lastSvg}</g>` : ''}
</svg>`;
  return Buffer.from(svg, 'utf8');
}

async function refreshTableEmbed(ctx: { channel: any }, s: HokmSession) {
  const img = await renderTableImage(s);
  const attachment = new AttachmentBuilder(img, { name: 'table.png' });
  const embed = new EmbedBuilder()
    .setTitle('Hokm — میز بازی')
    .setColor(0x2f3136)
    .setImage('attachment://table.png');
  const openRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`hokm-open-hand-${s.guildId}-${s.channelId}`).setLabel('دست من').setStyle(ButtonStyle.Secondary)
  );
  // add hokm choose buttons when waiting for hakim to pick
  const rows: any[] = [openRow];
  if (s.state === 'choosing_hokm' && s.hakim) {
    const chooseRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId('hokm-choose-S').setLabel('♠️ پیک').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('hokm-choose-H').setLabel('♥️ دل').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('hokm-choose-D').setLabel('♦️ خشت').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId('hokm-choose-C').setLabel('♣️ گیشنیز').setStyle(ButtonStyle.Success),
    );
    embed.setDescription(`حاکم: <@${s.hakim}> — لطفاً حکم را انتخاب کن.`);
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
    // hand complete -> award set(s) with koot rules
    const t1Tr = s.tricksTeam1||0; const t2Tr = s.tricksTeam2||0;
    const winnerTeam = t1Tr >= target ? 't1' : 't2';
    const winnerTr = winnerTeam==='t1' ? t1Tr : t2Tr;
    const loserTr = winnerTeam==='t1' ? t2Tr : t1Tr;
    s.setsTeam1 = s.setsTeam1 || 0; s.setsTeam2 = s.setsTeam2 || 0;
    let add = 1;
    if (winnerTr === 7 && loserTr === 0) {
      const hakimIsTeam1 = s.team1.includes(s.hakim!);
      const winnerIsHakimTeam = (winnerTeam==='t1' && hakimIsTeam1) || (winnerTeam==='t2' && !hakimIsTeam1);
      add = winnerIsHakimTeam ? 2 : 3; // koot=2, hakim-koot=3
    }
    if (winnerTeam==='t1') s.setsTeam1 += add; else s.setsTeam2 += add;
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
        const starter = s.ownerId ? `<@${s.ownerId}>` : '—';
        const lines: string[] = [];
        lines.push(`### ✹Starter: ${starter}`);
        lines.push(`### ✹Sets: ${s.targetSets ?? 1}`);
        lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬●');
        lines.push(`### ✹Team 1: ${s.team1.map(u=>`<@${u}>`).join(' , ')} ➤ ${t1Set}`);
        lines.push('════════════════════');
        lines.push(`### ✹Team 2: ${s.team2.map(u=>`<@${u}>`).join(' , ')} ➤ ${t2Set}`);
        lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬●');
        lines.push(`### ✹Winner: Team ${t1Set>t2Set?1:2} ✅`);
        const emb = new EmbedBuilder().setDescription(lines.join('\n')).setColor(t1Set>t2Set?0x3b82f6:0xef4444);
        await gameChannel.send({ embeds: [emb] });
      }
      return;
    }
    // prepare next hand in same match
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    clearHandOrderCache(s); // Clear cached suit order for new set
    s.hokm = undefined; s.table = []; s.leadSuit = null; s.tricksTeam1 = 0; s.tricksTeam2 = 0; s.tricksByPlayer = new Map(); s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
    // choose next hakim: if current hakim's team won, keep; else clockwise next player
    const hakimIdx = s.order.indexOf(s.hakim!);
    const hakimIsTeam1 = s.team1.includes(s.hakim!);
    const hakimTeamWon = (winnerTeam==='t1' && hakimIsTeam1) || (winnerTeam==='t2' && !hakimIsTeam1);
    s.hakim = hakimTeamWon ? s.hakim! : s.order[(hakimIdx+1) % s.order.length];
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.state = 'choosing_hokm';
    try { const user = await (interaction.client as Client).users.fetch(s.hakim); await user.send({ content: `ست جدید شروع شد. دست اولیه شما (۵ کارت):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    if (gameChannel) {
      const announceMsg = await gameChannel.send({ content: `ست جدید آغاز شد. حاکم: <@${s.hakim}> — لطفاً حکم را انتخاب کن.` });
      s.newSetAnnounceMsgId = announceMsg.id;
    }
    if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s);
    await refreshAllDMs({ client: (interaction.client as Client) }, s);
    if (isVirtualBot(s.hakim)) { await botChooseHokmAndStart(interaction.client as Client, gameChannel, s); }
    return;
  }

  if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s);
  await refreshAllDMs({ client: (interaction.client as Client) }, s);
  // trigger bot auto-play if next turn is bot
  await maybeBotAutoPlay(interaction.client as Client, s);
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
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      const uid = interaction.user.id;
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
      // Update control message as plain text (no embed)
      const contentText = controlListText(s);
      const rows = buildControlButtons();
      try { if (s.controlMsgId) { const m = await (interaction.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      return;
    }

    // Bot management buttons
    if (id === 'hokm-bot-add-t1' || id === 'hokm-bot-add-t2' || id === 'hokm-bot-remove-t1' || id === 'hokm-bot-remove-t2') {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (s.state !== 'waiting') { await interaction.reply({ content: 'فقط قبل از شروع بازی می‌توانید بات اضافه/حذف کنید.', ephemeral: true }); return; }
      if (s.ownerId && interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'فقط سازنده اتاق می‌تواند بات اضافه/حذف کند.', ephemeral: true }); return; }
      
      if (id === 'hokm-bot-add-t1') {
        const added = addBotToTeam(s, 1);
        await interaction.reply({ content: added ? `Bot به تیم 1 افزوده شد (${added.id.replace('BOT','Bot')}).` : 'امکان افزودن Bot به تیم 1 وجود ندارد (تیم پر است یا بات‌های موجود تمام شده‌اند).', ephemeral: true });
      } else if (id === 'hokm-bot-add-t2') {
        const added = addBotToTeam(s, 2);
        await interaction.reply({ content: added ? `Bot به تیم 2 افزوده شد (${added.id.replace('BOT','Bot')}).` : 'امکان افزودن Bot به تیم 2 وجود ندارد (تیم پر است یا بات‌های موجود تمام شده‌اند).', ephemeral: true });
      } else if (id === 'hokm-bot-remove-t1') {
        const botInTeam = s.team1.find(u => isVirtualBot(u));
        if (botInTeam) {
          s.team1 = s.team1.filter(u => u !== botInTeam);
          await interaction.reply({ content: `${botInTeam.replace('BOT','Bot')} از تیم 1 حذف شد.`, ephemeral: true });
        } else {
          await interaction.reply({ content: 'هیچ باتی در تیم 1 وجود ندارد.', ephemeral: true });
        }
      } else if (id === 'hokm-bot-remove-t2') {
        const botInTeam = s.team2.find(u => isVirtualBot(u));
        if (botInTeam) {
          s.team2 = s.team2.filter(u => u !== botInTeam);
          await interaction.reply({ content: `${botInTeam.replace('BOT','Bot')} از تیم 2 حذف شد.`, ephemeral: true });
        } else {
          await interaction.reply({ content: 'هیچ باتی در تیم 2 وجود ندارد.', ephemeral: true });
        }
      }
      
      // Update control message
      const contentText = controlListText(s);
      const rows = buildControlButtons();
      try { if (s.controlMsgId) { const m = await (interaction.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      return;
    }

    // Start button → 4 teams ready → init hands and start choosing hokm
    if (id === 'hokm-start') {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const s = ensureSession(interaction.guild.id, interaction.channel.id);
      if (!s.ownerId || interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'فقط سازنده اتاق می‌تواند شروع کند.', ephemeral: true }); return; }
      if (s.state !== 'waiting') { await interaction.reply({ content: 'اتاق در وضعیت شروع نیست.', ephemeral: true }); return; }
      if (s.team1.length !== 2 || s.team2.length !== 2) { await interaction.reply({ content: 'هر دو تیم باید ۲ نفر داشته باشند.', ephemeral: true }); return; }
      s.targetSets = s.targetSets ?? 1;
      s.targetTricks = s.targetTricks ?? 7;
      s.setsTeam1 = 0; s.setsTeam2 = 0;
      s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
      s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
      s.deck = shuffle(makeDeck());
      s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
      clearHandOrderCache(s); // Clear cached suit order for new game
      const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
      give(s.hakim, 5);
      s.state = 'choosing_hokm';
      try { const user = await interaction.client.users.fetch(s.hakim); await user.send({ content: `ست جدید شروع شد. دست اولیه شما (۵ کارت):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
      try {
        const chAny = interaction.channel as any;
        if (chAny && chAny.send) {
          const announceMsg = await chAny.send({ content: `ست جدید آغاز شد. حاکم: <@${s.hakim}> — لطفاً حکم را انتخاب کن.` });
          s.newSetAnnounceMsgId = announceMsg.id;
        }
      } catch {}
      if (interaction.guild) await refreshTableEmbed({ channel: interaction.channel as any }, s);
      await refreshAllDMs({ client: interaction.client }, s);
      if (isVirtualBot(s.hakim)) { await botChooseHokmAndStart(interaction.client as Client, interaction.channel as any, s); }
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
      const tableEmbed = new EmbedBuilder().setTitle('Hokm — میز بازی')
        .setDescription(`حکم: ${SUIT_EMOJI[s.hokm]} — نوبت: <@${s.order[s.turnIndex]}>`);
      try {
        if (s.tableMsgId) {
          const m = await (interaction.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
          if (m) await m.edit({ embeds: [tableEmbed] });
        }
      } catch {}
      // also delete text announce if present (human hakim)
      if (s.newSetAnnounceMsgId) {
        try {
          const am = await (interaction.channel as any).messages.fetch(s.newSetAnnounceMsgId).catch(()=>null);
          if (am) await am.delete().catch(()=>{});
          s.newSetAnnounceMsgId = undefined;
        } catch {}
      }
      await refreshTableEmbed({ channel: interaction.channel }, s);
      // delete the announce message if present
      if (s.newSetAnnounceMsgId) {
        try {
          const am = await (interaction.channel as any).messages.fetch(s.newSetAnnounceMsgId).catch(()=>null);
          if (am) await am.delete().catch(()=>{});
          s.newSetAnnounceMsgId = undefined;
        } catch {}
      }
      // no per-player channel hand messages; users open hand ephemerally via table button
      await interaction.reply({ content: `حکم انتخاب شد: ${SUIT_EMOJI[s.hokm]}. بازی شروع شد. برای دیدن دست خود، روی دکمه "دست من" زیر میز بزن.`, ephemeral: true });
      // trigger bot auto-play if first turn is a bot
      await maybeBotAutoPlay(interaction.client as Client, s);
      return;
    }

    // Open Hand button (ephemeral per-user hand in channel)
    if (id.startsWith('hokm-open-hand-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', ephemeral: true }); return; }
      const parts = id.split('-'); // hokm-open-hand-gId-cId
      const gId = parts[3]; const cId = parts[4];
      const s = ensureSession(gId, cId);
      const uid = interaction.user.id;
      const hand = s.hands.get(uid) || [];
      
      // Check if user has cards
      if (hand.length === 0) {
        const msg = s.state === 'choosing_hokm' ? 'شما هنوز کارتی ندارید. فقط حاکم در این مرحله کارت دارد.' : 'شما کارتی ندارید.';
        await interaction.reply({ content: msg, ephemeral: true });
        return;
      }
      
      // Store or retrieve initial suit order for this user
      const orderKey = `__hokm_suit_order_${gId}:${cId}:${uid}`;
      let initialOrder = (global as any)[orderKey] as Suit[] | undefined;
      if (!initialOrder) {
        // First time showing hand: determine order based on current suits
        initialOrder = (['S', 'H', 'D', 'C'] as Suit[]).filter(s => hand.some(c => c.s === s));
        (global as any)[orderKey] = initialOrder;
      }
      
      const rows = buildHandRowsSimple(hand, uid, s.guildId, s.channelId, s.hokm, initialOrder);
      const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${uid===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}`;
      await interaction.reply({ content, components: rows, ephemeral: true });
      return;
    }

    // DM hand filter buttons
    if (id.startsWith('hokm-hand-filter-')) {
      const parts = id.split('-'); // hokm-hand-filter-gId-cId-uid-FL
      const gId = parts[3]; const cId = parts[4]; const uid = parts[5]; const fl = parts[6] as any;
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'این دکمه برای دست شما نیست.', ephemeral: true }); return; }
      const key = `__hokm_dm_state_${gId}:${cId}:${uid}`;
      (global as any)[key] = { filter: fl, page: 0 };
      const s = ensureSession(gId, cId);
      if (interaction.guild) {
        const { rows, meta } = buildHandButtons(s, uid, { filter: fl, page: 0 });
        (global as any)[key] = { filter: meta.filter, page: meta.page };
        const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${uid===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}\nدست شما:\n${handToString(s.hands.get(uid) || [])}`;
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
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'این دکمه برای دست شما نیست.', ephemeral: true }); return; }
      const key = `__hokm_dm_state_${gId}:${cId}:${uid}`;
      const prev = (global as any)[key] || { filter: 'ALL', page: 0 };
      (global as any)[key] = { filter: prev.filter || 'ALL', page };
      const s = ensureSession(gId, cId);
      if (interaction.guild) {
        const { rows, meta } = buildHandButtons(s, uid, { filter: (prev.filter||'ALL') as any, page });
        (global as any)[key] = { filter: meta.filter, page: meta.page };
        const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${uid===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}\nدست شما:\n${handToString(s.hands.get(uid) || [])}`;
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
      if (!gId || !cId) { await interaction.reply({ content: 'خطای کانال بازی.', ephemeral: true }); return; }
      const s = ensureSession(gId, cId);
      if (s.state !== 'playing' || s.turnIndex==null) { await interaction.reply({ content: 'بازی در جریان نیست.', ephemeral: true }); return; }
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
      // update the ephemeral hand panel dynamically
      {
        const rows = buildHandRowsSimple(hand, uid, s.guildId, s.channelId, s.hokm);
        const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${uid===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}`;
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
      } else {
        // trigger bot if next turn is bot
        await maybeBotAutoPlay(interaction.client as Client, s);
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

  // .best — top 20 Hokm winners (by wins)
  if (isCmd('best')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const gId = msg.guildId!;
    const stats = hokmStats.get(gId);
    if (!stats || stats.size === 0) { await msg.reply({ content: 'در این سرور بازی انجام نشده است.' }); return; }
    const entries = Array.from(stats.entries()) as Array<[string, HokmUserStat]>;
    const arr = entries
      .filter(([, st]) => ((st?.games)||0) > 0)
      .sort((a: [string, HokmUserStat], b: [string, HokmUserStat]) => ((b[1].wins||0) - (a[1].wins||0)) || ((b[1].games||0) - (a[1].games||0)))
      .slice(0, 20);
    if (arr.length === 0) { await msg.reply({ content: 'در این سرور بازی انجام نشده است.' }); return; }
    const server = msg.guild.name;
    const lines: string[] = [];
    lines.push(`## ✵ ${server} WINNER LIST:`);
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    let idx = 0;
    for (const [uid, st] of arr) {
      idx++;
      const rank = String(idx).padStart(2, '0');
      lines.push(`### ➡ ${rank} - <@${uid}> ▶︎Games : ${st.games||0} 💫WIN: ${st.wins||0}`);
    }
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    const embedBest = new EmbedBuilder().setDescription(lines.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embedBest] });
    return;
  }

  // .bazikon — show user's Hokm stats
  if (isCmd('bazikon')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const gId = msg.guildId!;
    const targetIds = await resolveTargetIds(msg, content, '.bazikon');
    const targetId = targetIds[0] || msg.author.id;
    const stMap = hokmStats.get(gId);
    const st: HokmUserStat = stMap?.get(targetId) || { games: 0, wins: 0, teammateWins: {}, hokmPicks: {} };
    if (!st.games) { await msg.reply({ content: 'این کاربر بازی انجام نداده است.' }); return; }
    let bestMate: string | null = null; let bestWins = 0;
    for (const [uid, w] of Object.entries((st.teammateWins||{}) as Record<string, number>)) {
      const val = Number(w)||0;
      if (val > bestWins) { bestWins = val; bestMate = uid; }
    }
    const mateText = bestMate ? `<@${bestMate}> (${bestWins} WIN)` : '—';
    const picks = (st.hokmPicks || {}) as Partial<Record<Suit, number>>;
    const suitOrder: Suit[] = ['C','S','D','H'];
    const sortedSuits = suitOrder.sort((a,b)=> (picks[b]||0) - (picks[a]||0));
    const favArray = sortedSuits.filter(su => (picks[su]||0) > 0).map(su => SUIT_EMOJI[su as Suit]);
    const favText = favArray.join(' ');
    const lines: string[] = [];
    lines.push(`## ✵ <@${targetId}> Stats:`);
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    lines.push(`### ▶︎ Games : ${st.games||0}`);
    lines.push(`### 💫 WIN: ${st.wins||0}`);
    lines.push(`### 🫂 Best Teamate: ${mateText}`);
    lines.push(`### 🃏 Favorite hokm: ${favText}`);
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    const embedBaz = new EmbedBuilder().setDescription(lines.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embedBaz] });
    return;
  }

  // .topfriend — list top 10 pairs with most co-voice time (exclude bots)
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

  // .new — create room with join buttons
  if (isCmd('new')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    // reset session
    s.team1 = []; s.team2 = []; s.order = []; s.hakim = undefined; s.hokm = undefined; s.deck = []; s.hands.clear(); s.state = 'waiting'; s.ownerId = msg.author.id; s.tableMsgId = undefined;
    const contentText = controlListText(s);
    const rows = buildControlButtons();
    const sent = await msg.reply({ content: contentText, components: rows });
    s.controlMsgId = sent.id;
    return;
  }

  // .a1 @user — owner assigns user to Team 1
  if (isCmd('a1')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را اضافه کند.'); return; }
    const raw = content.slice(3).trim();
    if (/^bot\b/i.test(raw)) {
      const added = addBotToTeam(s, 1);
      const contentText = controlListText(s);
      const rows = buildControlButtons();
      try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      const replyMsg = await msg.reply({ content: added? `Bot به تیم 1 افزوده شد (${added.id.replace('BOT','Bot')}).` : 'امکان افزودن Bot به تیم 1 وجود ندارد.' });
      setTimeout(() => replyMsg.delete().catch(()=>{}), 2500);
      return;
    }
    const targets = await resolveTargetIds(msg, content, '.a1');
    if (targets.length === 0) { await msg.reply('استفاده: `.a1 @user1 @user2` یا `.a1 bot`'); return; }
    const added: string[] = []; const skipped: string[] = [];
    for (const uid of targets) {
      try { const u = await msg.client.users.fetch(uid); if (u.bot) { skipped.push(`<@${uid}> (bot)`); continue; } } catch { skipped.push(`<@${uid}> (نامعتبر)`); continue; }
      if (s.team1.includes(uid)) { skipped.push(`<@${uid}> (قبلاً تیم 1)`); continue; }
      s.team1 = s.team1.filter(x=>x!==uid); s.team2 = s.team2.filter(x=>x!==uid);
      if (s.team1.length >= 2) { skipped.push(`<@${uid}> (تیم 1 پر است)`); continue; }
      s.team1.push(uid); added.push(`<@${uid}>`);
    }
    const contentText = controlListText(s);
    const rows = buildControlButtons();
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
    await msg.reply({ content: `افزوده شد: ${added.join(' , ') || '—'}` });
    return;
  }

  // .a2 @user — owner assigns user to Team 2
  if (isCmd('a2')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را اضافه کند.'); return; }
    const raw = content.slice(3).trim();
    if (/^bot\b/i.test(raw)) {
      const added = addBotToTeam(s, 2);
      const contentText = controlListText(s);
      const rows = buildControlButtons();
      try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      const replyMsg = await msg.reply({ content: added? `Bot به تیم 2 افزوده شد (${added.id.replace('BOT','Bot')}).` : 'امکان افزودن Bot به تیم 2 وجود ندارد.' });
      setTimeout(() => replyMsg.delete().catch(()=>{}), 2500);
      return;
    }
    const targets = await resolveTargetIds(msg, content, '.a2');
    if (targets.length === 0) { await msg.reply('استفاده: `.a2 @user1 @user2` یا `.a2 bot`'); return; }
    const added: string[] = []; const skipped: string[] = [];
    for (const uid of targets) {
      try { const u = await msg.client.users.fetch(uid); if (u.bot) { skipped.push(`<@${uid}> (bot)`); continue; } } catch { skipped.push(`<@${uid}> (نامعتبر)`); continue; }
      if (s.team2.includes(uid)) { skipped.push(`<@${uid}> (قبلاً تیم 2)`); continue; }
      s.team1 = s.team1.filter(x=>x!==uid); s.team2 = s.team2.filter(x=>x!==uid);
      if (s.team2.length >= 2) { skipped.push(`<@${uid}> (تیم 2 پر است)`); continue; }
      s.team2.push(uid); added.push(`<@${uid}>`);
    }
    const contentText = controlListText(s);
    const rows = buildControlButtons();
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
    {
      const lines: string[] = [];
      lines.push(`افزوده شد: ${added.join(' , ') || '—'}`);
      if (skipped.length > 0) lines.push(`نادیده: ${skipped.join(' , ')}`);
      await msg.reply({ content: lines.join('\n') });
    }
    return;
  }

  // .r — owner removes a user from teams
  if (isCmd('r')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را حذف کند.'); return; }
    // special: remove virtual bots with `.r bot`
    const rawArg = content.slice(2);
    if (/^\s*bot/i.test(rawArg)) {
      const before1 = [...s.team1];
      const before2 = [...s.team2];
      s.team1 = s.team1.filter(u=>!isVirtualBot(u));
      s.team2 = s.team2.filter(u=>!isVirtualBot(u));
      const removedBots: string[] = [];
      for (const u of before1) if (isVirtualBot(u) && !s.team1.includes(u)) removedBots.push(`<@${u.replace('BOT','Bot')}>`);
      for (const u of before2) if (isVirtualBot(u) && !s.team2.includes(u)) removedBots.push(`<@${u.replace('BOT','Bot')}>`);
      const contentText = controlListText(s);
      const rows = buildControlButtons();
      try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      const replyMsg = await msg.reply({ content: `حذف شد: ${removedBots.join(' , ') || '—'}` });
      setTimeout(()=>replyMsg.delete().catch(()=>{}), 2500);
      return;
    }
    const targets = await resolveTargetIds(msg, content, '.r');
    if (targets.length === 0) { await msg.reply('استفاده: `.r @user1 @user2` یا ریپلای/آیدی'); return; }
    const removed: string[] = []; const notIn: string[] = [];
    for (const uid of targets) {
      const inAny = s.team1.includes(uid) || s.team2.includes(uid);
      s.team1 = s.team1.filter(x=>x!==uid);
      s.team2 = s.team2.filter(x=>x!==uid);
      if (inAny) removed.push(`<@${uid}>`); else notIn.push(`<@${uid}>`);
    }
    const contentText = controlListText(s);
    const rows = buildControlButtons();
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
    {
      const lines: string[] = [];
      lines.push(`حذف شد: ${removed.join(' , ') || '—'}`);
      if (notIn.length > 0) lines.push(`ناموجود: ${notIn.join(' , ')}`);
      const replyMsg = await msg.reply({ content: lines.join('\n') });
      setTimeout(() => replyMsg.delete().catch(()=>{}), 2500);
    }
    return;
  }

  // .end — owner ends the room and deletes control/table messages
  if (isCmd('end')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (!s.ownerId || msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند پایان دهد.'); return; }
    // delete control and table messages if exist
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.delete().catch(()=>{}); } } catch {}
    try { if (s.tableMsgId) { const m2 = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null); if (m2) await m2.delete().catch(()=>{}); } } catch {}
    // clear session
    s.team1 = []; s.team2 = []; s.order = []; s.hakim = undefined; s.hokm = undefined; s.deck = []; s.hands.clear(); s.state = 'finished'; s.controlMsgId = undefined; s.tableMsgId = undefined;
    await msg.reply('اتاق پایان یافت.');
    return;
  }

  // .reset — owner resets the room and redeals (like fresh start with current teams)
  if (isCmd('reset')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (!s.ownerId || msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند ریست کند.'); return; }
    if (s.team1.length !== 2 || s.team2.length !== 2) { await msg.reply('برای ریست، هر دو تیم باید ۲ نفر داشته باشند.'); return; }
    // reinitialize game state
    s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
    s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    clearHandOrderCache(s); // Clear cached suit order for reset
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.hokm = undefined; s.tableMsgId = undefined;
    s.state = 'choosing_hokm';
    try { const user = await msg.client.users.fetch(s.hakim); await user.send({ content: `بازی ریست شد. دست اولیه شما (۵ کارت):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    // update control list if exists
    if (s.controlMsgId) {
      const contentText = controlListText(s);
      const rows = buildControlButtons();
      try { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } catch {}
    }
    await msg.reply({ content: `ریست شد. حاکم: <@${s.hakim}> — لطفاً با ".hokm hokm <خال>" حکم را انتخاب کن.` });
    return;
  }

  // .list — recreate control list if waiting; otherwise re-render table
  if (isCmd('list')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state === 'waiting') {
      // delete previous control message if exists
      if (s.controlMsgId) {
        try { const prev = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (prev) await prev.delete().catch(()=>{}); } catch {}
        s.controlMsgId = undefined;
      }
      const contentText = controlListText(s);
      const rows = buildControlButtons();
      const sent = await msg.reply({ content: contentText, components: rows });
      s.controlMsgId = sent.id;
    } else {
      try { await refreshTableEmbed({ channel: msg.channel }, s); } catch {}
    }
    return;
  }

  // .tablepng — ارسال عکس میز برای دانلود/ادیت
  if (isCmd('tablepng')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    try {
      const buffer = await renderTableImage(s);
      const attachment = new AttachmentBuilder(buffer, { name: 'hokm-table.png' });
      await msg.reply({ files: [attachment] });
    } catch {
      await msg.reply({ content: 'خطا در ساخت تصویر میز.' });
    }
    return;
  }

  // .tablesvg — خروجی وکتور کامل میز با گروه‌بندی المان‌ها
  if (isCmd('tablesvg')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    try {
      const svgBuf = await renderTableSVG(s);
      const attachment = new AttachmentBuilder(svgBuf, { name: 'hokm-table.svg' });
      await msg.reply({ files: [attachment] });
    } catch {
      await msg.reply({ content: 'خطا در ساخت SVG میز.' });
    }
    return;
  }

  // .miz — پاک‌سازی پیام میز فعلی و نمایش دوباره میز در چنل
  if (isCmd('miz')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
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

  // .hokm start — start game; optional N sets to win match
  if (isSubCmd('hokm','start')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند بازی را شروع کند.'); return; }
    if (s.state !== 'waiting') { await msg.reply('اتاق در وضعیت شروع نیست.'); return; }
    if (s.team1.length !== 2 || s.team2.length !== 2) { await msg.reply('هر دو تیم باید ۲ نفر داشته باشند.'); return; }
    // parse optional target sets (full hands)
    const m = content.match(/^\.hokm start(?:\s+(\d+))?/);
    let targetSets = 1;
    if (m && m[1]) {
      const n = parseInt(m[1], 10);
      if (Number.isNaN(n) || n < 1 || n > 7) { await msg.reply('عدد معتبر بین 1 تا 7 وارد کنید. مثال: `.hokm start 3`'); return; }
      targetSets = n;
    }
    s.targetSets = targetSets; // number of sets to win
    s.targetTricks = s.targetTricks ?? 7; // tricks to win a set (always 7)
    s.setsTeam1 = 0; s.setsTeam2 = 0;
    s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
    s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
    s.deck = shuffle(makeDeck());
    s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
    clearHandOrderCache(s); // Clear cached suit order for new game
    // deal 5 to hakim
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.state = 'choosing_hokm';
    // DM hakim hand
    try { const user = await msg.client.users.fetch(s.hakim); await user.send({ content: `دست اولیه شما (۵ کارت):\n${handToString(s.hands.get(s.hakim)!)}` }); } catch {}
    // Show suit selection panel in channel
    {
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
          const m = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
          if (m) { await m.edit({ embeds: [embed], components: [row] }); msgObj = m; }
        }
      } catch {}
      if (!msgObj) {
        msgObj = await (msg.channel as any).send({ embeds: [embed], components: [row] });
        s.tableMsgId = msgObj.id;
      }
    }
    await msg.reply({ content: `بازی آغاز شد. هدف ست‌ها: ${s.targetSets} (هر ست = ۷ دست). حاکم: <@${s.hakim}> — از دکمه‌های میز برای انتخاب حکم استفاده کن.` });
    return;
  }

  // .hokm hokm <suit> — hakim chooses trump; then deal remaining to all and DM hands
  if (isSubCmd('hokm','hokm')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'choosing_hokm' || !s.hakim) { await msg.reply('الان وقت انتخاب حکم نیست.'); return; }
    if (msg.author.id !== s.hakim) { await msg.reply('فقط حاکم می‌تواند حکم را انتخاب کند.'); return; }
    const arg = content.replace('.hokm hokm', '').trim();
    const suit = parseSuit(arg);
    if (!suit) { await msg.reply('خال نامعتبر. گزینه‌ها: ♠️ پیک، ♥️ دل، ♦️ خشت، ♣️ گیشنیز'); return; }
    s.hokm = suit;
    // delete any pending announce message now that hokm is chosen
    try {
      if (s.newSetAnnounceMsgId) {
        const am = await (msg.channel as any).messages.fetch(s.newSetAnnounceMsgId).catch(()=>null);
        if (am) await am.delete().catch(()=>{});
        s.newSetAnnounceMsgId = undefined;
      }
    } catch {}
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
      try { const user = await msg.client.users.fetch(uid); await user.send({ content: `حکم: ${SUIT_EMOJI[s.hokm]}\nدست شما:\n${handToString(s.hands.get(uid)!)}\nنوبت آغاز با حاکم <@${s.hakim}>` }); } catch {}
    }
    await msg.reply({ content: `حکم انتخاب شد: ${SUIT_EMOJI[s.hokm]} — نوبت آغاز با حاکم <@${s.hakim}>. با ".hokm play <کارت>" بازی کنید. مثال: .hokm play A${SUIT_EMOJI['S']}` });
    return;
  }

  // .hokm hand — DM your hand
  if (isSubCmd('hokm','hand')) {
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
  if (isSubCmd('hokm','table')) {
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
  if (isCmd('komak')) {
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
  if (isCmd('llset')) {
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
  if (isCmd('llunset')) {
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
      await msg.reply({ content: 'خطا در ساخت تصویر عشق. لطفاً کمی بعد دوباره تلاش کنید.' });
      return;
    } finally {
      llInFlight.delete(msg.id);
    }
  }

  // .e command
  if (isCmd('e')) {
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

  if (!isCmd('t')) return;

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
