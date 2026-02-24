import 'dotenv/config';
import { Client, GatewayIntentBits, Interaction, Message, EmbedBuilder, VoiceState, Collection, PermissionsBitField, ActionRowBuilder, ButtonBuilder, ButtonStyle, GuildMember, AttachmentBuilder, ActivityType, MessageFlags } from 'discord.js';
import fs from 'fs';
import path from 'path';
import http from 'http';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { PgFriendStore } from './storage/pgFriendStore';
import { handleTimerInteraction, TimerManager, parseDuration, makeTimerSetEmbed } from './modules/timerManager';

declare const require: any;

let createCanvas: any;
let GlobalFonts: any;
let loadImage: any;
type Canvas = any;
let canvasAvailable = false;

try {
  const canvasModule = require('@napi-rs/canvas') as any;
  createCanvas = canvasModule.createCanvas;
  GlobalFonts = canvasModule.GlobalFonts;
  loadImage = canvasModule.loadImage;
  canvasAvailable = true;
} catch (err) {
  console.warn('[canvas] @napi-rs/canvas not available, image features disabled');
  console.error('[canvas] load error:', err);
}

const token = process.env.BOT_TOKEN || '';
const ownerId = process.env.OWNER_ID || '';
const geminiApiKey = process.env.GEMINI_API_KEY || '';
const geminiModel = process.env.GEMINI_MODEL || 'gemini-1.5-flash';
const aiWebSearchMode = (process.env.AI_WEB_SEARCH_MODE || 'auto').toLowerCase();
const tavilyApiKey = process.env.TAVILY_API_KEY || '';
const braveApiKey = process.env.BRAVE_API_KEY || '';
const serpApiKey = process.env.SERPAPI_API_KEY || '';
const hfApiKey = process.env.HF_API_KEY || '';
const stabilityApiKey = process.env.STABILITY_API_KEY || '';
const sportsDbBaseUrl = 'https://www.thesportsdb.com/api/v1/json/3';

type ChatHistoryMessage = { role: 'user' | 'assistant'; content: string };
const chatHistories = new Map<string, ChatHistoryMessage[]>();

function getChatHistoryKey(userId: string, channelId: string): string {
  return `${channelId}:${userId}`;
}

function shouldUseWebSearch(query: string): boolean {
  if (aiWebSearchMode === 'off' || aiWebSearchMode === '0' || aiWebSearchMode === 'false') return false;
  if (aiWebSearchMode === 'always' || aiWebSearchMode === 'on' || aiWebSearchMode === '1' || aiWebSearchMode === 'true') return true;

  const q = query.toLowerCase();
  const keywords = [
    'قیمت',
    'نرخ',
    'الان',
    'امروز',
    'لحظه',
    'بروز',
    'بهروز',
    'به روز',
    'خبر',
    'اخبار',
    'دلار',
    'یورو',
    'پوند',
    'تتر',
    'طلا',
    'سکه',
    'بورس',
    'سهام',
    'btc',
    'bitcoin',
    'eth',
    'ethereum',
    'usd',
    'dollar',
    'price',
    'rate',
    'news',
    'today',
    'now',
    'weather',
    'آب و هوا',
    'match',
    'score',
    'scores',
    'live',
    'result',
    'game',
    'sports',
    'فوتبال',
    'والیبال',
    'بسکتبال',
    'نتیجه',
    'بازی',
    'مسابقه',
    'گل',
    'دقیقه',
  ];
  return keywords.some(k => q.includes(k));
}

async function fetchJsonWithTimeout<T>(url: string, init: RequestInit, timeoutMs: number): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...init, signal: controller.signal });
    if (!res.ok) {
      const errText = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status}${errText ? ` - ${errText}` : ''}`);
    }
    return (await res.json()) as T;
  } catch (err: any) {
    if (err?.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

async function convertImageUrlToGif(url: string): Promise<Buffer | null> {
  try {
    const ffmpegPath = require('ffmpeg-static') as string | null;
    if (!ffmpegPath) return null;
    const { spawn } = require('child_process');

    return await new Promise<Buffer | null>((resolve) => {
      const args = [
        '-v', 'error',
        '-i', url,
        '-vf', 'fps=15,scale=480:-1:flags=lanczos',
        '-loop', '0',
        '-f', 'gif',
        'pipe:1',
      ];

      const proc = spawn(ffmpegPath, args, { stdio: ['ignore', 'pipe', 'ignore'] });
      const chunks: Buffer[] = [];
      proc.stdout.on('data', (c: Buffer) => chunks.push(c));
      proc.on('error', () => resolve(null));
      proc.on('close', (code: number) => {
        if (code === 0 && chunks.length) {
          resolve(Buffer.concat(chunks));
        } else {
          resolve(null);
        }
      });
    });
  } catch (err) {
    console.error('[IMAGE GIF CONVERT ERROR]', err);
    return null;
  }
}

async function sportsDbGet<T>(endpoint: string, timeoutMs = 10_000): Promise<T> {
  const url = `${sportsDbBaseUrl}${endpoint}`;
  const data = await fetchJsonWithTimeout<T>(url, { method: 'GET' }, timeoutMs);
  return data;
}

type EmojiGgItem = {
  id?: number;
  title?: string;
  slug?: string;
  image?: string;
};

let emojiGgData: EmojiGgItem[] | null = null;
let emojiGgLoadedAt = 0;

async function ensureEmojiGgData(): Promise<EmojiGgItem[] | null> {
  const now = Date.now();
  if (emojiGgData && now - emojiGgLoadedAt < 24 * 60 * 60 * 1000) return emojiGgData;
  try {
    const data = await fetchJsonWithTimeout<any[]>('https://emoji.gg/api', { method: 'GET' }, 10_000);
    if (Array.isArray(data)) {
      emojiGgData = data.filter((x: any) => typeof x?.image === 'string' && x.image) as EmojiGgItem[];
      emojiGgLoadedAt = now;
      return emojiGgData;
    }
  } catch (err) {
    console.error('[emoji.gg] fetch error', err);
  }
  return null;
}

function normalizeEmojiGgName(value: string | undefined | null): string {
  if (!value) return '';
  return value
    .toLowerCase()
    .replace(/:/g, '')
    .replace(/_/g, '')
    .replace(/[^a-z0-9]/g, '');
}

async function findEmojiGgByNames(names: string[], preferGif: boolean): Promise<{ url: string; name: string }[]> {
  const list = await ensureEmojiGgData();
  if (!list) return [];
  const wanted = Array.from(
    new Set(
      names
        .map(n => normalizeEmojiGgName(n))
        .filter(n => n.length >= 2)
    )
  );
  if (!wanted.length) return [];
  const results: { url: string; name: string }[] = [];
  const used = new Set<string>();
  for (const item of list) {
    const image = typeof item.image === 'string' ? item.image : '';
    if (!image) continue;
    if (preferGif && !image.toLowerCase().endsWith('.gif')) continue;
    const titleNorm = normalizeEmojiGgName(item.title);
    const slugNorm = normalizeEmojiGgName(item.slug);
    let ok = false;
    if (titleNorm && wanted.includes(titleNorm)) ok = true;
    else if (slugNorm && wanted.includes(slugNorm)) ok = true;
    if (!ok) continue;
    if (used.has(image)) continue;
    used.add(image);
    const name = item.title || item.slug || 'emoji';
    results.push({ url: image, name });
  }
  return results;
}

function normalizeFootballQuery(input: string): string {
  let s = (input || '').trim();
  s = s
    .replace(/\u200c/g, ' ')
    .replace(/[ي]/g, 'ی')
    .replace(/[ك]/g, 'ک')
    .replace(/[ۀة]/g, 'ه')
    .replace(/[إأٱآ]/g, 'ا')
    .replace(/[ؤ]/g, 'و')
    .replace(/[ئ]/g, 'ی');
  s = s.replace(/[\u064B-\u065F\u0610-\u061A\u06D6-\u06ED]/g, '');
  s = s.toLowerCase();
  s = s.replace(/[_\-]+/g, ' ');
  s = s.replace(/[^\p{L}\p{N}\s]+/gu, ' ');
  s = s.replace(/\s+/g, ' ').trim();
  return s;
}

const footballTeamAliases = new Map<string, string>(
  [
    ['barca', 'barcelona'],
    ['fc barcelona', 'barcelona'],
    ['barcelona fc', 'barcelona'],
    ['بارسا', 'barcelona'],
    ['بارسلونا', 'barcelona'],
    ['barcelona', 'barcelona'],

    ['real madrid', 'real madrid'],
    ['رئال', 'real madrid'],
    ['رئال مادرید', 'real madrid'],

    ['atletico madrid', 'atletico madrid'],
    ['atletico', 'atletico madrid'],
    ['اتلتیکو', 'atletico madrid'],
    ['اتلتیکو مادرید', 'atletico madrid'],

    ['bayern', 'bayern munich'],
    ['bayern munich', 'bayern munich'],
    ['بایرن', 'bayern munich'],
    ['بایرن مونیخ', 'bayern munich'],

    ['man utd', 'manchester united'],
    ['man united', 'manchester united'],
    ['manchester united', 'manchester united'],
    ['منچستر یونایتد', 'manchester united'],
    ['من یو', 'manchester united'],
    ['یونایتد', 'manchester united'],

    ['man city', 'manchester city'],
    ['manchester city', 'manchester city'],
    ['منچستر سیتی', 'manchester city'],
    ['سیتی', 'manchester city'],

    ['liverpool', 'liverpool'],
    ['لیورپول', 'liverpool'],

    ['chelsea', 'chelsea'],
    ['چلسی', 'chelsea'],

    ['arsenal', 'arsenal'],
    ['آرسنال', 'arsenal'],

    ['tottenham', 'tottenham'],
    ['spurs', 'tottenham'],
    ['تاتنهام', 'tottenham'],

    ['psg', 'paris saint germain'],
    ['paris saint germain', 'paris saint germain'],
    ['paris sg', 'paris saint germain'],
    ['پاری سن ژرمن', 'paris saint germain'],
    ['پاریس', 'paris saint germain'],

    ['juventus', 'juventus'],
    ['juve', 'juventus'],
    ['یوونتوس', 'juventus'],
    ['یوونتس', 'juventus'],
    ['یوو', 'juventus'],

    ['inter', 'inter milan'],
    ['inter milan', 'inter milan'],
    ['internazionale', 'inter milan'],
    ['اینتر', 'inter milan'],
    ['اینتر میلان', 'inter milan'],

    ['ac milan', 'ac milan'],
    ['milan', 'ac milan'],
    ['میلان', 'ac milan'],
    ['ای سی میلان', 'ac milan'],

    ['roma', 'roma'],
    ['as roma', 'roma'],
    ['رم', 'roma'],
    ['آ اس رم', 'roma'],

    ['lazio', 'lazio'],
    ['لاتزیو', 'lazio'],

    ['napoli', 'napoli'],
    ['ناپولی', 'napoli'],

    ['dortmund', 'borussia dortmund'],
    ['borussia dortmund', 'borussia dortmund'],
    ['bvb', 'borussia dortmund'],
    ['دورتموند', 'borussia dortmund'],
  ].map(([k, v]) => [normalizeFootballQuery(k), v])
);

type SportsDbTeam = {
  idTeam: string;
  strTeam: string;
  strSport: string;
  strLeague: string;
  strCountry: string;
  strBadge: string | null;
  strLogo: string | null;
  strStadium: string | null;
  strGender: string;
};

type SportsDbEvent = {
  idEvent: string;
  strEvent: string;
  strHomeTeam: string;
  strAwayTeam: string;
  intHomeScore: string | null;
  intAwayScore: string | null;
  strTimestamp: string;
  dateEvent: string;
  strTime: string;
  strStatus: string;
  strLeague: string;
  idHomeTeam: string;
  idAwayTeam: string;
  strHomeTeamBadge: string | null;
  strAwayTeamBadge: string | null;
  strVenue: string | null;
};

type FootballTeam = {
  id: string;
  name: string;
  country: string | null;
  logo: string | null;
  venueName: string | null;
};

const footballTeamSearchCache = new Map<string, { at: number; value: FootballTeam | null }>();
const footballTeamSearchInFlight = new Map<string, Promise<FootballTeam | null>>();
const FOOTBALL_TEAM_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

function pickBestSportsDbTeam(candidates: SportsDbTeam[], searchTerm: string): SportsDbTeam | null {
  const q = normalizeFootballQuery(searchTerm);
  if (!candidates.length) return null;
  let best: { score: number; item: SportsDbTeam } | null = null;
  for (const item of candidates) {
    const name = item?.strTeam;
    if (!name || item?.strSport !== 'Soccer') continue;
    const n = normalizeFootballQuery(name);
    const country = (item?.strCountry || '').toLowerCase();
    let score = 0;
    
    if (n === q) score += 1000;
    if (n.startsWith(q)) score += 250;
    if (n.includes(q)) score += 120;
    
    const nameLower = name.toLowerCase();
    if (/\b(u\d{1,2}|under.?\d{1,2})\b/i.test(nameLower)) score -= 500;
    if (/\b(women|woman|femmes|feminine|femenino|ladies)\b/i.test(nameLower)) score -= 500;
    if (item?.strGender?.toLowerCase() === 'female') score -= 500;
    if (/\b(youth|junior|academy|reserves?)\b/i.test(nameLower)) score -= 500;
    if (/\b(ii|iii|iv|b team|c team)\b/i.test(nameLower)) score -= 500;
    if (/\b(u19|u21|u23)\b/i.test(nameLower)) score -= 500;
    
    const topLeagueCountries = ['england', 'spain', 'germany', 'italy', 'france', 'portugal', 'netherlands'];
    if (topLeagueCountries.includes(country)) score += 50;
    
    score -= Math.min(60, Math.max(0, n.length - q.length));
    if (!best || score > best.score) best = { score, item };
  }
  return best?.item ?? candidates[0] ?? null;
}

async function findFootballTeamByQuery(rawQuery: string): Promise<FootballTeam | null> {
  const rawNorm = normalizeFootballQuery(rawQuery);
  if (!rawNorm) return null;
  const searchTerm = footballTeamAliases.get(rawNorm) || rawQuery.trim();
  const cacheKey = normalizeFootballQuery(searchTerm);
  if (!cacheKey) return null;

  const now = Date.now();
  const cached = footballTeamSearchCache.get(cacheKey);
  // Temporarily disable cache for debugging
  // if (cached && now - cached.at < FOOTBALL_TEAM_CACHE_TTL_MS) return cached.value;

  const inFlight = footballTeamSearchInFlight.get(cacheKey);
  if (inFlight) return inFlight;

  const p = (async (): Promise<FootballTeam | null> => {
    const endpoint = `/searchteams.php?t=${encodeURIComponent(searchTerm)}`;
    console.log(`[FOOTBALL SEARCH] Searching for: "${searchTerm}" via ${sportsDbBaseUrl}${endpoint}`);
    const resp = await sportsDbGet<{ teams: SportsDbTeam[] | null }>(endpoint);
    const teams = resp?.teams || [];
    console.log(`[FOOTBALL SEARCH] Found ${teams.length} teams`);
    
    if (teams.length > 0) {
      teams.forEach((t, i) => {
        console.log(`[FOOTBALL SEARCH] Team ${i + 1}: ${t.strTeam} (ID: ${t.idTeam}, League: ${t.strLeague}, Country: ${t.strCountry}, Sport: ${t.strSport})`);
      });
    }
    
    const best = pickBestSportsDbTeam(teams, searchTerm);
    if (best) {
      console.log(`[FOOTBALL SEARCH] Selected best match: ${best.strTeam} (ID: ${best.idTeam})`);
    }
    
    const team: FootballTeam | null = best
      ? {
          id: best.idTeam,
          name: best.strTeam,
          country: best.strCountry ?? null,
          logo: best.strBadge ?? best.strLogo ?? null,
          venueName: best.strStadium ?? null,
        }
      : null;
    footballTeamSearchCache.set(cacheKey, { at: Date.now(), value: team });
    return team;
  })();

  footballTeamSearchInFlight.set(cacheKey, p);
  try {
    return await p;
  } finally {
    footballTeamSearchInFlight.delete(cacheKey);
  }
}

type FootballMatchData = {
  kind: 'next';
  event: SportsDbEvent;
  homeTeam: { name: string; logo: string | null };
  awayTeam: { name: string; logo: string | null };
};
const FOOTBALL_POPULAR_LEAGUES: { id: string; apiName: string; title: string; flag: string }[] = [
  { id: 'epl', apiName: 'English Premier League', title: 'لیگ برتر انگلیس', flag: '🏴' },
  { id: 'laliga', apiName: 'Spanish La Liga', title: 'لالیگا اسپانیا', flag: '🇪🇸' },
  { id: 'seriea', apiName: 'Italian Serie A', title: 'سری آ ایتالیا', flag: '🇮🇹' },
  { id: 'bundesliga', apiName: 'German Bundesliga', title: 'بوندسلیگا آلمان', flag: '🇩🇪' },
  { id: 'ligue1', apiName: 'French Ligue 1', title: 'لیگ ۱ فرانسه', flag: '🇫🇷' },
  { id: 'ucl', apiName: 'UEFA Champions League', title: 'لیگ قهرمانان اروپا', flag: '⭐' },
];

type SportsDbLeagueTeamsResponse = {
  teams: SportsDbTeam[] | null;
};

async function footballFetchTeamsByLeague(apiLeagueName: string): Promise<SportsDbTeam[]> {
  // TheSportsDB example uses underscores but API also accepts spaces when URL-encoded.
  const endpoint = `/search_all_teams.php?l=${encodeURIComponent(apiLeagueName)}`;
  console.log(`[FOOTBALL] Fetching teams for league: "${apiLeagueName}" via ${sportsDbBaseUrl}${endpoint}`);
  const resp = await sportsDbGet<SportsDbLeagueTeamsResponse>(endpoint);
  const teams = resp?.teams || [];
  console.log(`[FOOTBALL] League teams response for "${apiLeagueName}":`, teams.length, 'teams');
  return teams.filter(t => (t?.strSport === 'Soccer') && !!t?.strTeam);
}

function footballFormatMatchSummary(team: FootballTeam, data: FootballMatchData): string {
  const event = data.event;
  const ts = event.strTimestamp || `${event.dateEvent}T${event.strTime}`;
  const dateText = footballFormatDateFa(ts);
  const league = event.strLeague || '';
  const venue = event.strVenue || '';
  const status = (event.strStatus || '').trim();
  const home = event.strHomeTeam;
  const away = event.strAwayTeam;

  const lines: string[] = [];
  lines.push(`**${team.name}**`);
  lines.push('');
  if (league) lines.push(`🏆 لیگ: **${league}**`);
  lines.push(`⚽️ بازی: **${home}** vs **${away}**`);
  lines.push(`📅 زمان: ${dateText}`);
  if (venue) lines.push(`🏟️ ورزشگاه: ${venue}`);
  if (status) lines.push(`📌 وضعیت: ${status}`);
  lines.push('');
  lines.push('منبع: TheSportsDB.com');

  return lines.join('\n');
}

async function getNextEventForTeam(teamId: string, teamName: string): Promise<SportsDbEvent | null> {
  console.log(`[FOOTBALL] Fetching next events for team ${teamName} (ID: ${teamId})`);
  try {
    // Use event search by team name - more reliable than league endpoint
    const endpoint = `/searchevents.php?e=${encodeURIComponent(teamName)}`;
    console.log(`[FOOTBALL] API endpoint: ${sportsDbBaseUrl}${endpoint}`);
    const resp = await sportsDbGet<{ event: SportsDbEvent[] | null }>(endpoint);
    const events = resp?.event || [];
    console.log(`[FOOTBALL] Event search response:`, events.length, 'items');
    
    if (events.length === 0) {
      console.log(`[FOOTBALL] No events found for team ${teamName}`);
      return null;
    }
    
    // Filter to only events where our team is actually playing (match team ID)
    const teamEvents = events.filter(e => {
      const isHomeTeam = e.idHomeTeam === teamId;
      const isAwayTeam = e.idAwayTeam === teamId;
      return isHomeTeam || isAwayTeam;
    });
    
    console.log(`[FOOTBALL] Found ${teamEvents.length} events for team ID ${teamId}`);
    
    if (teamEvents.length === 0) {
      return null;
    }
    
    // Filter to only upcoming matches (not started yet)
    const upcoming = teamEvents
      .filter(e => e.strStatus === 'Not Started' || e.strStatus === '')
      .sort((a, b) => {
        const timeA = new Date(a.strTimestamp || a.dateEvent).getTime();
        const timeB = new Date(b.strTimestamp || b.dateEvent).getTime();
        return timeA - timeB;
      });
    
    console.log(`[FOOTBALL] Upcoming matches: ${upcoming.length}`);
    
    if (upcoming.length > 0) {
      console.log(`[FOOTBALL] Next match: ${upcoming[0].strEvent} on ${upcoming[0].dateEvent} (${upcoming[0].strLeague})`);
      return upcoming[0];
    }
    
    return null;
  } catch (err) {
    console.error(`[FOOTBALL] Error fetching events:`, err);
    throw err;
  }
}



async function getMatchDataForTeam(team: FootballTeam): Promise<FootballMatchData | null> {
  console.log(`[FOOTBALL] Getting next match for team ${team.name} (ID: ${team.id})`);
  
  const event = await getNextEventForTeam(team.id, team.name).catch((err) => {
    console.error(`[FOOTBALL] Next event error:`, err);
    return null;
  });
  
  if (!event) {
    console.log(`[FOOTBALL] No next event found for team ${team.name}`);
    return null;
  }
  
  console.log(`[FOOTBALL] Found next match for team ${team.name}: ${event.strEvent}`);
  return {
    kind: 'next',
    event,
    homeTeam: { name: event.strHomeTeam, logo: event.strHomeTeamBadge },
    awayTeam: { name: event.strAwayTeam, logo: event.strAwayTeamBadge },
  };
}


function footballFormatDateFa(dateStr: string): string {
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return dateStr;
  try {
    return d.toLocaleString('fa-IR', { timeZone: 'Asia/Tehran' });
  } catch {
    return d.toLocaleString('fa-IR');
  }
}

function footballEllipsize(ctx: any, input: string, maxWidth: number): string {
  const text = (input || '').trim();
  if (!text) return '';
  try {
    if (ctx.measureText(text).width <= maxWidth) return text;
    let t = text;
    while (t.length > 1 && ctx.measureText(`${t}…`).width > maxWidth) {
      t = t.slice(0, -1);
    }
    return t.length > 1 ? `${t}…` : text;
  } catch {
    return text;
  }
}

function footballValueToString(v: number | string | null | undefined): string {
  if (v === null || typeof v === 'undefined') return '';
  if (typeof v === 'number') return String(v);
  return String(v);
}


async function footballTryLoadImage(url: string | null | undefined): Promise<any | null> {
  if (!url || !loadImage) return null;
  try {
    return await loadImage(url);
  } catch {
    return null;
  }
}


async function renderFootballMatchImage(team: FootballTeam, data: FootballMatchData): Promise<Buffer> {
  if (!canvasAvailable || !createCanvas || !loadImage) {
    throw new Error('Canvas not available in this environment');
  }

  const size = { w: 1000, h: 500 };
  const canvas = createCanvas(size.w, size.h);
  const ctx = canvas.getContext('2d');

  ctx.fillStyle = '#1e1f22';
  ctx.fillRect(0, 0, size.w, size.h);

  const headerH = 70;
  const headerGrad = ctx.createLinearGradient(0, 0, size.w, 0);
  headerGrad.addColorStop(0, '#1b2a4a');
  headerGrad.addColorStop(1, '#3b1b4a');
  ctx.fillStyle = headerGrad;
  ctx.fillRect(0, 0, size.w, headerH);

  ctx.textBaseline = 'middle';
  ctx.fillStyle = '#ffffff';
  ctx.textAlign = 'left';
  ctx.font = ssdFontAvailable ? `bold 30px "${ssdFontFamily}"` : 'bold 30px Arial';
  ctx.fillText(footballEllipsize(ctx, team.name, size.w * 0.55), 28, Math.floor(headerH / 2));

  ctx.textAlign = 'right';
  ctx.font = ssdFontAvailable ? `bold 24px "${ssdFontFamily}"` : 'bold 24px Arial';
  ctx.fillText('بازی بعدی', size.w - 28, Math.floor(headerH / 2));

  const event = data.event;
  const [homeImg, awayImg] = await Promise.all([
    footballTryLoadImage(data.homeTeam.logo),
    footballTryLoadImage(data.awayTeam.logo),
  ]);

  ctx.textAlign = 'center';
  ctx.font = ssdFontAvailable ? `bold 18px "${ssdFontFamily}"` : 'bold 18px Arial';
  ctx.fillStyle = '#dfe3ea';
  ctx.fillText(footballEllipsize(ctx, event.strLeague || '', size.w * 0.85), Math.floor(size.w / 2), headerH + 24);

  const panelX = 20;
  const panelY = headerH + 50;
  const panelW = size.w - panelX * 2;
  const panelH = 220;
  ctx.fillStyle = '#2b2d31';
  ctx.fillRect(panelX, panelY, panelW, panelH);

  const logoSize = 110;
  const logoY = panelY + 20;
  const homeLogoX = panelX + 80;
  const awayLogoX = panelX + panelW - 80 - logoSize;
  
  if (homeImg) {
    try {
      ctx.drawImage(homeImg, homeLogoX, logoY, logoSize, logoSize);
    } catch {}
  }
  if (awayImg) {
    try {
      ctx.drawImage(awayImg, awayLogoX, logoY, logoSize, logoSize);
    } catch {}
  }

  const nameY = logoY + logoSize + 24;
  ctx.font = ssdFontAvailable ? `bold 20px "${ssdFontFamily}"` : 'bold 20px Arial';
  ctx.fillStyle = '#ffffff';
  ctx.textAlign = 'center';
  ctx.fillText(footballEllipsize(ctx, data.homeTeam.name, 280), homeLogoX + Math.floor(logoSize / 2), nameY);
  ctx.fillText(footballEllipsize(ctx, data.awayTeam.name, 280), awayLogoX + Math.floor(logoSize / 2), nameY);

  ctx.fillStyle = '#ffffff';
  ctx.font = ssdFontAvailable ? `bold 64px "${ssdFontFamily}"` : 'bold 64px Arial';
  ctx.textAlign = 'center';
  ctx.fillText('VS', Math.floor(size.w / 2), panelY + 90);

  const matchTimestamp = event.strTimestamp || `${event.dateEvent}T${event.strTime}`;
  const dateText = footballFormatDateFa(matchTimestamp);
  ctx.font = ssdFontAvailable ? `bold 18px "${ssdFontFamily}"` : 'bold 18px Arial';
  ctx.fillStyle = '#c9ced6';
  ctx.textAlign = 'center';
  ctx.fillText(footballEllipsize(ctx, dateText, size.w * 0.85), Math.floor(size.w / 2), panelY + 170);

  const venueLine = event.strVenue || '';
  if (venueLine) {
    ctx.font = ssdFontAvailable ? `16px "${ssdFontFamily}"` : '16px Arial';
    ctx.fillStyle = '#aeb4be';
    ctx.textAlign = 'center';
    ctx.fillText(footballEllipsize(ctx, `🏟️ ${venueLine}`, size.w * 0.85), Math.floor(size.w / 2), panelY + 198);
  }

  const bottomY = panelY + panelH + 20;
  ctx.font = ssdFontAvailable ? `16px "${ssdFontFamily}"` : '16px Arial';
  ctx.fillStyle = '#8a8f99';
  ctx.textAlign = 'center';
  ctx.fillText('📡 TheSportsDB.com - رایگان و بدون محدودیت', Math.floor(size.w / 2), bottomY + 20);

  return canvas.toBuffer('image/png');
}

// Minimal web search helper using DuckDuckGo Instant Answer API (no API key required).
// This is best-effort and may not always return fresh or detailed results.
async function webSearchSummary(query: string): Promise<string | null> {
  try {
    const qLower = query.toLowerCase();
    const hasFa = /[\u0600-\u06FF]/.test(query);
    const isFinance =
      /\b(usd|dollar|btc|bitcoin|eth|ethereum|price|rate|gold|coin|currency|fx)\b/.test(qLower) ||
      /قیمت|نرخ|دلار|یورو|پوند|تتر|طلا|سکه|بورس|سهام|ارز/.test(query);
    const isNews = /\bnews\b/.test(qLower) || /خبر|اخبار/.test(query);
    const isSports =
      /\b(match|score|scores|goal|goals|minute|league|vs|live)\b/.test(qLower) ||
      /فوتبال|والیبال|بسکتبال|گل|نتیجه|چند چند|دقیقه|بازی|مسابقه|لیگ|استقلال|پرسپولیس/.test(query);
    const needsFresh = /الان|امروز|لحظه|live|today|now|minute|score|price|rate/i.test(query);
    const topic = isFinance ? 'finance' : isNews ? 'news' : 'general';

    type ProviderSummary = { provider: string; text: string; urls: string[] };

    const providerTasks: Array<Promise<ProviderSummary | null>> = [];

    if (tavilyApiKey) {
      providerTasks.push(
        (async (): Promise<ProviderSummary | null> => {
          try {
            const body: any = {
              query,
              topic,
              search_depth: 'basic',
              max_results: 6,
              include_answer: 'basic',
              include_raw_content: false,
              time_range: needsFresh ? 'day' : undefined,
            };

            const data: any = await fetchJsonWithTimeout(
              'https://api.tavily.com/search',
              {
                method: 'POST' as const,
                headers: {
                  'Content-Type': 'application/json',
                  Authorization: `Bearer ${tavilyApiKey}`,
                },
                body: JSON.stringify(body),
              },
              9000
            );

            const parts: string[] = [];
            const urls: string[] = [];
            if (typeof data?.answer === 'string' && data.answer.trim()) {
              parts.push(data.answer.trim());
            }
            if (Array.isArray(data?.results)) {
              for (const r of data.results.slice(0, 6)) {
                const title = typeof r?.title === 'string' ? r.title : '';
                const url = typeof r?.url === 'string' ? r.url : '';
                const content = typeof r?.content === 'string' ? r.content : '';
                if (url) urls.push(url);
                const block = [title, url, content].filter(Boolean).join('\n');
                if (block) parts.push(block);
              }
            }
            const joined = parts.join('\n\n').trim();
            if (!joined) return null;
            return { provider: 'tavily', text: joined, urls };
          } catch {
            return null;
          }
        })()
      );
    }

    if (braveApiKey) {
      providerTasks.push(
        (async (): Promise<ProviderSummary | null> => {
          try {
            const params = new URLSearchParams();
            params.set('q', query);
            params.set('count', '6');
            params.set('country', hasFa ? 'IR' : 'US');
            params.set('search_lang', hasFa ? 'fa' : 'en');

            const data: any = await fetchJsonWithTimeout(
              `https://api.search.brave.com/res/v1/web/search?${params.toString()}`,
              {
                method: 'GET' as const,
                headers: {
                  'X-Subscription-Token': braveApiKey,
                },
              },
              9000
            );

            const results: any[] =
              (Array.isArray(data?.web?.results) && data.web.results) ||
              (Array.isArray(data?.results) && data.results) ||
              [];

            const parts: string[] = [];
            const urls: string[] = [];
            for (const r of results.slice(0, 6)) {
              const title = typeof r?.title === 'string' ? r.title : typeof r?.name === 'string' ? r.name : '';
              const url = typeof r?.url === 'string' ? r.url : typeof r?.link === 'string' ? r.link : '';
              const desc =
                typeof r?.description === 'string'
                  ? r.description
                  : typeof r?.snippet === 'string'
                    ? r.snippet
                    : '';
              const extraSnips = Array.isArray(r?.extra_snippets)
                ? (r.extra_snippets.filter((x: any) => typeof x === 'string') as string[]).slice(0, 2)
                : [];
              if (url) urls.push(url);
              const block = [title, url, desc, ...extraSnips].filter(Boolean).join('\n');
              if (block) parts.push(block);
            }

            const joined = parts.join('\n\n').trim();
            if (!joined) return null;
            return { provider: 'brave', text: joined, urls };
          } catch {
            return null;
          }
        })()
      );
    }

    if (serpApiKey) {
      providerTasks.push(
        (async (): Promise<ProviderSummary | null> => {
          try {
            const params = new URLSearchParams();
            params.set('engine', isSports ? 'google_sports_results' : 'google');
            params.set('q', query);
            params.set('api_key', serpApiKey);
            params.set('num', '7');
            params.set('hl', hasFa ? 'fa' : 'en');
            params.set('gl', hasFa ? 'ir' : 'us');

            const data: any = await fetchJsonWithTimeout(
              `https://serpapi.com/search.json?${params.toString()}`,
              { method: 'GET' as const },
              12000
            );

            if (typeof data?.error === 'string' && data.error.trim()) {
              return null;
            }

            const parts: string[] = [];
            const urls: string[] = [];

            const sports = data?.sports_results?.game_spotlight;
            if (isSports && sports && Array.isArray(sports?.teams)) {
              const status = typeof sports?.status === 'string' ? sports.status : '';
              const league = typeof sports?.league === 'string' ? sports.league : '';
              const stage = typeof sports?.stage === 'string' ? sports.stage : '';
              const timeHash = sports?.in_game_time;
              const minute = typeof timeHash?.minute === 'number' ? timeHash.minute : null;
              const stoppage = typeof timeHash?.stoppage === 'number' ? timeHash.stoppage : 0;
              const timeText = minute !== null ? `${minute}${stoppage ? `+${stoppage}` : ''}` : '';

              const teams = sports.teams as any[];
              const teamLine = teams
                .map(t => {
                  const name = typeof t?.name === 'string' ? t.name : '';
                  const score =
                    typeof t?.score === 'string'
                      ? t.score
                      : typeof t?.score === 'number'
                        ? String(t.score)
                        : '';
                  const pen = typeof t?.penalty_score === 'number' ? ` (pen ${t.penalty_score})` : '';
                  return [name, score ? `(${score})` : '', pen].filter(Boolean).join(' ');
                })
                .filter(Boolean)
                .join(' vs ');

              const lines: string[] = [];
              lines.push([league, stage].filter(Boolean).join(' — '));
              lines.push([status, timeText ? `دقیقه ${timeText}` : ''].filter(Boolean).join(' — '));
              if (teamLine) lines.push(teamLine);

              for (const t of teams) {
                const tName = typeof t?.name === 'string' ? t.name : '';
                const goalSummary = Array.isArray(t?.goal_summary) ? (t.goal_summary as any[]) : [];
                const scorerBits: string[] = [];
                for (const gs of goalSummary.slice(0, 12)) {
                  const pName = typeof gs?.player?.name === 'string' ? gs.player.name : '';
                  const goalsArr = Array.isArray(gs?.goals) ? (gs.goals as any[]) : [];
                  const times: string[] = [];
                  for (const g of goalsArr.slice(0, 6)) {
                    const gt = g?.in_game_time;
                    const m = typeof gt?.minute === 'number' ? gt.minute : null;
                    const st = typeof gt?.stoppage === 'number' ? gt.stoppage : 0;
                    if (m !== null) times.push(`${m}${st ? `+${st}` : ''}`);
                  }
                  if (pName && times.length) scorerBits.push(`${pName} (${times.join('، ')}')`);
                }
                if (tName && scorerBits.length) {
                  lines.push(`گل‌های ${tName}: ${scorerBits.join(' | ')}`);
                }
              }

              const watchOn = typeof sports?.watch_on === 'string' ? sports.watch_on : '';
              if (watchOn) {
                urls.push(watchOn);
                lines.push(watchOn);
              }
              const highlight = typeof sports?.video_highlights?.link === 'string' ? sports.video_highlights.link : '';
              if (highlight) {
                urls.push(highlight);
                lines.push(highlight);
              }

              const joinedSports = lines.filter(Boolean).join('\n').trim();
              if (joinedSports) parts.push(joinedSports);
            }

            if (Array.isArray(data?.organic_results)) {
              for (const r of (data.organic_results as any[]).slice(0, 6)) {
                const title = typeof r?.title === 'string' ? r.title : '';
                const link = typeof r?.link === 'string' ? r.link : '';
                const snippet = typeof r?.snippet === 'string' ? r.snippet : '';
                if (link) urls.push(link);
                const block = [title, link, snippet].filter(Boolean).join('\n');
                if (block) parts.push(block);
              }
            }

            const joined = parts.join('\n\n').trim();
            if (!joined) return null;
            return { provider: 'serpapi', text: joined, urls };
          } catch {
            return null;
          }
        })()
      );
    }

    const settled = await Promise.allSettled(providerTasks);
    const providerSummaries: ProviderSummary[] = [];
    for (const s of settled) {
      if (s.status === 'fulfilled' && s.value) providerSummaries.push(s.value);
    }

    const ddgFallback = async (): Promise<string | null> => {
      try {
        const url = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
        const res = await fetch(url, { method: 'GET' as const });
        if (!res.ok) return null;
        const data: any = await res.json();

        const parts: string[] = [];
        if (data.AbstractText) {
          parts.push(data.AbstractText as string);
        }
        if (Array.isArray(data.RelatedTopics)) {
          for (const topic of data.RelatedTopics.slice(0, 3)) {
            if (topic && typeof topic.Text === 'string') {
              parts.push(topic.Text as string);
            } else if (topic && topic.Topics && Array.isArray(topic.Topics)) {
              for (const t of topic.Topics.slice(0, 2)) {
                if (t && typeof t.Text === 'string') parts.push(t.Text as string);
              }
            }
          }
        }
        if (!parts.length) return null;
        return parts.join('\n');
      } catch {
        return null;
      }
    };

    if (providerSummaries.length === 0) {
      return await ddgFallback();
    }

    const fetchedAt = new Date().toISOString();
    const header = `زمان دریافت نتایج: ${fetchedAt}`;
    const blocks: string[] = [header];
    const allUrls = new Set<string>();

    for (const ps of providerSummaries) {
      blocks.push(`\n---\n[${ps.provider}]\n${ps.text}`);
      for (const u of ps.urls) {
        if (u) allUrls.add(u);
      }
    }

    if (allUrls.size > 0) {
      blocks.push(`\n---\nمنابع (URLs):\n${Array.from(allUrls).slice(0, 15).join('\n')}`);
    }

    const joined = blocks.join('\n').trim();
    return joined.length > 4500 ? joined.slice(0, 4500) + '…' : joined;

  } catch {
    return null;
  }
 }

async function generateAiReply(
  prompt: string,
  userId: string,
  channelId: string,
  replyText?: string,
  replyImageUrl?: string | null
): Promise<string> {
  if (!geminiApiKey) {
    throw new Error('GEMINI_API_KEY is not set');
  }

  const genAI = new GoogleGenerativeAI(geminiApiKey);
  const modelCandidates = Array.from(new Set([
    geminiModel,
    'gemini-1.5-flash',
    'gemini-1.5-pro',
    'gemini-1.0-pro',
    'gemini-pro'
  ]));
  
  const historyKey = getChatHistoryKey(userId, channelId);
  const history = chatHistories.get(historyKey) || [];

  let baseText = prompt;
  if (replyText) {
    baseText = `پیام ارجاع‌شده:\n"${replyText}"\n\nدرخواست من: ${prompt}`;
  }

  const webSummary = shouldUseWebSearch(baseText) ? await webSearchSummary(baseText).catch(() => null) : null;
  
  const contents: any[] = [
    {
      role: 'user',
      parts: [{
        text: 'You are a helpful Persian-speaking assistant inside a Discord bot.\n' +
              'Always answer in Persian unless the user explicitly asks for another language.\n' +
              'For time-sensitive questions (live sports scores, prices, news, weather), do not guess. If web search results are provided, use them as the primary source of truth. If the provided results are insufficient, say so.\n' +
              'When using web information, include a final section named "منابع:" with 2 to 5 source URLs.\n' +
              'If sources conflict, mention the conflict briefly and prefer the most recent/credible source.\n' +
              'Be clear and reasonably concise, but include key details when available (e.g., score + minute + scorers/times for live matches).\n' +
              'Avoid explicit hate, threats, or sexual content. Be respectful.'
      }],
    },
    {
      role: 'model',
      parts: [{ text: 'متوجه شدم. من به عنوان یک دستیار فارسی‌زبان در دیسکورد عمل می‌کنم و قوانین ذکر شده را رعایت خواهم کرد.' }],
    }
  ];

  // Add history
  for (const h of history) {
    contents.push({
      role: h.role === 'user' ? 'user' : 'model',
      parts: [{ text: h.content }],
    });
  }

  // Add web search context if available
  if (webSummary) {
    contents.push({
      role: 'user',
      parts: [{
        text: 'نتایج جستجوی وب برای پرسش فعلی (برای پاسخ دقیق و استناد):\n' +
          webSummary +
          '\n\nقوانین: درباره اطلاعات زمان-حساس فقط بر اساس این نتایج جواب بده و حدس نزن. اگر کافی نیست بگو اطلاعات کافی پیدا نشد. در انتهای پاسخ حتماً بخش «منابع:» را با چند لینک معتبر بنویس. اگر منابع متناقض‌اند، تناقض را توضیح بده و منبع تازه‌تر/معتبرتر را ترجیح بده.'
      }],
    });
    contents.push({
      role: 'model',
      parts: [{ text: 'متوجه شدم. از اطلاعات وب برای پاسخ دقیق‌تر استفاده می‌کنم.' }],
    });
  }

  // Current prompt + Image
  const currentParts: any[] = [{ text: baseText }];
  
  if (replyImageUrl) {
    try {
      const response = await fetch(replyImageUrl);
      const buffer = await response.arrayBuffer();
      const base64Image = Buffer.from(buffer).toString('base64');
      const mimeType = response.headers.get('content-type') || 'image/jpeg';
      
      currentParts.push({
        inlineData: {
          data: base64Image,
          mimeType: mimeType
        }
      });
    } catch (err) {
      console.error('[GEMINI IMAGE FETCH ERROR]', err);
    }
  }

  contents.push({
    role: 'user',
    parts: currentParts,
  });

  try {
    let lastErr: any;
    for (const modelName of modelCandidates) {
      try {
        const model = genAI.getGenerativeModel({ model: modelName }, { apiVersion: 'v1' });
        const result = await model.generateContent({ contents });
        const response = await result.response;
        const text = response.text();
        
        if (!text) {
          throw new Error('Empty response from Gemini');
        }

        const trimmed = text.trim();

        // Update history
        const updatedHistory: ChatHistoryMessage[] = [
          ...history,
          { role: 'user', content: baseText },
          { role: 'assistant', content: trimmed },
        ];
        if (updatedHistory.length > 10) {
          updatedHistory.splice(0, updatedHistory.length - 10);
        }
        chatHistories.set(historyKey, updatedHistory);

        return trimmed;
      } catch (err: any) {
        lastErr = err;
        const status = err?.status ?? err?.response?.status;
        if (status === 404) {
          continue;
        }
        throw err;
      }
    }

    throw lastErr || new Error('No available Gemini models');
    
  } catch (err: any) {
    console.error('[GEMINI API ERROR]', err);
    throw err;
  }
}

async function translatePromptToEnglishForImage(originalPrompt: string): Promise<string> {
  if (!geminiApiKey) {
    throw new Error('GEMINI_API_KEY is not set');
  }

  const genAI = new GoogleGenerativeAI(geminiApiKey);
  const modelCandidates = Array.from(new Set([
    geminiModel,
    'gemini-1.5-flash',
    'gemini-1.5-pro',
    'gemini-1.0-pro',
    'gemini-pro'
  ]));

  try {
    const contents = [
      {
        role: 'user',
        parts: [{
          text: 'You are a professional prompt engineer for text-to-image models. ' +
                'The user will send a prompt in Persian (Farsi) or mixed Persian/English. ' +
                'Translate it into a single, fluent, detailed English prompt optimized for image generation. ' +
                'Preserve all visual details, styles, composition, camera information, lighting, and mood from the original. ' +
                'If the input is already in English, keep it and only make very small clarity improvements. ' +
                'Do not add new concepts that are not implied by the original. ' +
                'Return only the final English prompt, without quotation marks or any extra explanation.'
        }]
      },
      {
        role: 'model',
        parts: [{ text: 'Understood. I will provide only the final English prompt optimized for image generation.' }]
      },
      {
        role: 'user',
        parts: [{ text: originalPrompt }]
      }
    ];

    let lastErr: any;
    for (const modelName of modelCandidates) {
      try {
        const model = genAI.getGenerativeModel({ model: modelName }, { apiVersion: 'v1' });
        const result = await model.generateContent({ contents });
        const response = await result.response;
        return response.text().trim() || originalPrompt;
      } catch (err: any) {
        lastErr = err;
        const status = err?.status ?? err?.response?.status;
        if (status === 404) {
          continue;
        }
        throw err;
      }
    }

    throw lastErr || new Error('No available Gemini models');
  } catch (err) {
    console.error('[GEMINI TRANSLATE ERROR]', err);
    return originalPrompt;
  }
}

async function generateImageWithStability(prompt: string): Promise<Buffer> {
  if (!stabilityApiKey) {
    throw new Error('STABILITY_API_KEY is not set');
  }

  const form = new (globalThis as any).FormData();
  form.append('prompt', prompt);
  form.append('output_format', 'png');
  form.append('aspect_ratio', '16:9');
  form.append('none', '');

  const res = await fetch('https://api.stability.ai/v2beta/stable-image/generate/ultra', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${stabilityApiKey}`,
      Accept: 'image/*',
    },
    body: form as any,
  });

  if (!res.ok) {
    let errDetail = '';
    try {
      const contentType = res.headers.get('content-type') || '';
      if (contentType.includes('application/json')) {
        const errJson = await res.json();
        errDetail = JSON.stringify(errJson);
      } else {
        errDetail = await res.text();
      }
    } catch {
      // ignore parse errors
    }
    throw new Error(`Stability API error: ${res.status}${errDetail ? ` - ${errDetail}` : ''}`);
  }

  const arrayBuffer = await res.arrayBuffer();
  const buffer = Buffer.from(arrayBuffer);
  if (!buffer || buffer.length === 0) {
    throw new Error('Empty image output from Stability');
  }

  return buffer;
}

// Bot ready status for health checks
let botReady = false;

// Emoji font registration (optional)
let emojiFontAvailable = false;
if (canvasAvailable) {
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
  // Clear card order cache so cards are re-sorted with hokm
  clearHandOrderCache(s);
  // Schedule announce message deletion after 2.5 seconds
  await scheduleAnnounceMessageDeletion(channel, s);
  const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
  for (const uid of s.order) {
    const need = 13 - (s.hands.get(uid)?.length || 0);
    give(uid, need);
  }
  s.state = 'playing';
  s.leaderIndex = s.order.indexOf(s.hakim); if (s.leaderIndex < 0) s.leaderIndex = 0;
  s.turnIndex = s.leaderIndex; s.table = []; s.leadSuit = null; s.tricksTeam1 = 0; s.tricksTeam2 = 0;
  s.tricksByPlayer = new Map(); s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
  
  // فقط refreshTableEmbed را صدا می‌زنیم که تصویر و دکمه‌ها را نمایش می‌دهد
  // forceRender=true چون شروع بازی جدید است
  if (channel) await refreshTableEmbed({ channel }, s, true);
  // Start turn timeout for the first player
  await startTurnTimeout(client, s);
  await maybeBotAutoPlay(client, s);
}

// تابع displayTable حذف شد - refreshTableEmbed جایگزین آن است

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

function buildControlButtons(sessionId: string = 'legacy'): ActionRowBuilder<ButtonBuilder>[] {
  const row1 = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`hokm-join-t1-${sessionId}`).setLabel('🔵 تیم 1').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId(`hokm-join-t2-${sessionId}`).setLabel('🔴 تیم 2').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId(`hokm-leave-${sessionId}`).setLabel('🔙 خروج').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-start-${sessionId}`).setLabel('🏁 شروع').setStyle(ButtonStyle.Danger),
  );
  const row2 = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`hokm-bot-add-t1-${sessionId}`).setLabel('🤖 بات 1').setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId(`hokm-bot-add-t2-${sessionId}`).setLabel('🤖 بات 2').setStyle(ButtonStyle.Success),
    new ButtonBuilder().setCustomId(`hokm-bot-remove-t1-${sessionId}`).setLabel('❌ حذف 1').setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-bot-remove-t2-${sessionId}`).setLabel('❌ حذف 2').setStyle(ButtonStyle.Secondary),
  );
  return [row1, row2];
}
interface HokmSession {
  sessionId: string; // unique identifier for this game session
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
  // surrender votes
  surrenderVotesTeam1?: Set<string>; // userIds who voted to surrender from team1
  surrenderVotesTeam2?: Set<string>; // userIds who voted to surrender from team2
  // Kot and Hakem Kot tracking
  kotTeam1?: number; // Count of Kot (7-0 win when hakim team) for Team 1
  kotTeam2?: number; // Count of Kot for Team 2
  hakemKotTeam1?: number; // Count of Hakem Kot (7-0 win when NOT hakim team) for Team 1
  hakemKotTeam2?: number; // Count of Hakem Kot for Team 2
  // Total tricks across all sets
  allTricksByPlayer?: Map<string, number>; // userId -> total tricks across all sets
  // Turn timeout tracking
  lastTurnTime?: number; // timestamp of last turn start
  turnTimeoutId?: NodeJS.Timeout; // timeout handle for auto-forfeit
  // Smart rendering optimization
  lastRenderedHash?: string; // hash of last rendered state to avoid unnecessary re-renders
}

// ===== Turn Timeout Management =====
const TURN_TIMEOUT_MS = 60 * 60 * 1000; // 1 hour in milliseconds

function clearTurnTimeout(s: HokmSession) {
  if (s.turnTimeoutId) {
    clearTimeout(s.turnTimeoutId);
    s.turnTimeoutId = undefined;
  }
}

async function startTurnTimeout(client: Client, s: HokmSession) {
  // Clear any existing timeout
  clearTurnTimeout(s);
  
  // Don't set timeout for bots
  if (s.turnIndex !== undefined && isVirtualBot(s.order[s.turnIndex])) {
    return;
  }
  
  // Set last turn time
  s.lastTurnTime = Date.now();
  
  // Set new timeout
  s.turnTimeoutId = setTimeout(async () => {
    try {
      // Verify game is still in playing state
      if (s.state !== 'playing' || s.turnIndex === undefined) {
        return;
      }
      
      const currentPlayerId = s.order[s.turnIndex];
      const playerTeam = s.team1.includes(currentPlayerId) ? 1 : 2;
      
      console.log(`[TIMEOUT] Player ${currentPlayerId} (Team ${playerTeam}) timed out after 1 hour`);
      
      // Award all remaining sets to opponent team
      const targetSets = s.targetSets ?? 1;
      s.setsTeam1 = s.setsTeam1 || 0;
      s.setsTeam2 = s.setsTeam2 || 0;
      
      // Set winner team to have enough sets to win
      if (playerTeam === 1) {
        s.setsTeam2 = targetSets;
      } else {
        s.setsTeam1 = targetSets;
      }
      
      s.state = 'finished';
      
      // Update stats
      try {
        const t1 = s.team1;
        const t2 = s.team2;
        const winners = playerTeam === 1 ? t2 : t1;
        
        // Add games played
        for (const u of [...t1, ...t2]) {
          ensureUserStat(s.guildId, u).games += 1;
        }
        
        // Add wins
        for (const u of winners) {
          ensureUserStat(s.guildId, u).wins += 1;
        }
        
        // Add tricks to stats
        if (s.allTricksByPlayer) {
          for (const [uid, tricks] of s.allTricksByPlayer.entries()) {
            const stat = ensureUserStat(s.guildId, uid);
            stat.tricks = (stat.tricks || 0) + tricks;
          }
        }
        
        // Add current set tricks to total
        if (s.tricksByPlayer) {
          for (const [uid, tricks] of s.tricksByPlayer.entries()) {
            const stat = ensureUserStat(s.guildId, uid);
            stat.tricks = (stat.tricks || 0) + tricks;
            const totalTricks = (s.allTricksByPlayer?.get(uid) || 0) + tricks;
            if (s.allTricksByPlayer) {
              s.allTricksByPlayer.set(uid, totalTricks);
            }
          }
        }
        
        // Add Kot and Hakem Kot stats properly - winners and losers
        // Team 1 kots and hakem kots
        if (s.kotTeam1 && s.kotTeam1 > 0) {
          // Add kot to team 1 players
          for (const u of t1) {
            const stat = ensureUserStat(s.guildId, u);
            stat.kot = (stat.kot || 0) + s.kotTeam1;
          }
          // Add kot losses to team 2 players
          for (const u of t2) {
            const stat = ensureUserStat(s.guildId, u);
            stat.kotLose = (stat.kotLose || 0) + s.kotTeam1;
          }
        }
        
        // Team 2 kots
        if (s.kotTeam2 && s.kotTeam2 > 0) {
          // Add kot to team 2 players
          for (const u of t2) {
            const stat = ensureUserStat(s.guildId, u);
            stat.kot = (stat.kot || 0) + s.kotTeam2;
          }
          // Add kot losses to team 1 players
          for (const u of t1) {
            const stat = ensureUserStat(s.guildId, u);
            stat.kotLose = (stat.kotLose || 0) + s.kotTeam2;
          }
        }
        
        // Team 1 hakem kots
        if (s.hakemKotTeam1 && s.hakemKotTeam1 > 0) {
          // Add hakem kot to team 1 players
          for (const u of t1) {
            const stat = ensureUserStat(s.guildId, u);
            stat.hakemKot = (stat.hakemKot || 0) + s.hakemKotTeam1;
          }
          // Add hakem kot losses to team 2 players
          for (const u of t2) {
            const stat = ensureUserStat(s.guildId, u);
            stat.hakemKotLose = (stat.hakemKotLose || 0) + s.hakemKotTeam1;
          }
        }
        
        // Team 2 hakem kots
        if (s.hakemKotTeam2 && s.hakemKotTeam2 > 0) {
          // Add hakem kot to team 2 players
          for (const u of t2) {
            const stat = ensureUserStat(s.guildId, u);
            stat.hakemKot = (stat.hakemKot || 0) + s.hakemKotTeam2;
          }
          // Add hakem kot losses to team 1 players
          for (const u of t1) {
            const stat = ensureUserStat(s.guildId, u);
            stat.hakemKotLose = (stat.hakemKotLose || 0) + s.hakemKotTeam2;
          }
        }
        
        // Add teammate wins
        if (winners.length === 2) {
          const [a, b] = winners;
          ensureUserStat(s.guildId, a).teammateWins[b] = (ensureUserStat(s.guildId, a).teammateWins[b] || 0) + 1;
          ensureUserStat(s.guildId, b).teammateWins[a] = (ensureUserStat(s.guildId, b).teammateWins[a] || 0) + 1;
        }
        
        saveHokmStats();
      } catch (err) {
        console.error('[TIMEOUT STATS ERROR]:', err);
      }
      
      // Get channel and send result
      try {
        const channel = await client.channels.fetch(s.channelId).catch(() => null) as any;
        if (channel) {
          // forceRender=true چون timeout و پایان بازی
          await refreshTableEmbed({ channel }, s, true);
          
          const t1Set = s.setsTeam1 ?? 0;
          const t2Set = s.setsTeam2 ?? 0;
          const starter = s.ownerId ? `<@${s.ownerId}>` : '—';
          
          const lines: string[] = [];
          lines.push(`### ✹Starter: ${starter}`);
          lines.push(`### ✹Sets: ${s.targetSets ?? 1}`);
          lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬●');
          lines.push(`### ✹Team 1: ${s.team1.map(u => `<@${u}>`).join(' , ')} ➤ ${t1Set}`);
          lines.push('════════════════════');
          lines.push(`### ✹Team 2: ${s.team2.map(u => `<@${u}>`).join(' , ')} ➤ ${t2Set}`);
          lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬●');
          lines.push(`### ✹Winner: Team ${t1Set > t2Set ? 1 : 2} ✅`);
          lines.push(`\n**⏰ بازیکن <@${currentPlayerId}> (تیم ${playerTeam}) به دلیل عدم بازی به مدت 1 ساعت، بازی را واگذار کرد.**`);
          
          // Add final stats
          if (s.allTricksByPlayer && s.allTricksByPlayer.size > 0) {
            lines.push('\n**📊 آمار نهایی:**');
            for (const [uid, tricks] of s.allTricksByPlayer.entries()) {
              const currentSetTricks = s.tricksByPlayer?.get(uid) || 0;
              const totalTricks = tricks + currentSetTricks;
              lines.push(`<@${uid}>: ${totalTricks} تریک`);
            }
          }
          
          // Add Kot/Hakem Kot stats if any
          if ((s.kotTeam1 ?? 0) > 0 || (s.kotTeam2 ?? 0) > 0 || (s.hakemKotTeam1 ?? 0) > 0 || (s.hakemKotTeam2 ?? 0) > 0) {
            lines.push('\n**🏆 کت و حاکم کت:**');
            if ((s.kotTeam1 ?? 0) > 0) lines.push(`تیم 1: ${s.kotTeam1} کت`);
            if ((s.kotTeam2 ?? 0) > 0) lines.push(`تیم 2: ${s.kotTeam2} کت`);
            if ((s.hakemKotTeam1 ?? 0) > 0) lines.push(`تیم 1: ${s.hakemKotTeam1} حاکم کت`);
            if ((s.hakemKotTeam2 ?? 0) > 0) lines.push(`تیم 2: ${s.hakemKotTeam2} حاکم کت`);
          }
          
          const emb = new EmbedBuilder()
            .setDescription(lines.join('\n'))
            .setColor(t1Set > t2Set ? 0x3b82f6 : 0xef4444);
          
          await channel.send({ embeds: [emb] });
        }
      } catch (err) {
        console.error('[TIMEOUT RESULT ERROR]:', err);
      }
    } catch (err) {
      console.error('[TIMEOUT HANDLER ERROR]:', err);
    }
  }, TURN_TIMEOUT_MS);
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
      // حریف برنده است - منطق تهاجمی: حتماً سر کن یا برش بده
      const ace = follow.find(c=>c.r===14);
      if (ace) return ace;
      const wc = table[w].card;
      const better = follow.filter(c=>beatCard(c,wc));
      if (better.length) {
        // اگر K داری و حریف برنده است، بدون چک A حتماً K بزن
        const king = better.find(c=>c.r===13);
        if (king) return king;
        // یا بزرگترین کارت بالاتر از حریف
        return maxCard(better);
      }
      // نمی‌تونی سر کنی، کوچکترین بزن
      return minCard(follow);
    } else {
      // نمی‌تونی خال شروع بازی کنی
      if (mateCard.r===14 || (mateCard.r===13&&isAcePlayed(lead))) return minNonTrump(legal);
      if (mateWins && oppCard.s!==trump) return minNonTrump(legal);
      // حریف برنده است - حتماً برش بده
      const trumps = legal.filter(c=>c.s===trump);
      if (!trumps.length) return minNonTrump(legal);
      if (oppCard.s===trump) {
        // حریف با حکم برنده - باید بالاتر از حکم حریف بزنی
        const better = trumps.filter(c=>c.r>oppCard.r);
        return better.length? minCard(better) : minNonTrump(legal);
      }
      // حریف با غیر حکم برنده - با کوچکترین حکم برش بده
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
  if (hand.length===0) {
    console.error(`[BOT ERROR] Bot ${uid} has no cards in hand! State: ${s.state}, Turn: ${s.turnIndex}`);
    return;
  }
  // Reduced delay for faster bot play (300ms instead of 500ms)
  setTimeout(async () => {
    try {
      // Re-verify state hasn't changed
      if (s.state!=='playing' || s.turnIndex==null) return;
      if (s.order[s.turnIndex] !== uid) return; // Turn changed
      
      const currentHand = s.hands.get(uid) || [];
      if (currentHand.length === 0) {
        console.error(`[BOT ERROR] Bot ${uid} hand is empty at play time!`);
        return;
      }
      
      // compute legal card
      const card = chooseBotCard(currentHand, s);
      if (!card) {
        console.error(`[BOT ERROR] Bot ${uid} could not choose a card!`);
        return;
      }
      
      // play
      const idx = currentHand.findIndex(c=>c.s===card.s && c.r===card.r);
      if (idx<0) {
        console.error(`[BOT ERROR] Bot ${uid} card not found in hand!`);
        return;
      }
      currentHand.splice(idx,1); 
      s.hands.set(uid, currentHand);
      s.table = s.table || []; 
      s.table.push({ userId: uid, card });
      if (!s.leadSuit) s.leadSuit = card.s;
      const nextTurn = ((s.turnIndex ?? 0) + 1) % s.order.length;
      s.turnIndex = nextTurn;
      
      // Clear timeout since bot played
      clearTurnTimeout(s);
      
      // Only render if we're at end of trick or if it's a human's turn next
      const shouldRender = s.table.length === 4 || !isVirtualBot(s.order[s.turnIndex]);
      if (shouldRender) {
        let ch: any = null; 
        try { ch = await client.channels.fetch(s.channelId).catch(()=>null); } catch {}
        // forceRender=false - حرکت عادی بات
        if (ch) await refreshTableEmbed({ channel: ch }, s, false);
      }
      
      // resolve trick if complete
      if (s.table.length === 4) {
        await resolveTrickAndContinue({ client } as any, s);
      } else {
        // Start timeout for next player if it's a human
        if (!isVirtualBot(s.order[s.turnIndex])) {
          await startTurnTimeout(client, s);
        }
        // Continue with next bot (non-recursive for better performance)
        setImmediate(() => maybeBotAutoPlay(client, s));
      }
    } catch (err) {
      console.error('[BOT ERROR] Exception in maybeBotAutoPlay:', err);
    }
  }, 300); // Faster bot response
}

// ===== Hokm Stats =====
type HokmUserStat = {
  games: number;
  wins: number;
  teammateWins: Record<string, number>;
  hokmPicks: Partial<Record<Suit, number>>;
  tricks?: number; // Total tricks won
  sets?: number; // Total sets won
  kot?: number; // Total Kot (7-0 win when hakim)
  hakemKot?: number; // Total Hakem Kot (7-0 win when not hakim)
  kotLose?: number; // Total Kot loses
  hakemKotLose?: number; // Total Hakem Kot loses
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
  if (!st) { 
    st = { 
      games: 0, 
      wins: 0, 
      teammateWins: {}, 
      hokmPicks: {},
      tricks: 0,
      sets: 0,
      kot: 0,
      hakemKot: 0,
      kotLose: 0,
      hakemKotLose: 0
    }; 
    g.set(uid, st); 
  }
  return st;
}
function addHokmPick(gId: string, uid: string, suit: Suit) {
  const st = ensureUserStat(gId, uid);
  st.hokmPicks[suit] = (st.hokmPicks[suit] || 0) + 1;
}
loadHokmStats();
const hokmSessions = new Map<string, HokmSession>(); // key: guildId:channelId:sessionId
function keyGCS(g: string, c: string, s: string){ return `${g}:${c}:${s}`; }
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
  
  // Determine same color relationship
  const sameColor = (s: Suit): boolean => {
    if (s === hokm) return false; // hokm is not "same color", it's hokm itself
    if ((s==='H'&&hokm==='D')||(s==='D'&&hokm==='H')) return true;
    if ((s==='S'&&hokm==='C')||(s==='C'&&hokm==='S')) return true;
    return false;
  };
  
  // Count cards per suit
  const countMap = new Map<Suit,number>();
  hand.forEach(c=>countMap.set(c.s, (countMap.get(c.s)||0)+1));
  
  return [...hand].sort((a,b)=>{
    const aIsHokm = a.s === hokm;
    const bIsHokm = b.s === hokm;
    const aIsSameColor = !aIsHokm && sameColor(a.s);
    const bIsSameColor = !bIsHokm && sameColor(b.s);
    
    // 1. Hokm suit comes first
    if (aIsHokm && !bIsHokm) return -1;
    if (!aIsHokm && bIsHokm) return 1;
    
    // 2. Same color suit comes second
    if (aIsSameColor && !bIsSameColor && !bIsHokm) return -1;
    if (!aIsSameColor && bIsSameColor && !aIsHokm) return 1;
    
    // 3. Within hokm suit: sort by rank descending
    if (aIsHokm && bIsHokm) return b.r - a.r;
    
    // 4. Within same color suit: sort by rank descending
    if (aIsSameColor && bIsSameColor) {
      if (a.s === b.s) return b.r - a.r;
      // Different same-color suits: maintain order
      return ['S','H','D','C'].indexOf(a.s) - ['S','H','D','C'].indexOf(b.s);
    }
    
    // 5. Within other (non-hokm, non-same-color) suits: sort by count descending, then rank
    if (!aIsHokm && !bIsHokm && !aIsSameColor && !bIsSameColor) {
      const cA = countMap.get(a.s)||0;
      const cB = countMap.get(b.s)||0;
      if (cA !== cB) return cB - cA; // Higher count first
      if (a.s === b.s) return b.r - a.r; // Same suit: rank descending
      return ['S','H','D','C'].indexOf(a.s) - ['S','H','D','C'].indexOf(b.s);
    }
    
    // Fallback: maintain original order
    return 0;
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
      row.addComponents(new ButtonBuilder().setCustomId(`hokm-play-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-${c.s}-${c.r}`).setLabel(cardStr(c)).setStyle(ButtonStyle.Secondary));
    }
    rows.push(row);
  }
  // filter row
  const rowFilter = new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-ALL`).setLabel('همه').setStyle(filter==='ALL'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-S`).setLabel('♠️').setStyle(filter==='S'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-H`).setLabel('♥️').setStyle(filter==='H'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-D`).setLabel('♦️').setStyle(filter==='D'?ButtonStyle.Primary:ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`hokm-hand-filter-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-C`).setLabel('♣️').setStyle(filter==='C'?ButtonStyle.Primary:ButtonStyle.Secondary),
  );
  rows.push(rowFilter);
  // pagination row (if needed)
  if (totalPages > 1) {
    const rowPage = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId(`hokm-hand-page-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-${Math.max(0, page-1)}`).setLabel('قبلی').setStyle(ButtonStyle.Secondary).setDisabled(page<=0),
      new ButtonBuilder().setCustomId(`hokm-hand-page-${s.guildId}-${s.channelId}-${s.sessionId}-${userId}-${Math.min(totalPages-1, page+1)}`).setLabel('بعدی').setStyle(ButtonStyle.Secondary).setDisabled(page>=totalPages-1),
    );
    rows.push(rowPage);
  }
  return { rows, meta: { filter, page, totalPages } };
}

async function refreshPlayerDM(ctx: { client: Client }, s: HokmSession, userId: string) {
  // تابع خالی - دیگر هیچ پیام خصوصی ارسال نمی‌شود
  return;
}

async function refreshAllDMs(ctx: { client: Client }, s: HokmSession) {
  // تابع غیرفعال شده - هیچ پیام خصوصی ارسال نمی‌شود
  // ما تصمیم گرفتیم پیام‌های خصوصی را برای بهبود تجربه کاربری حذف کنیم
  return;
}

function clearHandOrderCache(s: HokmSession) {
  // Clear cached card order for all players when starting a new set/game
  for (const uid of s.order) {
    const orderKey = `__hokm_card_order_${s.guildId}:${s.channelId}:${s.sessionId}:${uid}`;
    delete (global as any)[orderKey];
  }
}

async function scheduleAnnounceMessageDeletion(channel: any, s: HokmSession) {
  // Schedule automatic deletion of announce message after 2.5 seconds
  if (s.newSetAnnounceMsgId) {
    const msgId = s.newSetAnnounceMsgId;
    setTimeout(async () => {
      try {
        if (channel?.messages?.fetch) {
          const am = await channel.messages.fetch(msgId).catch(()=>null);
          if (am) await am.delete().catch(()=>{});
        }
      } catch {}
    }, 2500);
  }
}

function buildHandRowsSimple(hand: Card[], userId: string, gId: string, cId: string, sessionId: string, hokm?: Suit): ActionRowBuilder<ButtonBuilder>[] {
  const rows: ActionRowBuilder<ButtonBuilder>[] = [];
  
  // Cache key for storing initial card order per player
  const orderKey = `__hokm_card_order_${gId}:${cId}:${sessionId}:${userId}`;
  
  let sortedHand: Card[];
  const cached = (global as any)[orderKey] as Card[] | undefined;
  
  if (cached && cached.length > 0) {
    // Use cached order: filter cards that still exist in hand and preserve order
    sortedHand = cached.filter(cachedCard => 
      hand.some(c => c.s === cachedCard.s && c.r === cachedCard.r)
    );
    // Add any new cards that weren't in cache (shouldn't happen normally)
    const cachedSet = new Set(sortedHand.map(c => `${c.s}${c.r}`));
    const newCards = hand.filter(c => !cachedSet.has(`${c.s}${c.r}`));
    if (newCards.length > 0) {
      sortedHand = [...sortedHand, ...sortHand(newCards, hokm)];
    }
  } else {
    // First time: sort and cache the order
    sortedHand = sortHand(hand, hokm);
    (global as any)[orderKey] = [...sortedHand]; // Deep copy for cache
  }
  
  // Build rows of 5 cards each
  for (let r = 0; r < 5; r++) {
    const slice = sortedHand.slice(r * 5, r * 5 + 5);
    if (!slice.length) break;
    const row = new ActionRowBuilder<ButtonBuilder>();
    for (const c of slice) {
      row.addComponents(new ButtonBuilder().setCustomId(`hokm-play-${gId}-${cId}-${sessionId}-${userId}-${c.s}-${c.r}`).setLabel(cardStr(c)).setStyle(ButtonStyle.Secondary));
    }
    rows.push(row);
  }
  return rows;
}

async function refreshPlayerChannelHand(ctx: { channel: any }, s: HokmSession, userId: string) {
  if (isVirtualBot(userId)) return; // bots don't need channel hand controls
  const hand = s.hands.get(userId) || [];
  const rows = buildHandRowsSimple(hand, userId, s.guildId, s.channelId, s.sessionId, s.hokm);
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
  if (!canvasAvailable || !loadImage) {
    return { tag: userId, img: null };
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
  // Validate session before rendering
  if (!s || !s.order || s.order.length !== 4) {
    console.error('[RENDER ERROR] Invalid session state - order:', s?.order?.length);
    throw new Error('Invalid session state for rendering');
  }
  if (!canvasAvailable || !createCanvas) {
    throw new Error('Canvas not available in this environment');
  }
  
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
  function drawSeatLabel(i: number, uid?: string, name?: string, avatar?: any, isTurn?: boolean, playerTricks?: number, isHakim?: boolean) {
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
    // Crown for hakim (on top edge of avatar)
    if (isHakim) {
      ctx.save();
      // Crown dimensions and position
      const crownW = avR * 0.8; // width of crown base
      const crownH = avR * 0.5; // height of crown
      const crownX = avX; // center x
      const crownY = avY - avR - crownH * 0.3; // position on top edge (half visible)
      
      // Draw crown with gradient
      const gradient = ctx.createLinearGradient(crownX - crownW/2, crownY - crownH/2, crownX + crownW/2, crownY + crownH/2);
      gradient.addColorStop(0, '#FFD700'); // gold top
      gradient.addColorStop(0.5, '#FFA500'); // orange middle
      gradient.addColorStop(1, '#FF8C00'); // dark orange bottom
      
      ctx.fillStyle = gradient;
      ctx.strokeStyle = '#B8860B'; // dark gold outline
      ctx.lineWidth = 2;
      
      // Draw crown shape
      ctx.beginPath();
      // Base of crown (bottom rectangle)
      const baseY = crownY + crownH/4;
      const baseH = crownH/3;
      // Five points of crown (zigzag top)
      const points = 5;
      const pointW = crownW / (points - 1);
      
      // Start from bottom left
      ctx.moveTo(crownX - crownW/2, baseY + baseH);
      ctx.lineTo(crownX - crownW/2, baseY);
      
      // Draw zigzag peaks
      for (let i = 0; i < points; i++) {
        const px = crownX - crownW/2 + i * pointW;
        const py = (i % 2 === 0) ? crownY - crownH/2 : baseY;
        ctx.lineTo(px, py);
      }
      
      // Complete the base
      ctx.lineTo(crownX + crownW/2, baseY);
      ctx.lineTo(crownX + crownW/2, baseY + baseH);
      ctx.closePath();
      
      // Fill and stroke
      ctx.fill();
      ctx.stroke();
      
      // Add highlights (small circles on peaks)
      ctx.fillStyle = '#FFFF00'; // bright yellow
      for (let i = 0; i < points; i += 2) {
        const px = crownX - crownW/2 + i * pointW;
        const py = crownY - crownH/2;
        ctx.beginPath();
        ctx.arc(px, py, 3, 0, Math.PI * 2);
        ctx.fill();
      }
      
      ctx.restore();
    }
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
    const isHakim = uid === s.hakim;
    drawSeatLabel(i, uid, mv.tag, mv.img, isTurn, pTricks, isHakim);
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

// Helper function to retry Discord API calls with exponential backoff
async function retryDiscordCall<T>(
  fn: () => Promise<T>,
  maxRetries: number = 2,
  baseDelay: number = 500
): Promise<T | null> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err: any) {
      const isNetworkError = err?.code === 'UND_ERR_SOCKET' || 
                             err?.code === 'ECONNRESET' || 
                             err?.code === 'ETIMEDOUT' ||
                             err?.message?.includes('network') ||
                             err?.message?.includes('socket');
      
      // اگر خطای شبکه است و تلاش‌های بیشتری مانده، صبر کن و دوباره امتحان کن
      if (isNetworkError && attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt);
        console.log(`[RETRY] Network error (${err?.code}), retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries})`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      
      // در غیر این صورت خطا را رها کن
      throw err;
    }
  }
  return null;
}

// محاسبه hash از state بازی برای تشخیص تغییرات
function calculateGameStateHash(s: HokmSession): string {
  const tableCards = s.table?.map(t => `${t.userId}:${t.card.s}${t.card.r}`).join('|') || '';
  const lastTrickCards = s.lastTrick?.map(t => `${t.userId}:${t.card.s}${t.card.r}`).join('|') || '';
  
  return [
    s.state,
    s.hokm || '',
    s.turnIndex ?? '',
    tableCards,
    lastTrickCards,
    s.tricksTeam1 ?? 0,
    s.tricksTeam2 ?? 0,
    s.setsTeam1 ?? 0,
    s.setsTeam2 ?? 0,
    s.leaderIndex ?? ''
  ].join(':');
}

async function refreshTableEmbed(ctx: { channel: any }, s: HokmSession, forceRender: boolean = false) {
  try {
    // Validate session state before rendering
    if (!s || !s.guildId || !s.channelId) {
      console.error('[TABLE ERROR] Invalid session object');
      return;
    }
    
    // Validate order array
    if (!s.order || s.order.length === 0) {
      console.error('[TABLE ERROR] No players in order array');
      return;
    }
    
    // Smart rendering: فقط زمانی render کن که state واقعاً تغییر کرده
    if (!forceRender) {
      const currentHash = calculateGameStateHash(s);
      if (s.lastRenderedHash === currentHash) {
        // هیچ تغییری نیست، render نکن
        console.log('[RENDER SKIP] No state changes detected, skipping render');
        return;
      }
      // به‌روزرسانی hash برای دفعه بعد
      s.lastRenderedHash = currentHash;
      console.log('[RENDER] State changed, rendering table');
    } else {
      // force render بود، hash را به‌روز کن
      s.lastRenderedHash = calculateGameStateHash(s);
      console.log('[RENDER FORCE] Force rendering table');
    }
    
    const img = await renderTableImage(s);
    const attachment = new AttachmentBuilder(img, { name: 'table.png' });
    
    // متن پیام برای انتخاب حکم (بدون embed)
    let messageContent = '';
    if (s.state === 'choosing_hokm' && s.hakim) {
      messageContent = `حاکم: <@${s.hakim}> — لطفاً حکم را انتخاب کن.`;
    }
    
    const openRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
      new ButtonBuilder().setCustomId(`hokm-open-hand-${s.guildId}-${s.channelId}-${s.sessionId}`).setLabel('دست من').setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId(`hokm-surrender-${s.guildId}-${s.channelId}-${s.sessionId}`).setLabel('تسلیم').setStyle(ButtonStyle.Danger)
    );
    // add hokm choose buttons when waiting for hakim to pick
    const rows: any[] = [openRow];
    if (s.state === 'choosing_hokm' && s.hakim) {
      const chooseRow = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId(`hokm-choose-S-${s.sessionId}`).setLabel('♠️ پیک').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId(`hokm-choose-H-${s.sessionId}`).setLabel('♥️ دل').setStyle(ButtonStyle.Danger),
        new ButtonBuilder().setCustomId(`hokm-choose-D-${s.sessionId}`).setLabel('♦️ خشت').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId(`hokm-choose-C-${s.sessionId}`).setLabel('♣️ گیشنیز').setStyle(ButtonStyle.Success),
      );
      rows.push(chooseRow);
    }
    
    // اگر پیام قبلی وجود دارد، سعی کنید آن را به‌روزرسانی کنید
    if (s.tableMsgId) {
      try {
        const m = await retryDiscordCall(() => ctx.channel.messages.fetch(s.tableMsgId)) as any;
        if (m) { 
          const editPayload: any = { components: rows, files: [attachment] };
          if (messageContent) editPayload.content = messageContent;
          
          const editResult = await retryDiscordCall(() => m.edit(editPayload)) as any;
          
          if (editResult) {
            // ویرایش موفق بود
            return;
          } else {
            // تمام تلاش‌ها ناموفق بودند - فقط لاگ کن و ادامه نده
            console.warn(`[TABLE WARNING] Failed to edit message after retries, skipping update`);
            return; // خروج بدون ایجاد پیام جدید
          }
        } else {
          // پیام یافت نشد، پاکسازی شناسه قبلی
          console.log(`[TABLE INFO] Message with ID ${s.tableMsgId} not found, creating new one`);
          s.tableMsgId = undefined;
        }
      } catch (editErr: unknown) {
        const errMsg = editErr instanceof Error ? editErr.message : 'Unknown error';
        const errCode = (editErr as any)?.code;
        
        // فقط برای خطاهای غیر شبکه‌ای پیام جدید بساز
        const isNetworkError = errCode === 'UND_ERR_SOCKET' || 
                               errCode === 'ECONNRESET' || 
                               errCode === 'ETIMEDOUT';
        
        if (isNetworkError) {
          console.warn(`[TABLE WARNING] Network error editing message: ${errCode}, skipping update`);
          return; // خروج بدون ایجاد پیام جدید
        }
        
        // برای خطاهای دیگر (مثل پیام حذف شده)، پیام جدید بساز
        console.warn(`[TABLE WARNING] Error editing message: ${errCode || errMsg}, creating new message`);
        s.tableMsgId = undefined;
      }
    }
    
    // ایجاد پیام جدید (فقط اگر tableMsgId تنظیم نشده باشد)
    if (!s.tableMsgId) {
      try {
        const sendPayload: any = { components: rows, files: [attachment] };
        if (messageContent) sendPayload.content = messageContent;
        
        const sent = await retryDiscordCall(() => ctx.channel.send(sendPayload)) as any;
        if (sent) {
          s.tableMsgId = sent.id;
        } else {
          console.error(`[TABLE ERROR] Failed to send new table message after retries`);
        }
      } catch (sendErr: unknown) {
        const errMsg = sendErr instanceof Error ? sendErr.message : 'Unknown error';
        const errCode = (sendErr as any)?.code;
        console.error(`[TABLE ERROR] Failed to send new table message: ${errCode || errMsg}`);
      }
    }
  } catch (err: unknown) {
    // در صورت خطای کلی، شناسه پیام را پاک می‌کنیم تا دفعه بعد دوباره تلاش شود
    console.error('[TABLE ERROR] Failed to refresh table embed:', err);
    const errCode = (err as any)?.code;
    if (errCode === 10008) { // Unknown Message
      s.tableMsgId = undefined;
      console.log('[TABLE RECOVERY] Cleared tableMsgId due to Unknown Message error');
    }
    // Don't throw - just log and continue
  }
}

async function resolveTrickAndContinue(interaction: Interaction, s: HokmSession) {
  try {
    // Validate state
    if (!s.table || s.table.length !== 4) {
      console.error(`[TRICK ERROR] Invalid table state: ${s.table?.length} cards`);
      return;
    }

    // Attempt to recover missing leadSuit from the first card on table when possible
    if ((!s.leadSuit || !s.hokm) && s.table && s.table.length === 4 && (s.table[0] as any)?.card) {
      if (!s.leadSuit) {
        s.leadSuit = s.table[0].card.s;
        console.warn('[TRICK RECOVERY] Missing leadSuit, inferred from first card on table');
      }
    }

    if (!s.leadSuit || !s.hokm) {
      console.error(`[TRICK ERROR] Missing lead suit or hokm`);
      return;
    }
    if (!s.order || s.order.length !== 4) {
      console.error(`[TRICK ERROR] Invalid order: ${s.order?.length} players`);
      return;
    }
  
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
  let trickStartIndex = s.leaderIndex ?? -1;
  if (trickStartIndex < 0 || trickStartIndex >= s.order.length) {
    const firstUserId = s.table[0]?.userId;
    const inferredIndex = firstUserId ? s.order.indexOf(firstUserId) : -1;
    if (inferredIndex >= 0) {
      trickStartIndex = inferredIndex;
      s.leaderIndex = inferredIndex;
      console.warn(`[TRICK RECOVERY] Fixed invalid leaderIndex using table: ${inferredIndex}`);
    } else {
      trickStartIndex = 0;
      s.leaderIndex = 0;
      console.warn('[TRICK RECOVERY] leaderIndex invalid and could not be inferred; defaulting to 0');
    }
  }
  const winnerTurnIndex = (trickStartIndex + winnerIdxInTrick) % 4;
  const winnerUserId = s.order[winnerTurnIndex];
  const team = s.team1.includes(winnerUserId) ? 't1' : 't2';
  if (team==='t1') s.tricksTeam1 = (s.tricksTeam1||0)+1; else s.tricksTeam2 = (s.tricksTeam2||0)+1;
  // per-player trick counter for current set
  if (!s.tricksByPlayer) s.tricksByPlayer = new Map();
  s.tricksByPlayer.set(winnerUserId, (s.tricksByPlayer.get(winnerUserId) || 0) + 1);
  
  // per-player trick counter for all sets
  if (!s.allTricksByPlayer) s.allTricksByPlayer = new Map();
  s.allTricksByPlayer.set(winnerUserId, (s.allTricksByPlayer.get(winnerUserId) || 0) + 1);
  
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
    
    // Initialize Kot counters if they don't exist
    s.kotTeam1 = s.kotTeam1 ?? 0;
    s.kotTeam2 = s.kotTeam2 ?? 0;
    s.hakemKotTeam1 = s.hakemKotTeam1 ?? 0;
    s.hakemKotTeam2 = s.hakemKotTeam2 ?? 0;
    
    let add = 1;
    let isKot = false;
    let isHakemKot = false;
    
    if (winnerTr === 7 && loserTr === 0) {
      const hakimIsTeam1 = s.team1.includes(s.hakim!);
      const winnerIsHakimTeam = (winnerTeam==='t1' && hakimIsTeam1) || (winnerTeam==='t2' && !hakimIsTeam1);
      
      if (winnerIsHakimTeam) {
        add = 2; // Kot (hakim team wins 7-0) gets 2 sets
        isKot = true;
        // Increment Kot counter for winner team
        if (winnerTeam === 't1') s.kotTeam1 += 1;
        else s.kotTeam2 += 1;
      } else {
        add = 3; // Hakem Kot (non-hakim team wins 7-0) gets 3 sets
        isHakemKot = true;
        // Increment Hakem Kot counter for winner team
        if (winnerTeam === 't1') s.hakemKotTeam1 += 1;
        else s.hakemKotTeam2 += 1;
      }
    }
    if (winnerTeam==='t1') s.setsTeam1 += add; else s.setsTeam2 += add;
    const targetSets = s.targetSets ?? 1;
    if ((s.setsTeam1>=targetSets) || (s.setsTeam2>=targetSets)) {
      // Clear turn timeout since game is finished
      clearTurnTimeout(s);
      s.state = 'finished';
      // update stats
      try {
        const gId = s.guildId;
        const t1 = s.team1; const t2 = s.team2;
        const winners = (s.setsTeam1 ?? 0) >= targetSets ? t1 : t2;
        const losers = winners === t1 ? t2 : t1;
        
        // Update basic game stats
        for (const uid of [...t1, ...t2]) {
          const stat = ensureUserStat(gId, uid);
          stat.games = (stat.games || 0) + 1;
          
          // Add trick counts to player stats - use allTricksByPlayer which contains tricks across all sets
          const playerTricks = s.allTricksByPlayer?.get(uid) || 0;
          stat.tricks = (stat.tricks || 0) + playerTricks;
          
          // Add set count for winner teams
          if (winners.includes(uid)) {
            // Win count
            stat.wins = (stat.wins || 0) + 1;
            
            // Sets count (one player gets all the sets from their team)
            const setCount = winners === t1 ? (s.setsTeam1 || 0) : (s.setsTeam2 || 0);
            stat.sets = (stat.sets || 0) + setCount;
            
            // Kot and Hakem Kot stats for winners - only count ones your team made
            const userTeam = t1.includes(uid) ? 't1' : 't2';
            if (userTeam === 't1') {
              if (s.kotTeam1 && s.kotTeam1 > 0) stat.kot = (stat.kot || 0) + s.kotTeam1;
              if (s.hakemKotTeam1 && s.hakemKotTeam1 > 0) stat.hakemKot = (stat.hakemKot || 0) + s.hakemKotTeam1;
              // Team 1 member also tracks Team 2's kots as losses
              if (s.kotTeam2 && s.kotTeam2 > 0) stat.kotLose = (stat.kotLose || 0) + s.kotTeam2;
              if (s.hakemKotTeam2 && s.hakemKotTeam2 > 0) stat.hakemKotLose = (stat.hakemKotLose || 0) + s.hakemKotTeam2;
            } else {
              if (s.kotTeam2 && s.kotTeam2 > 0) stat.kot = (stat.kot || 0) + s.kotTeam2;
              if (s.hakemKotTeam2 && s.hakemKotTeam2 > 0) stat.hakemKot = (stat.hakemKot || 0) + s.hakemKotTeam2;
              // Team 2 member also tracks Team 1's kots as losses
              if (s.kotTeam1 && s.kotTeam1 > 0) stat.kotLose = (stat.kotLose || 0) + s.kotTeam1;
              if (s.hakemKotTeam1 && s.hakemKotTeam1 > 0) stat.hakemKotLose = (stat.hakemKotLose || 0) + s.hakemKotTeam1;
            }
          } else {
            // Kot and Hakem Kot stats for losers - only count ones your team made
            const userTeam = t1.includes(uid) ? 't1' : 't2';
            if (userTeam === 't1') {
              if (s.kotTeam1 && s.kotTeam1 > 0) stat.kot = (stat.kot || 0) + s.kotTeam1;
              if (s.hakemKotTeam1 && s.hakemKotTeam1 > 0) stat.hakemKot = (stat.hakemKot || 0) + s.hakemKotTeam1;
              // Team 1 member also tracks Team 2's kots as losses
              if (s.kotTeam2 && s.kotTeam2 > 0) stat.kotLose = (stat.kotLose || 0) + s.kotTeam2;
              if (s.hakemKotTeam2 && s.hakemKotTeam2 > 0) stat.hakemKotLose = (stat.hakemKotLose || 0) + s.hakemKotTeam2;
            } else {
              if (s.kotTeam2 && s.kotTeam2 > 0) stat.kot = (stat.kot || 0) + s.kotTeam2;
              if (s.hakemKotTeam2 && s.hakemKotTeam2 > 0) stat.hakemKot = (stat.hakemKot || 0) + s.hakemKotTeam2;
              // Team 2 member also tracks Team 1's kots as losses
              if (s.kotTeam1 && s.kotTeam1 > 0) stat.kotLose = (stat.kotLose || 0) + s.kotTeam1;
              if (s.hakemKotTeam1 && s.hakemKotTeam1 > 0) stat.hakemKotLose = (stat.hakemKotLose || 0) + s.hakemKotTeam1;
            }
          }
        }
        
        // teammate wins (per team, +1 for both teammates)
        if (winners.length === 2) {
          const [a,b] = winners;
          ensureUserStat(gId, a).teammateWins[b] = (ensureUserStat(gId, a).teammateWins[b] || 0) + 1;
          ensureUserStat(gId, b).teammateWins[a] = (ensureUserStat(gId, b).teammateWins[a] || 0) + 1;
        }
        saveHokmStats();
      } catch (error) {
        console.error('[STATS ERROR]', error);
      }
      // forceRender=true چون بازی تمام شده
      if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s, true);
      // result embed
      if (gameChannel) {
        const t1Set = s.setsTeam1 ?? 0; const t2Set = s.setsTeam2 ?? 0;
        const starter = s.ownerId ? `<@${s.ownerId}>` : '—';
        const lines: string[] = [];
        
        // Get trick counts for each player across all sets
        const playerTricks: Record<string, number> = {};
        for (const [uid, tricks] of s.allTricksByPlayer?.entries() ?? []) {
          playerTricks[uid] = tricks;
        }
        
        // Header info
        lines.push(`### 🚩 Starter: ${starter}`);
        lines.push(`### 🎯 Sets: ${s.targetSets ?? 1}`);
        lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬●');
        
        // Team 1 info
        lines.push(`### 👥 Team 1: ${s.team1.map(u=>`<@${u}>`).join(' , ')} ➤ ${t1Set}`);
        lines.push('### ◦⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯◦');
        
        // Individual player tricks for Team 1
        for (const uid of s.team1) {
          lines.push(`### 🎴 <@${uid}> Trick ➩ ${playerTricks[uid] || 0}`);
        }
        
        // Team 1 Kot stats
        lines.push(`### ⭐ Kot ➩ ${s.kotTeam1 || 0}`);
        lines.push(`### 💎 Hakem Kot ➩ ${s.hakemKotTeam1 || 0}`);
        lines.push('### ════════════════════');
        
        // Team 2 info
        lines.push(`### 👥 Team 2: ${s.team2.map(u=>`<@${u}>`).join(' , ')} ➤ ${t2Set}`);
        lines.push('### ◦⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯◦');
        
        // Individual player tricks for Team 2
        for (const uid of s.team2) {
          lines.push(`### 🎴 <@${uid}> Trick ➩ ${playerTricks[uid] || 0}`);
        }
        
        // Team 2 Kot stats
        lines.push(`### ⭐ Kot ➩ ${s.kotTeam2 || 0}`);
        lines.push(`### 💎 Hakem Kot ➩ ${s.hakemKotTeam2 || 0}`);
        lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬●');
        
        // Winner info
        lines.push(`### ✅ Winner: Team ${t1Set>t2Set?1:2} ✅`);
        
        const emb = new EmbedBuilder().setDescription(lines.join('\n')).setColor(t1Set>t2Set?0x3b82f6:0xef4444);
        await gameChannel.send({ embeds: [emb] });
      }
      return;
    }
    // prepare next hand in same match
    console.log(`[NEW SET] Starting new set. Current score: Team1=${s.setsTeam1}, Team2=${s.setsTeam2}`);
    
    // Clear turn timeout since we're starting a new set
    clearTurnTimeout(s);
    
    s.deck = shuffle(makeDeck());
    s.hands.clear(); 
    s.order.forEach(u=>s.hands.set(u, []));
    clearHandOrderCache(s); // Clear cached suit order for new set
    s.hokm = undefined; 
    s.table = []; 
    s.leadSuit = null; 
    s.tricksTeam1 = 0; 
    s.tricksTeam2 = 0; 
    s.lastTrick = undefined;
    
    // Reset tricks for current set, but keep cumulative tricks for all sets
    s.tricksByPlayer = new Map(); 
    s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
    
    // Make sure allTricksByPlayer is initialized (shouldn't be necessary but adding as a safety check)
    if (!s.allTricksByPlayer) {
      s.allTricksByPlayer = new Map();
      // Initialize with current tricksByPlayer values if needed
      s.order.forEach(u => s.allTricksByPlayer!.set(u, 0));
    }
    
    s.turnIndex = undefined;
    s.leaderIndex = undefined;
    
    // choose next hakim: if current hakim's team won, keep; else clockwise next player
    const hakimIdx = s.order.indexOf(s.hakim!);
    if (hakimIdx < 0) {
      console.error(`[NEW SET ERROR] Current hakim not in order!`);
      s.hakim = s.order[0]; // fallback
    } else {
      const hakimIsTeam1 = s.team1.includes(s.hakim!);
      const hakimTeamWon = (winnerTeam==='t1' && hakimIsTeam1) || (winnerTeam==='t2' && !hakimIsTeam1);
      s.hakim = hakimTeamWon ? s.hakim! : s.order[(hakimIdx+1) % s.order.length];
    }
    
    console.log(`[NEW SET] New hakim: ${s.hakim}`);
    const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
    give(s.hakim, 5);
    s.state = 'choosing_hokm';
    if (gameChannel) {
      const announceMsg = await gameChannel.send({ content: `ست جدید آغاز شد. حاکم: <@${s.hakim}> — لطفاً حکم را انتخاب کن.` });
      s.newSetAnnounceMsgId = announceMsg.id;
    }
    // forceRender=true چون ست جدید شروع شده
    if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s, true);
    await refreshAllDMs({ client: (interaction.client as Client) }, s);
    if (isVirtualBot(s.hakim)) { await botChooseHokmAndStart(interaction.client as Client, gameChannel, s); }
    return;
  }

    // Parallel render for better performance
    // forceRender=false چون فقط یک کارت جدید افزوده شده (hash تغییر کرده)
    const renderPromises = [];
    if (gameChannel) renderPromises.push(refreshTableEmbed({ channel: gameChannel }, s, false));
    renderPromises.push(refreshAllDMs({ client: (interaction.client as Client) }, s));
    await Promise.all(renderPromises);
    // Start turn timeout for the next player
    await startTurnTimeout(interaction.client as Client, s);
    // trigger bot auto-play if next turn is bot
    await maybeBotAutoPlay(interaction.client as Client, s);
  } catch (err) {
    console.error('[TRICK ERROR] Exception in resolveTrickAndContinue:', err);
    // Try to recover by refreshing table
    try {
      const gameChannel = await (interaction.client as Client).channels.fetch(s.channelId).catch(()=>null);
      // forceRender=true برای recovery
      if (gameChannel) await refreshTableEmbed({ channel: gameChannel }, s, true);
    } catch {}
  }
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
// Create a new game session
function createNewSession(gId: string, cId: string, ownerId?: string): HokmSession {
  const sessionId = generateTableNumber(gId, cId);
  const s: HokmSession = { 
    sessionId, 
    guildId: gId, 
    channelId: cId, 
    ownerId,
    team1: [], 
    team2: [], 
    order: [], 
    deck: [], 
    hands: new Map(), 
    state: 'waiting', 
    tricksByPlayer: new Map(),
    // Initialize Kot and Hakem Kot counters
    kotTeam1: 0,
    kotTeam2: 0,
    hakemKotTeam1: 0,
    hakemKotTeam2: 0,
    // Initialize total tricks counter
    allTricksByPlayer: new Map()
  };
  const k = keyGCS(gId, cId, sessionId);
  hokmSessions.set(k, s);
  return s;
}

// Get session by sessionId
function getSession(gId: string, cId: string, sessionId: string): HokmSession | null {
  const k = keyGCS(gId, cId, sessionId);
  return hokmSessions.get(k) || null;
}

// Get all active sessions in a channel
function getChannelSessions(gId: string, cId: string): HokmSession[] {
  const sessions: HokmSession[] = [];
  for (const [key, session] of hokmSessions.entries()) {
    if (session.guildId === gId && session.channelId === cId) {
      sessions.push(session);
    }
  }
  return sessions;
}

// Count active games (not finished) in a channel
function countActiveGames(gId: string, cId: string): number {
  return getChannelSessions(gId, cId).filter(s => s.state !== 'finished').length;
}

// Generate table number (1-4) for this channel, reusing finished tables
function generateTableNumber(gId: string, cId: string): string {
  const sessions = getChannelSessions(gId, cId);
  const activeNumbers = new Set<string>();
  
  // Collect active (not finished) table numbers
  for (const s of sessions) {
    if (s.state !== 'finished') {
      activeNumbers.add(s.sessionId);
    }
  }
  
  // Find first available number from 1-4
  for (let i = 1; i <= 4; i++) {
    const num = String(i);
    if (!activeNumbers.has(num)) {
      return num;
    }
  }
  
  // Should never reach here if countActiveGames check is working
  return '1';
}

// Backwards-compat shim: return the most recent (by sessionId) non-finished session in this channel,
// or create a new empty session if none exists. Used by legacy commands.
function ensureSession(gId: string, cId: string): HokmSession {
  const list = getChannelSessions(gId, cId);
  if (list.length > 0) {
    const active = list.filter(s => s.state !== 'finished');
    const pickFrom = active.length ? active : list;
    pickFrom.sort((a, b) => a.sessionId.localeCompare(b.sessionId));
    return pickFrom[pickFrom.length - 1];
  }
  // Create a placeholder session (no owner yet)
  const s = createNewSession(gId, cId);
  return s;
}

// Find user's most recent active session (for .miz command)
function findUserActiveSession(gId: string, userId: string): HokmSession | null {
  // Find all sessions in this guild where user is playing
  const userSessions: HokmSession[] = [];
  for (const [key, session] of hokmSessions.entries()) {
    if (session.guildId === gId && session.order.includes(userId)) {
      // Only consider active game states
      if (session.state === 'playing' || session.state === 'choosing_hokm') {
        userSessions.push(session);
      }
    }
  }
  
  // Return the most recent one (last in the map)
  return userSessions.length > 0 ? userSessions[userSessions.length - 1] : null;
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
 const footballInFlight = new Set<string>();
 
 // ===== Voice co-presence tracking (for .friend) =====
 // channelMembers[guildId][channelId] -> Set<userId>
 const channelMembers: Map<string, Map<string, Set<string>>> = new Map();
// pairStarts[guildId][pairKey] -> startEpochMs (active session per channel)
const pairStarts: Map<string, Map<string, number>> = new Map();
// userVoiceStarts[guildId][userId] -> startEpochMs (active single-user session)
const userVoiceStarts: Map<string, Map<string, number>> = new Map();
// partnerTotals[guildId][userId][partnerId] -> totalMs
const partnerTotals: Map<string, Map<string, Map<string, number>>> = new Map();

// ===== Activity tracking for .idlist command =====
// voiceActivityLog[guildId][channelId] -> Array<{userId, timestamp}>
const voiceActivityLog: Map<string, Map<string, Array<{userId: string, timestamp: number}>>> = new Map();
// textActivityLog[guildId][channelId] -> Array<{userId, timestamp}>
const textActivityLog: Map<string, Map<string, Array<{userId: string, timestamp: number}>>> = new Map();

// ===== .dmall command state management =====
// dmallStates[userId] -> {step, message, timestamp}
type DmallState = {
  step: 'awaiting_message' | 'awaiting_userlist';
  message?: string;
  timestamp: number;
};
const dmallStates: Map<string, DmallState> = new Map();

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

// Timer prefix global for all servers (default is '!')
let globalTimerPrefix: string = '!';
const timerPrefixFile = path.join(process.cwd(), 'data', 'timer-prefix.json');
function loadTimerPrefix() {
  try {
    // Load stored prefix if exists
    const raw = fs.existsSync(timerPrefixFile) ? fs.readFileSync(timerPrefixFile, 'utf8') : '';
    if (raw) {
      const obj = JSON.parse(raw) as { prefix: string };
      if (obj.prefix) globalTimerPrefix = obj.prefix;
    } else {
      // If no file exists, save the default prefix '!' to persist it
      globalTimerPrefix = '!';
      saveTimerPrefix();
    }
  } catch {}
}
function saveTimerPrefix() {
  try {
    fs.mkdirSync(path.dirname(timerPrefixFile), { recursive: true });
    const obj = { prefix: globalTimerPrefix };
    fs.writeFileSync(timerPrefixFile, JSON.stringify(obj, null, 2), 'utf8');
  } catch {}
}
function getTimerPrefix(): string {
  return globalTimerPrefix;
}

// DM allowed users storage (users who can use .dm and .dmh commands)
const dmAllowedUsersSet = new Set<string>();
const dmAllowedUsersFile = path.join(process.cwd(), 'data', 'dm-allowed-users.json');
function loadDMAllowedUsers() {
  try {
    const raw = fs.existsSync(dmAllowedUsersFile) ? fs.readFileSync(dmAllowedUsersFile, 'utf8') : '';
    if (raw) {
      const obj = JSON.parse(raw) as { users: string[] };
      if (obj.users && Array.isArray(obj.users)) {
        dmAllowedUsersSet.clear();
        obj.users.forEach(id => dmAllowedUsersSet.add(id));
      }
    }
  } catch {}
}
function saveDMAllowedUsers() {
  try {
    fs.mkdirSync(path.dirname(dmAllowedUsersFile), { recursive: true });
    const obj = { users: Array.from(dmAllowedUsersSet) };
    fs.writeFileSync(dmAllowedUsersFile, JSON.stringify(obj, null, 2), 'utf8');
  } catch {}
}
function canUseDMCommands(userId: string): boolean {
  return userId === ownerId || dmAllowedUsersSet.has(userId);
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
  addVoiceDuration: (guildId: string, userId: string, deltaMs: number) => Promise<void> | void;
  getVoiceDuration: (guildId: string, userId: string) => Promise<number> | number;
  getTopVoice: (guildId: string, limit?: number) => Promise<{ user_id: string, total_ms: number }[]> | { user_id: string, total_ms: number }[];
};

let store: Store;
const pgUrl = process.env.DATABASE_URL;
if (pgUrl) {
  const pg = new PgFriendStore(pgUrl);
  store = {
    init: () => pg.init(),
    addDuration: (g, a, b, ms) => pg.addDuration(g, a, b, ms),
    loadGuild: (g) => pg.loadGuild(g),
    addVoiceDuration: (g, u, ms) => pg.addVoiceDuration(g, u, ms),
    getVoiceDuration: (g, u) => pg.getVoiceDuration(g, u),
    getTopVoice: (g, l) => pg.getTopVoice(g, l),
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
    addVoiceDuration: (g, u, ms) => sqlite.addVoiceDuration(g, u, ms),
    getVoiceDuration: (g, u) => sqlite.getVoiceDuration(g, u),
    getTopVoice: (g, l) => sqlite.getTopVoice(g, l),
  };
}

loadLoveOverrides();
loadLoveRandoms();
loadTimerPrefix();
loadDMAllowedUsers();

async function addDuration(guildId: string, a: string, b: string, deltaMs: number) {
  // افزودن بررسی مقدار درست زمان
  if (deltaMs <= 0) return;

  // گرد کردن به عدد صحیح برای جلوگیری از خطاهای احتمالی
  const roundedDelta = Math.floor(deltaMs);

  // دریافت نقشه‌های ذخیره‌سازی
  const gMap = getMap(partnerTotals, guildId, () => new Map());
  const aMap = getMap(gMap, a, () => new Map());
  const bMap = getMap(gMap, b, () => new Map());

  // اطمینان از مقداردهی متقارن برای هر دو کاربر
  const aCurrentTotal = aMap.get(b) || 0;
  const bCurrentTotal = bMap.get(a) || 0;

  // افزودن زمان جدید به مقادیر قبلی
  const aNewTotal = aCurrentTotal + roundedDelta;
  const bNewTotal = bCurrentTotal + roundedDelta;

  // مطمئن شوید که هر دو رکورد مقدار یکسانی دارند
  aMap.set(b, aNewTotal);
  bMap.set(a, bNewTotal);

  try {
    // ذخیره در پایگاه داده
    await store.addDuration(guildId, a, b, roundedDelta);
  } catch (err) {
    console.error(`[FRIEND DATA] Error saving duration for ${a}-${b}:`, err);
    // اگر ذخیره با مشکل مواجه شد، دوباره تلاش می‌کنیم
    try {
      await store.addDuration(guildId, a, b, roundedDelta);
    } catch {}
  }
}

// Helper to format duration in ms to readable string
function formatDuration(ms: number): string {
  const totalSeconds = Math.floor(ms / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

// Compute live totals for a user: persisted totals + ongoing sessions until now
function computeTotalsUpToNow(guildId: string, userId: string): Map<string, number> | null {
  // Создаем новую карту для выходных данных
  const out = new Map<string, number>();

  // 1. Сначала загружаем сохраненные итоги из partnerTotals
  const baseGuild = partnerTotals.get(guildId);
  const base = baseGuild?.get(userId);
  if (base) {
    // Копируем все сохраненные данные в выходную карту
    for (const [pid, ms] of base.entries()) {
      out.set(pid, ms);
    }
  }

  // 2. Теперь добавляем текущие активные сеансы из pairStarts
  const pMap = pairStarts.get(guildId);
  if (pMap && pMap.size > 0) {
    const now = Date.now();
    for (const [key, start] of pMap.entries()) {
      // key format: idA:idB:channelId
      const parts = key.split(':');
      if (parts.length < 3) continue;

      const idA = parts[0];
      const idB = parts[1];

      // Проверяем, является ли это релевантной парой для текущего пользователя
      const other = userId === idA ? idB : (userId === idB ? idA : null);
      if (!other) continue;

      // Вычисляем продолжительность текущей сессии
      const delta = now - start;
      if (delta > 0) {
        // Добавляем текущую продолжительность к имеющимся данным
        out.set(other, (out.get(other) || 0) + delta);
      }
    }
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

function isGifBuffer(buf: Buffer): boolean {
  return buf.length >= 4 && buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x38; // 'GIF8'
}

async function convertBufferToGif(buffer: Buffer): Promise<Buffer | null> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const sharpLib: any = require('sharp');
    const converted: Buffer = await sharpLib(buffer).gif().toBuffer();
    return converted;
  } catch (err) {
    console.error('[GIF CONVERT ERROR]', err);
    return null;
  }
}

async function convertVideoUrlToGif(url: string): Promise<Buffer | null> {
  try {
    const ffmpegPath = require('ffmpeg-static') as string | null;
    if (!ffmpegPath) return null;
    const { spawn } = require('child_process');

    return await new Promise<Buffer | null>((resolve) => {
      const args = [
        '-v', 'error',
        '-i', url,
        '-t', '7',
        '-vf', 'fps=15,scale=480:-1:flags=lanczos',
        '-loop', '0',
        '-f', 'gif',
        'pipe:1',
      ];

      const proc = spawn(ffmpegPath, args, { stdio: ['ignore', 'pipe', 'ignore'] });
      const chunks: Buffer[] = [];
      proc.stdout.on('data', (c: Buffer) => chunks.push(c));
      proc.on('error', () => resolve(null));
      proc.on('close', (code: number) => {
        if (code === 0 && chunks.length) {
          resolve(Buffer.concat(chunks));
        } else {
          resolve(null);
        }
      });
    });
  } catch (err) {
    console.error('[VIDEO GIF CONVERT ERROR]', err);
    return null;
  }
}

async function resolveImageForHosh(msg: Message): Promise<{ buffer: Buffer; mimeType: string } | null> {
  const pick = (m: Message) => {
    const att = m.attachments.find(a => {
      const ct = a.contentType || '';
      const name = (a.name || '').toLowerCase();
      if (ct.startsWith('image/')) return true;
      return name.endsWith('.png') || name.endsWith('.jpg') || name.endsWith('.jpeg') || name.endsWith('.webp');
    });
    return att || null;
  };

  let attachment = pick(msg);
  if (!attachment && msg.reference?.messageId) {
    try {
      const replied = await msg.channel.messages.fetch(msg.reference.messageId);
      attachment = pick(replied);
    } catch {
      // ignore
    }
  }
  if (!attachment) return null;

  let mimeType = attachment.contentType || '';
  if (!mimeType) {
    const name = (attachment.name || '').toLowerCase();
    if (name.endsWith('.png')) mimeType = 'image/png';
    else if (name.endsWith('.jpg') || name.endsWith('.jpeg')) mimeType = 'image/jpeg';
    else if (name.endsWith('.webp')) mimeType = 'image/webp';
  }
  if (!mimeType) mimeType = 'image/png';

  const buffer = await fetchBuffer(attachment.url);
  return { buffer, mimeType };
}

client.once('clientReady', async () => {
  console.log(`TimeSSD is online as ${client.user?.tag}`);
  botReady = true; // Mark bot as ready for health checks
  
  // Set bot status
  client.user?.setActivity('.komak | HOKM', { type: ActivityType.Playing });
  
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
  
  // ذخیره دوره‌ای جلسات صوتی جاری برای جلوگیری از از دست دادن داده‌ها هنگام راه‌اندازی مجدد
  setInterval(async () => {
    try {
      const now = Date.now();
      let saveCount = 0;
      let errorCount = 0;
      
      // ذخیره تمام جلسات صوتی جاری به پایگاه داده قبل از اینکه از دست بروند
      // بررسی هر سرور
      for (const [guildId, pMap] of pairStarts.entries()) {
        try {
          // برای هر جلسه فعال در هر سرور
          for (const [key, start] of pMap.entries()) {
            try {
              const parts = key.split(':');
              if (parts.length < 3) {
                // کلید نامعتبر - حذف آن
                pMap.delete(key);
                continue;
              }
              
              const a = parts[0];
              const b = parts[1];
              
              // بررسی برای اطمینان از اینکه شروع با یک زمان معتبر است
              if (typeof start !== 'number' || start <= 0 || start > now) {
                // زمان نامعتبر - تنظیم مجدد به زمان فعلی
                pMap.set(key, now);
                continue;
              }
              
              // محاسبه دقیق زمان سپری شده
              const deltaMs = now - start;
              if (deltaMs <= 0) continue; // اگر مشکلی در محاسبه زمان وجود دارد
              
              // اطمینان از اینکه مدت زمان منطقی است (کمتر از دو برابر فاصله ذخیره)
              if (deltaMs > 4 * 60 * 1000) {
                // زمان بیش از حد طولانی - احتمالاً غیرعادی - تنظیم مجدد به زمان فعلی
                console.warn(`[FRIEND DATA] Unusually long session (${Math.round(deltaMs/1000/60)}min) for ${a}-${b} - resetting timer`);
                pMap.set(key, now);
                continue;
              }
              
              // تلاش برای ذخیره زمان در دیتابیس
              await addDuration(guildId, a, b, deltaMs);
              saveCount++;
              
              // تنظیم زمان شروع به اکنون
              pMap.set(key, now);
            } catch (err) {
              errorCount++;
              console.error(`[FRIEND DATA] Error saving session for ${key}:`, err);
            }
          }
        } catch (err) {
          console.error(`[FRIEND DATA] Error processing guild ${guildId}:`, err);
        }
      }
      
      console.log(`[FRIEND DATA] Periodic save completed: ${saveCount} sessions saved, ${errorCount} errors`);
    } catch (err) {
      console.error('[FRIEND DATA] Periodic save error:', err);
    }
  }, 2 * 60 * 1000); // Every 2 minutes (reduced from 5min for more accuracy)
});

client.on('voiceStateUpdate', async (oldState: VoiceState, newState: VoiceState) => {
  try {
    const guildId = oldState.guild.id;
    const userId = oldState.id;
    const oldCid = oldState.channelId;
    const newCid = newState.channelId;
    const now = Date.now();
    
    // فقط تغییرات کانال را بررسی کنید
    if (oldCid === newCid) return; // نادیده گرفتن تغییرات mute/deaf
    
    // بررسی کاربران بات - اگر کاربر بات باشد، زمان را محاسبه نکنید
    if (newState.member?.user.bot) return;
    
    const chMap = getMap<string, Map<string, Set<string>>>(channelMembers, guildId, () => new Map<string, Set<string>>());
    const pMap = getMap<string, Map<string, number>>(pairStarts, guildId, () => new Map<string, number>());
    const uvMap = getMap<string, Map<string, number>>(userVoiceStarts, guildId, () => new Map<string, number>());
    
    // خروج از کانال قدیم: نهایی کردن جلسات با اعضای باقیمانده در آنجا
    if (oldCid) {
      // ثبت زمان کل ویس کاربر
      const startU = uvMap.get(userId);
      if (startU) {
        const duration = now - startU;
        if (duration > 0) {
          const res = store.addVoiceDuration(guildId, userId, duration);
          if (res instanceof Promise) res.catch((e: any) => console.error('[VOICE TOTAL ERR]', e));
        }
        uvMap.delete(userId);
      }

      const set = chMap.get(oldCid);
      if (set && set.has(userId)) {
        // حذف کاربر از کانال قدیم
        set.delete(userId);
        
        // برای هر کاربر دیگر در کانال، جلسه را ببندید و زمان را ذخیره کنید
        for (const otherId of set) {
          // اطمینان از اینکه بات نباشد
          try {
            const otherMember = await oldState.guild.members.fetch(otherId).catch(() => null);
            if (otherMember?.user.bot) continue;
            
            const key = pairKey(userId, otherId, oldCid);
            const start = pMap.get(key);
            if (start) {
              // محاسبه و ذخیره زمان با هم بودن
              const duration = now - start;
              if (duration > 0) {
                await addDuration(guildId, userId, otherId, duration);
              }
              pMap.delete(key);
            }
          } catch (err) {
            console.error(`[VOICE] Error processing voice partner ${otherId}:`, err);
          }
        }
        
        // اگر کانال خالی است، آن را حذف کنید
        if (set.size === 0) chMap.delete(oldCid);
      }
    }
    
    // پیوستن به کانال جدید: شروع جلسات با اعضای موجود در آنجا
    if (newCid) {
      const set = getMap<string, Set<string>>(chMap, newCid, () => new Set<string>());
      
      // برای هر کاربر موجود در کانال، یک جلسه جدید شروع کنید
      for (const otherId of set) {
        try {
          // اطمینان از اینکه بات نباشد
          const otherMember = await newState.guild.members.fetch(otherId).catch(() => null);
          if (otherMember?.user.bot) continue;
          
          const key = pairKey(userId, otherId, newCid);
          // اطمینان از اینکه جلسه فقط یک بار شروع شود
          if (!pMap.has(key)) {
            pMap.set(key, now);
          }
        } catch (err) {
          console.error(`[VOICE] Error starting session with ${otherId}:`, err);
        }
      }
      
      // افزودن کاربر به مجموعه کاربران کانال
      set.add(userId);

      // شروع جلسه ویس انفرادی
      if (!uvMap.has(userId)) {
        uvMap.set(userId, now);
      }
      
      // ثبت فعالیت صوتی برای دستور .idlist
      const voiceLog = getMap(voiceActivityLog, guildId, () => new Map());
      const channelLog = getMap(voiceLog, newCid, () => []);
      channelLog.push({ userId, timestamp: now });
    }
  } catch (err) {
    // مدیریت خطاهای ممکن برای جلوگیری از crash کردن بات
    console.error('[VOICE STATE ERROR]', err);
  }
});

// Helper function to safely reply to interactions (prevents double-reply and handles expired tokens)
async function safeInteractionReply(interaction: any, options: any): Promise<boolean> {
  try {
    if (interaction.replied || interaction.deferred) {
      // Already replied, try to edit or followUp
      if (options.ephemeral || options.fetchReply) {
        // Can't edit ephemeral messages easily, just skip
        return false;
      }
      try {
        await interaction.editReply(options);
        return true;
      } catch {
        return false;
      }
    } else {
      await interaction.reply(options);
      return true;
    }
  } catch (err: any) {
    // Silently ignore expired interaction errors (10062) and already acknowledged (40060)
    if (err?.code === 10062 || err?.code === 40060) {
      console.log(`[INTERACTION] Interaction expired or already handled (code ${err.code})`);
      return false;
    }
    console.error('[SAFE REPLY ERROR]:', err?.message || err);
    return false;
  }
}

// Global error handlers to prevent crashes
client.on('error', (error) => {
  console.error('[CLIENT ERROR]:', error);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[UNHANDLED REJECTION]:', reason);
  // Don't crash the bot
});

process.on('uncaughtException', (error) => {
  console.error('[UNCAUGHT EXCEPTION]:', error);
  console.error('[UNCAUGHT EXCEPTION] Stack:', error.stack);
  // Log but don't exit - keep bot alive
});

client.on('interactionCreate', async (interaction: Interaction) => {
  // Wrap everything in try-catch to prevent crashes
  try {
  // Hokm buttons
  if (interaction.isButton()) {
    const id = interaction.customId;
    // Join/Leave (session-aware)
    if (id.startsWith('hokm-join-t1-') || id.startsWith('hokm-join-t2-') || id.startsWith('hokm-leave-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', flags: [MessageFlags.Ephemeral] }); return; }
      const sessionId = id.split('-').pop() as string;
      const s = getSession(interaction.guild.id, interaction.channel.id, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
      const uid = interaction.user.id;
      if (id.startsWith('hokm-leave-')) {
        s.team1 = s.team1.filter(x=>x!==uid);
        s.team2 = s.team2.filter(x=>x!==uid);
        await interaction.reply({ content: 'از اتاق خارج شدی.', flags: [MessageFlags.Ephemeral] });
      } else {
        // First remove from both teams to prevent duplicates
        s.team1 = s.team1.filter(x=>x!==uid);
        s.team2 = s.team2.filter(x=>x!==uid);
        
        const target = id.startsWith('hokm-join-t1-') ? s.team1 : s.team2;
        if (target.length >= 2) { await interaction.reply({ content: 'این تیم پر است.', flags: [MessageFlags.Ephemeral] }); return; }
        target.push(uid);
        await interaction.reply({ content: `به تیم ${id.includes('t1')? '1':'2'} پیوستی.`, flags: [MessageFlags.Ephemeral] });
      }
      // Update control message as plain text (no embed)
      const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
      const rows = buildControlButtons(s.sessionId);
      try { if (s.controlMsgId) { const m = await (interaction.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      return;
    }

    // Bot management buttons
    if (id.startsWith('hokm-bot-add-t1-') || id.startsWith('hokm-bot-add-t2-') || id.startsWith('hokm-bot-remove-t1-') || id.startsWith('hokm-bot-remove-t2-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', flags: [MessageFlags.Ephemeral] }); return; }
      const sessionId = id.split('-').pop() as string;
      const s = getSession(interaction.guild.id, interaction.channel.id, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
      if (s.state !== 'waiting') { await interaction.reply({ content: 'فقط قبل از شروع بازی می‌توانید بات اضافه/حذف کنید.', flags: [MessageFlags.Ephemeral] }); return; }
      if (s.ownerId && interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'فقط سازنده اتاق می‌تواند بات اضافه/حذف کند.', flags: [MessageFlags.Ephemeral] }); return; }
      
      if (id.startsWith('hokm-bot-add-t1-')) {
        addBotToTeam(s, 1);
      } else if (id.startsWith('hokm-bot-add-t2-')) {
        addBotToTeam(s, 2);
      } else if (id.startsWith('hokm-bot-remove-t1-')) {
        const botInTeam = s.team1.find(u => isVirtualBot(u));
        if (botInTeam) {
          s.team1 = s.team1.filter(u => u !== botInTeam);
        }
      } else if (id.startsWith('hokm-bot-remove-t2-')) {
        const botInTeam = s.team2.find(u => isVirtualBot(u));
        if (botInTeam) {
          s.team2 = s.team2.filter(u => u !== botInTeam);
        }
      }
      
      // Acknowledge interaction without showing message
      await interaction.deferUpdate();
      
      // Update control message
      const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
      const rows = buildControlButtons(s.sessionId);
      try { if (s.controlMsgId) { const m = await (interaction.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      return;
    }

    // Start button → 4 teams ready → ask for number of sets
    if (id.startsWith('hokm-start-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', flags: [MessageFlags.Ephemeral] }); return; }
      const sessionId = id.split('-').pop() as string;
      const s = getSession(interaction.guild.id, interaction.channel.id, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
      if (!s.ownerId || interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'فقط سازنده اتاق می‌تواند شروع کند.', flags: [MessageFlags.Ephemeral] }); return; }
      if (s.state !== 'waiting') { await interaction.reply({ content: 'اتاق در وضعیت شروع نیست.', flags: [MessageFlags.Ephemeral] }); return; }
      if (s.team1.length !== 2 || s.team2.length !== 2) { await interaction.reply({ content: 'هر دو تیم باید ۲ نفر داشته باشند.', flags: [MessageFlags.Ephemeral] }); return; }
      
      // Show set selection buttons (7 options in 2 rows)
      const row1 = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId(`hokm-sets-1-${s.sessionId}`).setLabel('1 ست').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId(`hokm-sets-2-${s.sessionId}`).setLabel('2 ست').setStyle(ButtonStyle.Secondary),
        new ButtonBuilder().setCustomId(`hokm-sets-3-${s.sessionId}`).setLabel('3 ست').setStyle(ButtonStyle.Primary),
        new ButtonBuilder().setCustomId(`hokm-sets-4-${s.sessionId}`).setLabel('4 ست').setStyle(ButtonStyle.Primary),
      );
      const row2 = new ActionRowBuilder<ButtonBuilder>().addComponents(
        new ButtonBuilder().setCustomId(`hokm-sets-5-${s.sessionId}`).setLabel('5 ست').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId(`hokm-sets-6-${s.sessionId}`).setLabel('6 ست').setStyle(ButtonStyle.Success),
        new ButtonBuilder().setCustomId(`hokm-sets-7-${s.sessionId}`).setLabel('7 ست').setStyle(ButtonStyle.Danger),
      );
      await interaction.reply({ content: 'چند دست می‌خواهید بازی کنید؟', components: [row1, row2], flags: [MessageFlags.Ephemeral] });
      return;
    }
    
    // Set selection buttons
    if (id.startsWith('hokm-sets-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', flags: [MessageFlags.Ephemeral] }); return; }
      const partsSets = id.split('-');
      const sessionId = partsSets[3];
      const s = getSession(interaction.guild.id, interaction.channel.id, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
      if (!s.ownerId || interaction.user.id !== s.ownerId) { await interaction.reply({ content: 'فقط سازنده اتاق می‌تواند شروع کند.', flags: [MessageFlags.Ephemeral] }); return; }
      if (s.state !== 'waiting') { await interaction.reply({ content: 'اتاق در وضعیت شروع نیست.', flags: [MessageFlags.Ephemeral] }); return; }
      if (s.team1.length !== 2 || s.team2.length !== 2) { await interaction.reply({ content: 'هر دو تیم باید ۲ نفر داشته باشند.', flags: [MessageFlags.Ephemeral] }); return; }
      
      const numSets = parseInt(partsSets[2]);
      s.targetSets = numSets;
      s.targetTricks = s.targetTricks ?? 7;
      s.setsTeam1 = 0; s.setsTeam2 = 0;
      s.order = [s.team1[0], s.team2[0], s.team1[1], s.team2[1]];
      s.hakim = s.order[Math.floor(Math.random() * s.order.length)];
      s.deck = shuffle(makeDeck());
      s.hands.clear(); s.order.forEach(u=>s.hands.set(u, []));
      
      // Initialize allTricksByPlayer for the game
      s.allTricksByPlayer = new Map();
      s.order.forEach(u => s.allTricksByPlayer!.set(u, 0));
      
      clearHandOrderCache(s); // Clear cached suit order for new game
      const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
      give(s.hakim, 5);
      s.state = 'choosing_hokm';
      
      await interaction.update({ content: `بازی با ${numSets} ست شروع شد!`, components: [] });
      
      try {
        const chAny = interaction.channel as any;
        if (chAny && chAny.send) {
          const announceMsg = await chAny.send({ content: `ست جدید آغاز شد. حاکم: <@${s.hakim}> — لطفاً حکم را انتخاب کن.` });
          s.newSetAnnounceMsgId = announceMsg.id;
        }
      } catch {}
      // forceRender=true چون ست جدید بعد از surrender
      if (interaction.guild) await refreshTableEmbed({ channel: interaction.channel as any }, s, true);
      await refreshAllDMs({ client: interaction.client }, s);
      if (isVirtualBot(s.hakim)) { await botChooseHokmAndStart(interaction.client as Client, interaction.channel as any, s); }
      return;
    }

    // Suit choice buttons
    if (id.startsWith('hokm-choose-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', flags: [MessageFlags.Ephemeral] }); return; }
      const partsChoose = id.split('-'); // hokm-choose-S-sessionId
      const sessionId = partsChoose[3];
      const s = getSession(interaction.guild.id, interaction.channel.id, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
      if (s.state !== 'choosing_hokm' || !s.hakim) { await interaction.reply({ content: 'الان وقت انتخاب حکم نیست.', flags: [MessageFlags.Ephemeral] }); return; }
      if (interaction.user.id !== s.hakim) { await interaction.reply({ content: 'فقط حاکم می‌تواند حکم را انتخاب کند.', flags: [MessageFlags.Ephemeral] }); return; }
      const suitKey = partsChoose[2] as Suit;
      const suit: Suit | undefined = (['S','H','D','C'] as Suit[]).find(x=>x===suitKey);
      if (!suit) { await interaction.reply({ content: 'خال نامعتبر.', flags: [MessageFlags.Ephemeral] }); return; }
      s.hokm = suit;
      try { addHokmPick(s.guildId, s.hakim!, suit); saveHokmStats(); } catch {}
      // Clear card order cache so cards are re-sorted with hokm
      clearHandOrderCache(s);
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
      
      // Schedule announce message deletion after 2.5 seconds
      await scheduleAnnounceMessageDeletion(interaction.channel, s);
      // فقط refreshTableEmbed را صدا می‌زنیم که تصویر و دکمه‌ها را نمایش می‌دهد
      // forceRender=true چون حکم انتخاب شده و بازی شروع شده
      await refreshTableEmbed({ channel: interaction.channel }, s, true);
      // no per-player channel hand messages; users open hand ephemerally via table button
      await interaction.reply({ content: `حکم انتخاب شد: ${SUIT_EMOJI[s.hokm]}. بازی شروع شد. برای دیدن دست خود، روی دکمه "دست من" زیر میز بزن.`, flags: [MessageFlags.Ephemeral] });
      // Start turn timeout for the first player
      await startTurnTimeout(interaction.client as Client, s);
      // trigger bot auto-play if first turn is a bot
      await maybeBotAutoPlay(interaction.client as Client, s);
      return;
    }

    // Open Hand button (ephemeral per-user hand in channel)
    if (id.startsWith('hokm-open-hand-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', flags: [MessageFlags.Ephemeral] }); return; }
      const parts = id.split('-'); // hokm-open-hand-gId-cId-sessionId
      const gId = parts[3]; const cId = parts[4]; const sessionId = parts[5];
      
      // Verify this is the correct channel
      if (cId !== interaction.channelId) {
        await interaction.reply({ content: 'این دکمه برای کانال دیگری است.', flags: [MessageFlags.Ephemeral] });
        return;
      }
      const s = getSession(gId, cId, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
      const uid = interaction.user.id;
      
      // Check if user is in the game
      if (!s.order.includes(uid)) {
        await interaction.reply({ content: 'شما در این بازی نیستید.', flags: [MessageFlags.Ephemeral] });
        return;
      }
      
      let hand = s.hands.get(uid) || [];
      
      // Check if user has cards
      if (hand.length === 0) {
        // Recovery: if game is in playing state and table has 4 cards, try to auto-resolve stuck trick
        if (s.state === 'playing' && s.table && s.table.length === 4) {
          console.warn(`[HAND RECOVERY] Detected full table with 4 cards and empty hand for user ${uid}. Attempting auto-resolve.`);
          try {
            await resolveTrickAndContinue(interaction, s);
          } catch (err) {
            console.error('[HAND RECOVERY ERROR] Failed to auto-resolve stuck trick:', err);
          }
          hand = s.hands.get(uid) || [];
          if (hand.length > 0 && s.state === 'playing') {
            const rows = buildHandRowsSimple(hand, uid, s.guildId, s.channelId, s.sessionId, s.hokm);
            const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${uid===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}`;
            await interaction.reply({ content, components: rows, flags: [MessageFlags.Ephemeral] });
            return;
          }
        }

        let msg = 'شما کارتی ندارید.';
        if (s.state === 'choosing_hokm') {
          msg = 'شما هنوز کارتی ندارید. فقط حاکم در این مرحله کارت دارد.';
        } else if (s.state === 'waiting') {
          msg = 'بازی هنوز شروع نشده است.';
        } else if (s.state === 'finished') {
          msg = 'بازی پایان یافته است.';
        }
        if (s.state === 'playing') {
          console.warn(`[HAND WARNING] User ${uid} opened hand with no cards during playing state. Order: ${s.order.length}`);
        }
        await interaction.reply({ content: msg, flags: [MessageFlags.Ephemeral] });
        return;
      }
      
      const rows = buildHandRowsSimple(hand, uid, s.guildId, s.channelId, s.sessionId, s.hokm);
      const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${uid===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}`;
      await interaction.reply({ content, components: rows, flags: [MessageFlags.Ephemeral] });
      return;
    }

    // Surrender button
    if (id.startsWith('hokm-surrender-')) {
      if (!interaction.guild || !interaction.channel) { await interaction.reply({ content: 'خطای سرور.', flags: [MessageFlags.Ephemeral] }); return; }
      const parts = id.split('-'); // hokm-surrender-gId-cId-sessionId
      const gId = parts[2]; const cId = parts[3]; const sessionId = parts[4];
      
      // Verify this is the correct channel
      if (cId !== interaction.channelId) {
        await interaction.reply({ content: 'این دکمه برای کانال دیگری است.', flags: [MessageFlags.Ephemeral] });
        return;
      }
      const s = getSession(gId, cId, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
      const uid = interaction.user.id;
      
      // Check if user is in the game
      if (!s.order.includes(uid)) {
        await interaction.reply({ content: 'شما در این بازی نیستید.', flags: [MessageFlags.Ephemeral] });
        return;
      }
      
      // Can only surrender during active play
      if (s.state !== 'playing' && s.state !== 'choosing_hokm') {
        await interaction.reply({ content: 'فقط در حین بازی می‌توانید تسلیم کنید.', flags: [MessageFlags.Ephemeral] });
        return;
      }
      
      // Determine user's team
      const userTeam = s.team1.includes(uid) ? 1 : 2;
      
      // Check if team has won at least one trick
      const teamTricks = userTeam === 1 ? (s.tricksTeam1 || 0) : (s.tricksTeam2 || 0);
      if (teamTricks === 0) {
        await interaction.reply({ content: 'برای تسلیم شدن، تیم شما باید حداقل یک تریک برنده شده باشد.', flags: [MessageFlags.Ephemeral] });
        return;
      }
      
      s.surrenderVotesTeam1 = s.surrenderVotesTeam1 || new Set<string>();
      s.surrenderVotesTeam2 = s.surrenderVotesTeam2 || new Set<string>();
      const votes = userTeam === 1 ? s.surrenderVotesTeam1 : s.surrenderVotesTeam2;
      
      // Add vote
      votes.add(uid);
      
      // Check if both team members voted
      const teamMembers = userTeam === 1 ? s.team1 : s.team2;
      const allVoted = teamMembers.every(m => votes.has(m));
      
      if (allVoted) {
        // Award set to opponent team
        s.setsTeam1 = s.setsTeam1 || 0;
        s.setsTeam2 = s.setsTeam2 || 0;
        if (userTeam === 1) {
          s.setsTeam2 += 1;
        } else {
          s.setsTeam1 += 1;
        }
        
        // Clear surrender votes
        s.surrenderVotesTeam1.clear();
        s.surrenderVotesTeam2.clear();
        
        const targetSets = s.targetSets ?? 1;
        
        // Check if match is finished
        if ((s.setsTeam1 >= targetSets) || (s.setsTeam2 >= targetSets)) {
          // Clear turn timeout since game is finished
          clearTurnTimeout(s);
          s.state = 'finished';
          // Update stats
          try {
            const t1 = s.team1; const t2 = s.team2;
            const winners = (s.setsTeam1 ?? 0) >= targetSets ? t1 : t2;
            for (const u of [...t1, ...t2]) ensureUserStat(gId, u).games += 1;
            for (const u of winners) ensureUserStat(gId, u).wins += 1;
            if (winners.length === 2) {
              const [a,b] = winners;
              ensureUserStat(gId, a).teammateWins[b] = (ensureUserStat(gId, a).teammateWins[b] || 0) + 1;
              ensureUserStat(gId, b).teammateWins[a] = (ensureUserStat(gId, b).teammateWins[a] || 0) + 1;
            }
            saveHokmStats();
          } catch {}
          
          // forceRender=true چون بازی تمام شده (surrender تمام کننده)
          await refreshTableEmbed({ channel: interaction.channel }, s, true);
          
          // Result embed
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
          lines.push(`\n**تیم ${userTeam} تسلیم شد.**`);
          const emb = new EmbedBuilder().setDescription(lines.join('\n')).setColor(t1Set>t2Set?0x3b82f6:0xef4444);
          await (interaction.channel as any).send({ embeds: [emb] });
          await interaction.reply({ content: 'تیم شما تسلیم کرد. بازی به پایان رسید.', flags: [MessageFlags.Ephemeral] });
          return;
        }
        
        // Start new set
        console.log(`[SURRENDER] Team ${userTeam} surrendered. Starting new set. Score: Team1=${s.setsTeam1}, Team2=${s.setsTeam2}`);
        
        // Clear turn timeout since we're starting a new set
        clearTurnTimeout(s);
        
        s.deck = shuffle(makeDeck());
        s.hands.clear(); 
        s.order.forEach(u=>s.hands.set(u, []));
        clearHandOrderCache(s);
        s.hokm = undefined; 
        s.table = []; 
        s.leadSuit = null; 
        s.tricksTeam1 = 0; 
        s.tricksTeam2 = 0; 
        s.lastTrick = undefined;
        s.tricksByPlayer = new Map(); 
        s.order.forEach(u=>s.tricksByPlayer!.set(u,0));
        s.turnIndex = undefined;
        s.leaderIndex = undefined;
        
        // Choose next hakim: if surrendered team's hakim, give to opponent; else clockwise rotation
        const hakimIdx = s.hakim ? s.order.indexOf(s.hakim) : -1;
        if (hakimIdx >= 0) {
          const hakimIsTeam1 = s.team1.includes(s.hakim!);
          const hakimTeamSurrendered = (userTeam === 1 && hakimIsTeam1) || (userTeam === 2 && !hakimIsTeam1);
          if (hakimTeamSurrendered) {
            // Hakim's team surrendered, give hakim to opponent team (like losing a set)
            s.hakim = s.order[(hakimIdx + 1) % s.order.length];
          }
          // else keep current hakim (winning team keeps hakim)
        } else {
          s.hakim = s.order[0]; // fallback
        }
        
        const give = (u: string, n: number)=>{ const h = s.hands.get(u)!; for(let i=0;i<n;i++) h.push(s.deck.pop()!); };
        give(s.hakim!, 5);
        s.state = 'choosing_hokm';
        
        const announceMsg = await (interaction.channel as any).send({ content: `تیم ${userTeam} تسلیم کرد. ست جدید آغاز شد. حاکم: <@${s.hakim!}> — لطفاً حکم را انتخاب کن.` });
        s.newSetAnnounceMsgId = announceMsg.id;
        
        // forceRender=true چون ست جدید بعد از surrender
        await refreshTableEmbed({ channel: interaction.channel }, s, true);
        await refreshAllDMs({ client: (interaction.client as Client) }, s);
        
        if (isVirtualBot(s.hakim!)) { 
          await botChooseHokmAndStart(interaction.client as Client, interaction.channel as any, s); 
        }
        
        await interaction.reply({ content: 'تیم شما تسلیم کرد. یک ست به حریف داده شد.', flags: [MessageFlags.Ephemeral] });
        return;
      } else {
        // Not all team members voted yet
        const votedCount = Array.from(votes).length;
        await interaction.reply({ content: `رای شما ثبت شد. (${votedCount}/${teamMembers.length}) اعضای تیم رای دادند.`, flags: [MessageFlags.Ephemeral] });
        return;
      }
    }

    // DM hand filter buttons
    if (id.startsWith('hokm-hand-filter-')) {
      const parts = id.split('-'); // hokm-hand-filter-gId-cId-uid-FL
      const gId = parts[3]; const cId = parts[4]; const sessionId = parts[5]; const uid = parts[6]; const fl = parts[7] as any;
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'این دکمه برای دست شما نیست.', flags: [MessageFlags.Ephemeral] }); return; }
      const key = `__hokm_dm_state_${gId}:${cId}:${sessionId}:${uid}`;
      (global as any)[key] = { filter: fl, page: 0 };
      const s = getSession(gId, cId, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
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
      const gId = parts[3]; const cId = parts[4]; const sessionId = parts[5]; const uid = parts[6]; const page = parseInt(parts[7], 10) || 0;
      if (interaction.user.id !== uid) { await interaction.reply({ content: 'این دکمه برای دست شما نیست.', flags: [MessageFlags.Ephemeral] }); return; }
      const key = `__hokm_dm_state_${gId}:${cId}:${sessionId}:${uid}`;
      const prev = (global as any)[key] || { filter: 'ALL', page: 0 };
      (global as any)[key] = { filter: prev.filter || 'ALL', page };
      const s = getSession(gId, cId, sessionId);
      if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
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
      if (parts.length === 8) {
        // hokm-play-gId-cId-sessionId-uid-suit-rank
        gId = parts[2]; cId = parts[3]; const sessionId = parts[4]; uid = parts[5]; suit = parts[6] as Suit; rank = parseInt(parts[7], 10);
        const s = getSession(gId, cId, sessionId);
        if (!s) { await interaction.reply({ content: 'اتاق بازی معتبر نیست یا منقضی شده.', flags: [MessageFlags.Ephemeral] }); return; }
        // proceed with s below
        if (s.state !== 'playing' || s.turnIndex==null) { await interaction.reply({ content: 'بازی در جریان نیست.', flags: [MessageFlags.Ephemeral] }); return; }
        if (interaction.user.id !== uid) { await interaction.reply({ content: 'این دکمه برای دست شما نیست.', flags: [MessageFlags.Ephemeral] }); return; }
        if (s.order[s.turnIndex] !== uid) { await interaction.reply({ content: 'الان نوبت شما نیست.', flags: [MessageFlags.Ephemeral] }); return; }
        const hand = s.hands.get(uid) || [];
        const card: Card = { s: suit, r: rank };
        const idx = hand.findIndex(c=>sameCard(c, card));
        if (idx === -1) { await interaction.reply({ content: 'این کارت در دست شما نیست.', flags: [MessageFlags.Ephemeral] }); return; }
        // follow-suit
        if (!s.table || s.table.length === 0) {
          s.leadSuit = card.s;
        } else {
          const lead = s.leadSuit!;
          const hasLead = hand.some(c=>c.s===lead);
          if (hasLead && card.s !== lead) { await interaction.reply({ content: `باید خال شروع (${SUIT_EMOJI[lead]}) را دنبال کنید.`, flags: [MessageFlags.Ephemeral] }); return; }
        }
        // play
        hand.splice(idx,1); s.hands.set(uid, hand);
        s.table = s.table || []; s.table.push({ userId: uid, card });
        s.turnIndex = (s.turnIndex + 1) % s.order.length;
        
        // Clear the current turn timeout since player made their move
        clearTurnTimeout(s);
        // update the ephemeral hand panel dynamically
        {
          const rows = buildHandRowsSimple(hand, uid, s.guildId, s.channelId, s.sessionId, s.hokm);
          const content = `حکم: ${s.hokm?SUIT_EMOJI[s.hokm]:''} — ${uid===s.order[s.turnIndex??0]?'نوبت شماست.':'منتظر نوبت بمانید.'}`;
          try {
            if (!interaction.replied && !interaction.deferred) {
              await interaction.update({ content, components: rows });
            }
          } catch (err: any) {
            if (err?.code !== 40060 && err?.code !== 10062) {
              console.error('[HOKM INTERACTION ERROR]:', err?.message || err);
            } else if (err?.code === 10062) {
              console.log('[INTERACTION] Interaction token expired (15min timeout)');
            }
          }
        }
        // update table only
        try {
          const ch = await interaction.client.channels.fetch(cId).catch(()=>null) as any;
          // forceRender=false - render معمولی بعد از حرکت بات
          if (ch) await refreshTableEmbed({ channel: ch }, s, false);
        } catch (err) {
          console.error('[HOKM TABLE UPDATE ERROR]:', err);
        }
        // check trick resolve
        try {
          if (s.table.length === 4) {
            await resolveTrickAndContinue(interaction, s);
          } else {
            // Start timeout for next player if trick is not complete
            await startTurnTimeout(interaction.client as Client, s);
            await maybeBotAutoPlay(interaction.client as Client, s);
          }
        } catch (err) {
          console.error('[HOKM GAME FLOW ERROR]:', err);
        }
        return;
      } else {
        // hokm-play-uid-suit-rank (clicked in channel)
        uid = parts[2]; suit = parts[3] as Suit; rank = parseInt(parts[4], 10);
        const chAny = interaction.channel as any;
        if (chAny?.isThread && chAny.parentId) { cId = chAny.parentId; }
        if (!gId && chAny?.guildId) { gId = chAny.guildId; }
      }
      if (!gId || !cId) { await interaction.reply({ content: 'خطای کانال بازی.', flags: [MessageFlags.Ephemeral] }); return; }
      // Unknown session variant is no longer supported for channel buttons
      await interaction.reply({ content: 'این دکمه معتبر نیست. لطفاً پنل جدید دست را باز کنید.', flags: [MessageFlags.Ephemeral] });
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

  } catch (err: any) {
    // Don't log expired interaction errors (they're expected after 15 minutes)
    if (err?.code === 10062) {
      console.log('[INTERACTION] Interaction token expired (15min timeout) - ignoring');
      return;
    }
    
    console.error('[INTERACTION HANDLER ERROR]:', err);
    
    // Try to notify user if possible (but don't crash if this fails too)
    try {
      if (interaction.isButton() || interaction.isChatInputCommand()) {
        if (!interaction.replied && !interaction.deferred) {
          await interaction.reply({ content: '❌ خطایی رخ داد. لطفاً دوباره تلاش کنید.', flags: [MessageFlags.Ephemeral] }).catch(() => {});
        }
      }
    } catch {}
  }
});

// ===== Small Caps Conversion =====
// Map for converting English letters (both uppercase and lowercase) to Unicode small caps
const smallCapsMap: Record<string, string> = {
  'A': 'ᴀ', 'a': 'ᴀ',
  'B': 'ʙ', 'b': 'ʙ',
  'C': 'ᴄ', 'c': 'ᴄ',
  'D': 'ᴅ', 'd': 'ᴅ',
  'E': 'ᴇ', 'e': 'ᴇ',
  'F': 'ғ', 'f': 'ғ',
  'G': 'ɢ', 'g': 'ɢ',
  'H': 'ʜ', 'h': 'ʜ',
  'I': 'ɪ', 'i': 'ɪ',
  'J': 'ᴊ', 'j': 'ᴊ',
  'K': 'ᴋ', 'k': 'ᴋ',
  'L': 'ʟ', 'l': 'ʟ',
  'M': 'ᴍ', 'm': 'ᴍ',
  'N': 'ɴ', 'n': 'ɴ',
  'O': 'ᴏ', 'o': 'ᴏ',
  'P': 'ᴘ', 'p': 'ᴘ',
  'Q': 'Q', 'q': 'Q',
  'R': 'ʀ', 'r': 'ʀ',
  'S': 'ꜱ', 's': 'ꜱ',
  'T': 'ᴛ', 't': 'ᴛ',
  'U': 'ᴜ', 'u': 'ᴜ',
  'V': 'ᴠ', 'v': 'ᴠ',
  'W': 'ᴡ', 'w': 'ᴡ',
  'X': 'x', 'x': 'x',
  'Y': 'ʏ', 'y': 'ʏ',
  'Z': 'ᴢ', 'z': 'ᴢ',
};

function toSmallCaps(text: string): string {
  return text.split('').map(char => smallCapsMap[char] || char).join('');
}

// ===== Mathematical Sans-Serif Bold Conversion =====
// Map for converting English letters to Unicode Mathematical Sans-Serif Bold
const mathSansSerifBoldMap: Record<string, string> = {
  'A': '𝗔', 'a': '𝗮',
  'B': '𝗕', 'b': '𝗯',
  'C': '𝗖', 'c': '𝗰',
  'D': '𝗗', 'd': '𝗱',
  'E': '𝗘', 'e': '𝗲',
  'F': '𝗙', 'f': '𝗳',
  'G': '𝗚', 'g': '𝗴',
  'H': '𝗛', 'h': '𝗵',
  'I': '𝗜', 'i': '𝗶',
  'J': '𝗝', 'j': '𝗷',
  'K': '𝗞', 'k': '𝗸',
  'L': '𝗟', 'l': '𝗹',
  'M': '𝗠', 'm': '𝗺',
  'N': '𝗡', 'n': '𝗻',
  'O': '𝗢', 'o': '𝗼',
  'P': '𝗣', 'p': '𝗽',
  'Q': '𝗤', 'q': '𝗾',
  'R': '𝗥', 'r': '𝗿',
  'S': '𝗦', 's': '𝘀',
  'T': '𝗧', 't': '𝘁',
  'U': '𝗨', 'u': '𝘂',
  'V': '𝗩', 'v': '𝘃',
  'W': '𝗪', 'w': '𝘄',
  'X': '𝗫', 'x': '𝘅',
  'Y': '𝗬', 'y': '𝘆',
  'Z': '𝗭', 'z': '𝘇',
};

function toMathSansSerifBold(text: string): string {
  return text.split('').map(char => mathSansSerifBoldMap[char] || char).join('');
}

// Helper function to save current voice session times to the database
async function saveCurrent(guildId: string) {
  try {
    const pMap = pairStarts.get(guildId);
    if (!pMap || pMap.size === 0) return;
    
    const now = Date.now();
    let count = 0;
    
    // For each active session
    for (const [key, start] of pMap.entries()) {
      const parts = key.split(':');
      if (parts.length < 3) continue;
      
      const a = parts[0];
      const b = parts[1];
      const deltaMs = now - start;
      
      if (deltaMs > 0) {
        await addDuration(guildId, a, b, deltaMs);
        count++;
        // Update start time to now
        pMap.set(key, now);
      }
    }
    
    if (count > 0) {
      console.log(`[FRIEND DATA] Saved ${count} current sessions for guild ${guildId}`);
    }
  } catch (err) {
    console.error(`[FRIEND DATA] Error saving current sessions for guild ${guildId}:`, err);
  }
}

// Dot-prefix command: .t <duration> [reason]
client.on('messageCreate', async (msg: Message) => {
  if (!msg.inGuild()) return;
  if (msg.author.bot) return;
  if (processedMessages.has(msg.id)) return;
  processedMessages.add(msg.id);
  setTimeout(() => processedMessages.delete(msg.id), 60_000);
  const content = msg.content.trim();
  const isCmd = (name: string) => new RegExp(`^\\.${name}(?:\\s|$)`).test(content);
  const extractCustomEmojisFromText = (text?: string | null) => {
    const result: { id: string; animated: boolean; name: string }[] = [];
    if (!text) return result;
    const regex = /<(a?):(\w+):(\d+)>/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(text)) !== null) {
      result.push({ id: match[3], animated: match[1] === 'a', name: match[2] });
    }
    return result;
  };

  const extractEmojiNamesFromText = (text?: string | null) => {
    const result: string[] = [];
    if (!text) return result;
    const regex = /:([a-zA-Z0-9_]{2,}):/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(text)) !== null) {
      result.push(match[1]);
    }
    return result;
  };

  const collectEmojiLikeMedia = (source: Message) => {
    const customEmoji: { id: string; animated: boolean; name: string }[] = [];
    const stickers: any[] = [];
    const imageUrls: string[] = [];
    const imageSeen = new Set<string>();
    const videoUrls: string[] = [];
    const videoSeen = new Set<string>();

    const addFromText = (text?: string | null) => {
      const found = extractCustomEmojisFromText(text);
      for (const e of found) {
        customEmoji.push(e);
      }
    };

    addFromText(source.content);

    for (const embed of source.embeds) {
      addFromText(embed.title ?? undefined);
      addFromText(embed.description ?? undefined);
      if (embed.fields) {
        for (const field of embed.fields) {
          addFromText(field.name);
          addFromText(field.value);
        }
      }
      if (embed.footer?.text) addFromText(embed.footer.text);
      const anyEmbed = embed as any;
      if (anyEmbed.author?.name) addFromText(anyEmbed.author.name);
      const imgUrl = embed.image?.url;
      if (imgUrl && !imageSeen.has(imgUrl)) {
        imageSeen.add(imgUrl);
        imageUrls.push(imgUrl);
      }
      const thumbUrl = embed.thumbnail?.url;
      if (thumbUrl && !imageSeen.has(thumbUrl)) {
        imageSeen.add(thumbUrl);
        imageUrls.push(thumbUrl);
      }
      const videoUrl = (anyEmbed.video && (anyEmbed.video.url || anyEmbed.video.proxy_url)) as string | undefined;
      if (videoUrl && !videoSeen.has(videoUrl)) {
        videoSeen.add(videoUrl);
        videoUrls.push(videoUrl);
      }
    }

    if (source.stickers && source.stickers.size > 0) {
      for (const st of source.stickers.values()) {
        stickers.push(st);
      }
    }

    source.attachments.forEach(att => {
      const ct = att.contentType || '';
      const name = (att.name || '').toLowerCase();
      if (
        ct.startsWith('image/') ||
        name.endsWith('.png') ||
        name.endsWith('.jpg') ||
        name.endsWith('.jpeg') ||
        name.endsWith('.gif') ||
        name.endsWith('.webp')
      ) {
        if (!imageSeen.has(att.url)) {
          imageSeen.add(att.url);
          imageUrls.push(att.url);
        }
      } else if (
        ct.startsWith('video/') ||
        name.endsWith('.mp4') ||
        name.endsWith('.webm') ||
        name.endsWith('.mov') ||
        name.endsWith('.m4v')
      ) {
        if (!videoSeen.has(att.url)) {
          videoSeen.add(att.url);
          videoUrls.push(att.url);
        }
      }
    });

    return { customEmoji, stickers, imageUrls, videoUrls };
  };

  const collectEmojiNamesFromMessage = (source: Message) => {
    const names: string[] = [];
    const addNames = (text?: string | null) => {
      const found = extractEmojiNamesFromText(text);
      for (const n of found) names.push(n);
    };

    addNames(source.content);

    for (const embed of source.embeds) {
      addNames(embed.title ?? undefined);
      addNames(embed.description ?? undefined);
      if (embed.fields) {
        for (const field of embed.fields) {
          addNames(field.name);
          addNames(field.value);
        }
      }
      if (embed.footer?.text) addNames(embed.footer.text);
      const anyEmbed = embed as any;
      if (anyEmbed.author?.name) addNames(anyEmbed.author.name);
    }

    return names;
  };

  // تلاش برای پیدا کردن پیام اصلی وقتی پیام ریپلای‌شده فقط یک فوروارد/امبد/لینک است
  const resolveForwardedMessage = async (source: Message): Promise<Message | null> => {
    // 1) اگر خودِ این پیام ریپلایِ یک پیام دیگر است
    if (source.reference?.messageId) {
      try {
        const original = await source.fetchReference();
        if (original) return original as Message;
      } catch {}
    }

    // الگوی لینک پیام دیسکورد
    const linkRegex = /https?:\/\/(?:canary\.|ptb\.)?discord\.com\/channels\/(?:\d+|@me)\/(\d+)\/(\d+)/;

    const tryFetchByLink = async (text?: string | null): Promise<Message | null> => {
      if (!text) return null;
      const m = text.match(linkRegex);
      if (!m) return null;
      const [, channelId, messageId] = m;
      try {
        const channel: any = await source.client.channels.fetch(channelId).catch(() => null);
        if (!channel || !channel.isTextBased?.()) return null;
        const original: any = await channel.messages.fetch(messageId).catch(() => null);
        return original ?? null;
      } catch {
        return null;
      }
    };

    // 2) جستجو داخل امبدها (url / description / فیلدها)
    for (const embed of source.embeds) {
      if (embed.url) {
        const found = await tryFetchByLink(embed.url);
        if (found) return found;
      }
      if (embed.description) {
        const found = await tryFetchByLink(embed.description);
        if (found) return found;
      }
      if (embed.fields) {
        for (const field of embed.fields) {
          const found = await tryFetchByLink(field.value);
          if (found) return found;
        }
      }
    }

    // 3) جستجو داخل متن خود پیام
    const fromContent = await tryFetchByLink(source.content);
    if (fromContent) return fromContent;

    return null;
  };

  // نسخه‌ی «عمیق‌تر» که اگر چیزی در خود پیام نبود، سعی می‌کند پیام اصلی فوروارد/کوت‌شده را پیدا کند
  const collectEmojiLikeMediaDeep = async (source: Message) => {
    const first = collectEmojiLikeMedia(source);
    if (first.customEmoji.length || first.stickers.length || first.imageUrls.length || first.videoUrls.length) {
      return first;
    }

    const original = await resolveForwardedMessage(source);
    if (!original) return first;

    const second = collectEmojiLikeMedia(original as Message);
    return {
      customEmoji: [...first.customEmoji, ...second.customEmoji],
      stickers: [...first.stickers, ...second.stickers],
      imageUrls: [...first.imageUrls, ...second.imageUrls],
      videoUrls: [...first.videoUrls, ...second.videoUrls],
    };
  };

  const sendEmojiLikeMedia = async (format: 'gif' | 'png', rawArg?: string) => {
    const idArg = rawArg?.trim();
    const numericId = idArg && /^\d+$/.test(idArg) ? idArg : null;

    const sendGifFromUrls = async (urls: string[]) => {
      const attachments: { attachment: Buffer; name: string }[] = [];
      let index = 0;
      for (const url of urls) {
        try {
          const buf = await fetchBuffer(url);
          let gifBuf: Buffer | null;
          if (isGifBuffer(buf)) {
            // خود فایل از قبل GIF است
            gifBuf = buf;
          } else {
            // برای تصاویر (از جمله APNG/WEBP) ابتدا با ffmpeg تلاش می‌کنیم
            gifBuf = await convertImageUrlToGif(url);
            // اگر ffmpeg موفق نشد، در مرحله‌ی بعد با sharp تلاش می‌کنیم تا حداقل یک GIF بسازیم
            if (!gifBuf) {
              gifBuf = await convertBufferToGif(buf);
            }
          }
          if (!gifBuf) continue;
          attachments.push({ attachment: gifBuf, name: `emoji_${++index}.gif` });
        } catch {
          // اگر دانلود/تبدیل این URL شکست خورد، ادامه می‌دهیم سراغ بعدی
        }
      }

      if (!attachments.length) {
        // اگر هیچ GIF معتبری نتوانستیم بسازیم، توضیح می‌دهیم چرا (مثلاً استیکرهای Lottie یا مدیای پاک‌شده)
        await msg.reply({
          content:
            'نتوانستم این مدیا را به GIF متحرک تبدیل کنم. این معمولاً زمانی رخ می‌دهد که فایل اصلی دیگر در دسترس نیست، یا از نوع استیکرهای خاص (مثل Lottie) باشد که دیسکورد فقط سمت کلاینت رندرشان می‌کند.',
        });
        return;
      }

      const maxPerMessage = 10;
      let firstBatch = true;
      try {
        for (let i = 0; i < attachments.length; i += maxPerMessage) {
          const slice = attachments.slice(i, i + maxPerMessage);
          if (firstBatch) {
            await msg.reply({ files: slice });
            firstBatch = false;
          } else {
            await msg.channel.send({ files: slice });
          }
        }
      } catch {
        await msg.reply({ content: 'ارسال فایل به عنوان GIF با خطا مواجه شد.' }).catch(() => {});
      }
    };

    if (numericId) {
      const urlsToTry: string[] = [];
      if (format === 'gif') {
        urlsToTry.push(`https://cdn.discordapp.com/emojis/${numericId}.gif?v=1`);
        urlsToTry.push(`https://cdn.discordapp.com/stickers/${numericId}.gif`);
        urlsToTry.push(`https://cdn.discordapp.com/emojis/${numericId}.png?v=1`);
        urlsToTry.push(`https://cdn.discordapp.com/stickers/${numericId}.png`);
        await sendGifFromUrls(urlsToTry);
      } else {
        urlsToTry.push(`https://cdn.discordapp.com/emojis/${numericId}.png?v=1`);
        urlsToTry.push(`https://cdn.discordapp.com/stickers/${numericId}.png`);
        urlsToTry.push(`https://cdn.discordapp.com/emojis/${numericId}.gif?v=1`);
        urlsToTry.push(`https://cdn.discordapp.com/stickers/${numericId}.gif`);
        let sent = false;
        for (const url of urlsToTry) {
          try {
            await msg.reply({ files: [url] });
            sent = true;
            break;
          } catch {}
        }
        if (!sent) {
          await msg.reply({ content: 'هیچ فایل معتبری برای این آیدی پیدا نشد. مطمئن شوید آیدی درست است و استیکر/اموجی هنوز پاک نشده باشد.' });
        }
      }
      return;
    }

    if (!msg.reference?.messageId) {
      await msg.reply({ content: 'برای استفاده از این دستور باید روی یک پیام حاوی اموجی سرور یا استیکر ریپلای کنید.' });
      return;
    }

    const replied = await msg.fetchReference().catch(() => null as any);
    if (!replied) {
      await msg.reply({ content: 'پیام مورد نظر پیدا نشد.' });
      return;
    }

    const { customEmoji, stickers, imageUrls, videoUrls } = await collectEmojiLikeMediaDeep(replied as Message);

    const urls: string[] = [];
    const videoSources: string[] = [];

    for (const st of stickers) {
      if (st && typeof st.url === 'string') {
        urls.push(st.url);
      }
    }

    for (const e of customEmoji) {
      let url: string;
      if (format === 'gif' && (e as any).animated) {
        url = `https://cdn.discordapp.com/emojis/${e.id}.gif?v=1`;
      } else {
        url = `https://cdn.discordapp.com/emojis/${e.id}.png?v=1`;
      }
      urls.push(url);
    }

    for (const url of imageUrls) {
      urls.push(url);
    }

    for (const v of videoUrls) {
      videoSources.push(v);
    }

    if (!urls.length && !videoSources.length) {
      let nameCandidates = collectEmojiNamesFromMessage(replied as Message);
      try {
        const original = await resolveForwardedMessage(replied as Message);
        if (original) {
          nameCandidates = nameCandidates.concat(collectEmojiNamesFromMessage(original as Message));
        }
      } catch {}
      const fromEmojiGg = nameCandidates.length
        ? await findEmojiGgByNames(nameCandidates, format === 'gif')
        : [];
      for (const item of fromEmojiGg) {
        urls.push(item.url);
      }
    }

    if (!urls.length && !videoSources.length) {
      await msg.reply({ content: 'در پیامی که ریپلای کردید هیچ اموجی سرور یا استیکر معتبری پیدا نشد.\nاین دستور فقط روی اموجی‌های سرور، استیکرهای دیسکورد و تصاویر مرتبط کار می‌کند.' });
      return;
    }

    if (format === 'gif') {
      if (videoSources.length) {
        const videoAttachments: { attachment: Buffer; name: string }[] = [];
        let vIndex = 0;
        for (const v of videoSources) {
          try {
            const gifBuf = await convertVideoUrlToGif(v);
            if (!gifBuf) continue;
            videoAttachments.push({ attachment: gifBuf, name: `video_${++vIndex}.gif` });
          } catch {}
        }

        if (videoAttachments.length) {
          const maxPerMessage = 10;
          let firstBatch = true;
          try {
            for (let i = 0; i < videoAttachments.length; i += maxPerMessage) {
              const slice = videoAttachments.slice(i, i + maxPerMessage);
              if (firstBatch) {
                await msg.reply({ files: slice });
                firstBatch = false;
              } else {
                await msg.channel.send({ files: slice });
              }
            }
          } catch {
            await msg.reply({ content: 'ارسال فایل به عنوان GIF با خطا مواجه شد.' }).catch(() => {});
          }
          return;
        }

        await msg.reply({
          content:
            'نتوانستم این ویدیو را به GIF متحرک تبدیل کنم. معمولاً وقتی کُدک ویدیو یا دسترسی به فایل محدود باشد این اتفاق می‌افتد.',
        }).catch(() => {});
        return;
      }

      await sendGifFromUrls(urls);
      return;
    }

    // PNG حالت قبلی: فقط ارسال URL ها
    const maxPerMessage = 10;
    let firstBatch = true;
    try {
      for (let i = 0; i < urls.length; i += maxPerMessage) {
        const slice = urls.slice(i, i + maxPerMessage);
        if (firstBatch) {
          await msg.reply({ files: slice });
          firstBatch = false;
        } else {
          await msg.channel.send({ files: slice });
        }
      }
    } catch {
      await msg.reply({ content: 'ارسال فایل به عنوان PNG با خطا مواجه شد.' }).catch(() => {});
    }
  };

  const makeEmojiOrStickerName = (base: string, index: number) => {
    let name = (base || 'ts').toLowerCase().replace(/[^a-z0-9_]/g, '');
    if (!name) name = 'ts';
    if (name.length > 32) name = name.slice(0, 32);
    if (index > 0) {
      const suffix = `_${index + 1}`;
      if (name.length + suffix.length > 32) {
        name = name.slice(0, 32 - suffix.length);
      }
      name += suffix;
    }
    return name;
  };
  
  // Log text activity for .idlist command
  if (msg.guildId && msg.channelId) {
    const textLog = getMap(textActivityLog, msg.guildId, () => new Map());
    const channelLog = getMap(textLog, msg.channelId, () => []);
    channelLog.push({ userId: msg.author.id, timestamp: Date.now() });
  }

  // Handle .dmall workflow states
  const dmallState = dmallStates.get(msg.author.id);
  if (dmallState) {
    // Allow cancel at any step
    if (isCmd('cancel')) {
      dmallStates.delete(msg.author.id);
      await msg.reply({ content: '✅ عملیات .dmall لغو شد.' });
      return;
    }
    
    // Cleanup old states (> 10 minutes)
    if (Date.now() - dmallState.timestamp > 10 * 60 * 1000) {
      dmallStates.delete(msg.author.id);
    } else if (dmallState.step === 'awaiting_message') {
      // User is sending the message to broadcast
      const messageContent = content;
      if (!messageContent) {
        await msg.reply({ content: 'پیام نمی‌تواند خالی باشد. لطفاً پیام مورد نظر را ارسال کنید یا با `.cancel` لغو کنید.' });
        return;
      }
      dmallState.message = messageContent;
      dmallState.step = 'awaiting_userlist';
      dmallState.timestamp = Date.now();
      await msg.reply({ content: '✅ پیام دریافت شد.\n\nحالا لیست یوزر آیدی‌ها یا منشن کاربران را ارسال کنید (با فاصله از هم جدا شوند).\nیا فایل TXT حاوی یوزر آیدی‌ها را آپلود کنید.' });
      return;
    } else if (dmallState.step === 'awaiting_userlist') {
      // User is sending the user list
      let userIds: string[] = [];
      
      // Check if message has attachment (TXT file)
      if (msg.attachments.size > 0) {
        const attachment = msg.attachments.first();
        if (attachment && attachment.name?.endsWith('.txt')) {
          try {
            const response = await fetch(attachment.url);
            const text = await response.text();
            // Parse user IDs from file (space-separated)
            userIds = text.split(/\s+/).filter(id => /^\d+$/.test(id.trim())).map(id => id.trim());
          } catch (err) {
            await msg.reply({ content: '❌ خطا در خواندن فایل.' });
            dmallStates.delete(msg.author.id);
            return;
          }
        }
      } else {
        // Parse from message content (mentions or user IDs)
        const mentionRegex = /<@!?(\d+)>/g;
        let match;
        while ((match = mentionRegex.exec(content)) !== null) {
          userIds.push(match[1]);
        }
        
        // Also parse plain user IDs
        const tokens = content.split(/\s+/);
        for (const token of tokens) {
          if (/^\d{17,20}$/.test(token) && !userIds.includes(token)) {
            userIds.push(token);
          }
        }
      }
      
      if (userIds.length === 0) {
        await msg.reply({ content: '❌ هیچ یوزر آیدی معتبری یافت نشد. لطفاً یوزر آیدی‌ها یا منشن‌ها را ارسال کنید یا با `.cancel` لغو کنید.' });
        return;
      }
      
      // Start sending DMs
      const messageToSend = dmallState.message!;
      dmallStates.delete(msg.author.id);
      
      const progressMsg = await msg.reply({ content: `📤 در حال ارسال به ${userIds.length} کاربر...
⏳ پیشرفت: 0/${userIds.length}` });
      
      const failedUsers: string[] = [];
      const rateLimitedUsers: string[] = [];
      let successCount = 0;
      
      for (let i = 0; i < userIds.length; i++) {
        const userId = userIds[i];
        try {
          const user = await msg.client.users.fetch(userId);
          await user.send(messageToSend);
          successCount++;
          
          // Update progress every 10 messages or at the end
          if ((i + 1) % 10 === 0 || i === userIds.length - 1) {
            try {
              await progressMsg.edit({ content: `📤 در حال ارسال به ${userIds.length} کاربر...
⏳ پیشرفت: ${i + 1}/${userIds.length}
✅ موفق: ${successCount} | ❌ ناموفق: ${failedUsers.length + rateLimitedUsers.length}` });
            } catch {}
          }
          
          // Discord rate limit: ~1 DM per second is safe
          // Use 1500ms delay to be extra safe
          await new Promise(resolve => setTimeout(resolve, 1500));
        } catch (err: any) {
          // Check if it's a rate limit error
          if (err?.code === 429 || err?.status === 429) {
            rateLimitedUsers.push(userId);
            // Wait longer if rate limited (5 seconds)
            await new Promise(resolve => setTimeout(resolve, 5000));
          } else {
            failedUsers.push(userId);
          }
        }
      }
      
      // Send final report
      const totalFailed = failedUsers.length + rateLimitedUsers.length;
      let reportMsg = `✅ ارسال کامل شد!

📊 آمار:
• موفق: ${successCount}/${userIds.length}
• ناموفق: ${totalFailed}`;
      
      if (rateLimitedUsers.length > 0) {
        reportMsg += `
• ⚠️ Rate Limited: ${rateLimitedUsers.length}`;
      }
      
      // Calculate how many mentions we can fit in a 2000 char message
      // Each mention is ~22 chars (<@123456789012345678> + space)
      // Reserve 500 chars for the message structure
      const maxCharsForMentions = 1500;
      const avgMentionLength = 23;
      const maxMentionsPerMessage = Math.floor(maxCharsForMentions / avgMentionLength);
      
      const allFailedUsers = [...failedUsers, ...rateLimitedUsers];
      if (allFailedUsers.length > 0) {
        reportMsg += `

❌ کاربران با دایرکت بسته یا محدود شده:
`;
        const failedToMention = allFailedUsers.slice(0, maxMentionsPerMessage).map(id => `<@${id}>`).join(' ');
        reportMsg += failedToMention;
        if (allFailedUsers.length > maxMentionsPerMessage) {
          reportMsg += `

... و ${allFailedUsers.length - maxMentionsPerMessage} کاربر دیگر`;
          // Send remaining users in separate messages if needed
          for (let i = maxMentionsPerMessage; i < allFailedUsers.length; i += maxMentionsPerMessage) {
            const batch = allFailedUsers.slice(i, i + maxMentionsPerMessage);
            const batchMsg = batch.map(id => `<@${id}>`).join(' ');
            try {
              await msg.channel.send({ content: batchMsg, allowedMentions: { parse: [] } });
            } catch {}
          }
        }
      }
      
      try {
        await progressMsg.edit({ content: reportMsg, allowedMentions: { parse: [] } });
      } catch {
        await msg.reply({ content: reportMsg, allowedMentions: { parse: [] } });
      }
      return;
    }
  }
  
  if (isCmd('komak') || isCmd('komakfa')) {
    const lines: string[] = [
      '📚 راهنمای کامل دستورات 𝐋 𝐔 𝐍 𝐀',
      '',
      '🃏 بازی حکم',
      '.new ⟹ ساخت اتاق بازی جدید',
      '.a1 @user ⟹ افزودن به تیم ۱',
      '.a2 @user ⟹ افزودن به تیم ۲',
      '.rem @user ⟹ حذف از تیم‌ها',
      '.reset ⟹ ریست بازی با همان تیم‌ها',
      '.end ⟹ پایان و حذف اتاق',
      '.list ⟹ نمایش لیست/وضعیت',
      '.miz ⟹ نمایش مجدد میز',
      '.change @user bot1 ⟹ جایگزینی بازیکن با بات (bot1/bot2/bot3)',
      '.tablepng ⟹ دانلود تصویر PNG میز',
      '.tablesvg ⟹ دانلود فایل SVG میز',
      '.best ⟹ بازیکنان برتر',
      '.bazikon @user ⟹ آمار بازیکن',
      '',
      '✒️ زیبا سازی آیدی',
      '.esm @user TEST ⟹ ᴛᴇꜱᴛ تغییر نیک نیم کاربر',
      '.esm TEST ⟹ ᴛᴇꜱᴛ خروجی فونت',
      '.esm1 @user TEST ⟹ 𝗧𝗘𝗦𝗧 تغییر نیک نیم کاربر',
      '.esm1 TEST ⟹ 𝗧𝗘𝗦𝗧 خروجی فونت',
      '',
      '⏱️ دستورات تایمر',
      '!t 30',
      '.e 30 ⟹ افزودن 30 ثانیه به آخرین تایمر',
      '',
      '❤️ دستورات محاسبه عشق',
      '.ll ⟹ درصد رندوم',
      '.ll @user ⟹ درصد کاربر با شخص منشن شده',
      '.ll @user1 @user2 ⟹ درصد بین دو شخص منشن شده',
      '',
      '👥 دستورات هم‌حضوری ویس',
      '.friend ⟹ دوستان برتر کاربر',
      '.friend @user ⟹ دوستان برتر شخص منشن شده',
      '.topfriend / .topfriends / .top / .تاپ ⟹ زوج های برتر سرور',
      '',
      '🎙️ دستورات فعالیت صوتی (Voice)',
      '.voice / .ویس ⟹ مشاهده کل زمان حضور در ویس',
      '.voice @user ⟹ زمان حضور شخص منشن شده در ویس',
      '.topvoice / .تاپ‌ویس ⟹ لیست ۲۰ نفر برتر از نظر زمان ویس',
      '',
      '👤 پروفایل کاربر',
      '.av @user ⟹ نمایش آواتار با لینک',
      '.ba @user ⟹ نمایش بنر کاربر',
      '',
      '🎲 دستورات رندومایز',
      '.sort name1 name2 ... ⟹ لیست رندوم',
      '.sort group1...\ngroup2... ⟹ جفت کردن رندوم',
      '.sortpv ⟹ رندوم و ارسال به دایرکت',
      'استفاده از ! برای گروه‌بندی: !item1 item2! = یک آیتم',
      '',
      '🖼️ استیکر و اموجی',
      '.gif / .گیف ⟹ تبدیل استیکر یا اموجی سرور (یا ویدیو روی پیام ریپلای‌شده) به GIF متحرک',
      '.png ⟹ تبدیل استیکر یا اموجی سرور در پیام ریپلای‌شده به تصویر PNG',
      '.addst ⟹ ساخت استیکر سرور از اموجی، استیکر یا تصویر موجود در پیام ریپلای‌شده',
      '.addem ⟹ ساخت اموجی سرور از اموجی، استیکر یا تصویر موجود در پیام ریپلای‌شده',
      '.deleteem ⟹ حذف اموجی سرور با ریپلای روی پیام حاوی آن اموجی',
      '.deletest ⟹ حذف استیکر سرور با ریپلای روی پیام حاوی آن استیکر',
      '',
      '🤖 هوش مصنوعی و فوتبال',
      '.chat سوال / .چت سوال ⟹ چت با هوش مصنوعی (متن/عکس، با امکان ریپلای)',
      '.pic توضیح عکس / .عکس توضیح عکس ⟹ ساخت تصویر با هوش مصنوعی',
      '.football تیم1 vs تیم2 / .فوتبال تیم1 vs تیم2 ⟹ نتایج و اطلاعات بازی‌های فوتبال',
    ];
    const embed = new EmbedBuilder()
      .setTitle('راهنمای دستورات')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .best — top 20 Hokm winners (by wins)
  if (isCmd('best') || isCmd('بست')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const gId = msg.guildId!;

    const stats = hokmStats.get(gId);
    if (!stats || stats.size === 0) { await msg.reply({ content: 'در این سرور بازی انجام نشده است.' }); return; }
    const entries = Array.from(stats.entries()) as Array<[string, HokmUserStat]>;
    const arr = entries
      .filter(([uid, st]) => ((st?.games) || 0) > 0 && !isVirtualBot(uid)) // Exclude bots
      .sort((a: [string, HokmUserStat], b: [string, HokmUserStat]) => ((b[1].wins || 0) - (a[1].wins || 0)) || ((b[1].games || 0) - (a[1].games || 0)))
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
      lines.push(`### ➡ ${rank} - <@${uid}> 🎮Games : ${st.games || 0} 💫WIN: ${st.wins || 0}`);
    }
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    const embedBest = new EmbedBuilder().setDescription(lines.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embedBest] });
    return;
  }

  // .bazikon — show user's Hokm stats
  if (isCmd('bazikon') || isCmd('بازیکن')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const gId = msg.guildId!;
    const targetIds = await resolveTargetIds(msg, content, content.startsWith('.بازیکن') ? '.بازیکن' : '.bazikon');
    const targetId = targetIds[0] || msg.author.id;
    const stMap = hokmStats.get(gId);
    const st: HokmUserStat = stMap?.get(targetId) || { games: 0, wins: 0, teammateWins: {}, hokmPicks: {} };
    if (!st.games) { await msg.reply({ content: 'این کاربر بازی انجام نداده است.' }); return; }
    
    // Best teammate (exclude bots)
    let bestMate: string | null = null; let bestWins = 0;
    for (const [uid, w] of Object.entries((st.teammateWins || {}) as Record<string, number>)) {
      if (isVirtualBot(uid)) continue; // Skip bots
      const val = Number(w) || 0;
      if (val > bestWins) { bestWins = val; bestMate = uid; }
    }
    const mateText = bestMate ? `<@${bestMate}> (${bestWins} WIN)` : '—';
    
    // Favorite hokm (only show suit(s) with most picks)
    const picks = (st.hokmPicks || {}) as Partial<Record<Suit, number>>;
    const suitOrder: Suit[] = ['C', 'S', 'D', 'H'];
    const sortedSuits = suitOrder.sort((a, b) => (picks[b] || 0) - (picks[a] || 0));
    const maxPicks = picks[sortedSuits[0]] || 0;
    const favArray = maxPicks > 0 
      ? sortedSuits.filter(su => picks[su] === maxPicks).map(su => SUIT_EMOJI[su as Suit])
      : [];
    const favText = favArray.length > 0 ? favArray.join(' ') : '—';
    const lines: string[] = [];
    lines.push(`## 𖣔 <@${targetId}> Stats:`);
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    lines.push(`### 🎮 Games : ${st.games || 0}`);
    lines.push(`### 💫 WIN: ${st.wins || 0}`);
    lines.push('### ◦⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯◦');
    lines.push(`### 🀄 Trick: ${st.tricks || 0}`);
    lines.push(`### 🎯 Set: ${st.sets || 0}`);
    lines.push('### ◦⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯◦');
    lines.push(`### ⭐ Kot: ${st.kot || 0}`);
    lines.push(`### ❌ Kot Lose: ${st.kotLose || 0}`);
    lines.push('### ◦⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯◦');
    lines.push(`### 💎 Hakem Kot: ${st.hakemKot || 0}`);
    lines.push(`### ☠️ HakemKot Lose: ${st.hakemKotLose || 0}`);
    lines.push('### ◦⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯◦');
    lines.push(`### 🫂 Best Teamate: ${mateText}`);
    lines.push(`### 🃏 Favorite hokm: ${favText}`);
    const embedBaz = new EmbedBuilder().setDescription(lines.join('\n')).setColor(0x2f3136);
    await msg.reply({ embeds: [embedBaz] });
    return;
  }

  // .voice [@user] - Total voice duration
  if (isCmd('voice') || isCmd('ویس')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const gId = msg.guildId!;
    const cmdName = content.startsWith('.ویس') ? '.ویس' : '.voice';
    const targetIds = await resolveTargetIds(msg, content, cmdName);
    const targetId = targetIds[0] || msg.author.id;
    
    let totalMs = await store.getVoiceDuration(gId, targetId);
    
    // Add ongoing session if user is currently in voice
    const uvMap = userVoiceStarts.get(gId);
    const currentStart = uvMap?.get(targetId);
    if (currentStart) {
      totalMs += (Date.now() - currentStart);
    }

    if (totalMs <= 0) {
      await msg.reply('این کاربر زمان حضور در ویس ندارد.');
      return;
    }

    const durationStr = formatDuration(totalMs);
    const embedVoice = new EmbedBuilder()
      .setDescription(`## ✵ VOICE STATS:\n### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●\n### ➡ <@${targetId}>\n### ⌛ Total Voice: ${durationStr}\n### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●`)
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embedVoice] });
    return;
  }

  // .topvoice - Leaderboard for voice time
  if (isCmd('topvoice') || isCmd('تاپ‌ویس')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const gId = msg.guildId!;
    const topData = await store.getTopVoice(gId, 20);
    
    if (topData.length === 0) {
      await msg.reply('هنوز اطلاعاتی برای این سرور ثبت نشده است.');
      return;
    }

    const lines: string[] = [];
    const serverName = msg.guild.name.toUpperCase();
    lines.push(`## ✵ ${serverName} TOP VOICE:`);
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    
    for (let i = 0; i < topData.length; i++) {
      const entry = topData[i];
      let totalMs = entry.total_ms;
      
      // Add ongoing session if active
      const uvMap = userVoiceStarts.get(gId);
      const currentStart = uvMap?.get(entry.user_id);
      if (currentStart) {
        totalMs += (Date.now() - currentStart);
      }
      
      const rank = String(i + 1).padStart(2, '0');
      lines.push(`### ➡ ${rank} - <@${entry.user_id}> ⌛ ${formatDuration(totalMs)}`);
    }
    lines.push('### ●▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬●');
    
    const embedTopVoice = new EmbedBuilder()
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embedTopVoice] });
    return;
  }

  // .friend [@user|userId] or .friends
  if (isCmd('friend') || isCmd('friends') || isCmd('دوست')) {
    const cmdLen = content.startsWith('.friends') ? 8 : content.startsWith('.دوست') ? 5 : 7;
    const arg = content.slice(cmdLen).trim();

    const args = arg ? arg.split(/\s+/).filter(Boolean) : [];
    const pairIds: string[] = [];

    // اول از منشن‌ها آی‌دی‌ها را جمع می‌کنیم
    for (const u of msg.mentions.users.values()) {
      if (pairIds.length >= 2) break;
      if (!pairIds.includes(u.id)) pairIds.push(u.id);
    }
    // سپس از توکن‌ها (آی‌دی یا منشن به صورت متن)
    for (const token of args) {
      if (pairIds.length >= 2) break;
      const m = token.match(/^<@!?(\d+)>$/);
      if (m) {
        if (!pairIds.includes(m[1])) pairIds.push(m[1]);
        continue;
      }
      if (/^\d+$/.test(token) && !pairIds.includes(token)) {
        pairIds.push(token);
      }
    }

    // حالت مقایسه دو نفره: .friend @user1 @user2
    if (pairIds.length >= 2 && msg.guild) {
      const gId = msg.guildId!;
      const [id1, id2] = pairIds;

      if (id1 !== id2) {
        const [user1, user2] = await Promise.all([
          msg.client.users.fetch(id1).catch(() => null),
          msg.client.users.fetch(id2).catch(() => null),
        ]);
        if (!user1 || !user2) {
          await msg.reply({ content: 'خطایی در دریافت اطلاعات کاربران رخ داد.' });
          return;
        }

        const totals1 = computeTotalsUpToNow(gId, id1);
        const totalMs = totals1?.get(id2) || 0;
        if (!totalMs || totalMs <= 0) {
          await msg.reply({ content: 'هنوز دیتای مشترکی برای این دو نفر در ویس ثبت نشده است.' });
          return;
        }

        const buildRank = async (selfId: string, otherId: string): Promise<number | null> => {
          const map = computeTotalsUpToNow(gId, selfId);
          if (!map || map.size === 0) return null;
          const rawEntries = Array.from(map.entries()).filter(([pid]) => pid !== selfId);
          const entries: Array<[string, number]> = [];
          for (const [pid, ms] of rawEntries) {
            try {
              const member = await msg.guild!.members.fetch(pid).catch(() => null);
              if (member && !member.user.bot) entries.push([pid, ms]);
            } catch {}
          }
          if (entries.length === 0) return null;
          entries.sort((a, b) => b[1] - a[1]);
          const idx = entries.findIndex(([pid]) => pid === otherId);
          return idx >= 0 ? idx + 1 : null;
        };

        const [rank1, rank2] = await Promise.all([
          buildRank(id1, id2),
          buildRank(id2, id1),
        ]);

        const lines: string[] = [];
        lines.push(`**⏱️ زمان هم‌حضوری روی ویس:** ${formatDuration(totalMs)}`);
        lines.push('');
        lines.push(`**${user1} friend:**`);
        lines.push(rank1 ? `${rank1}. <@${id2}>` : 'دیتای کافی برای رتبه‌بندی این دوست وجود ندارد.');
        lines.push('');
        lines.push(`**${user2} friend:**`);
        lines.push(rank2 ? `${rank2}. <@${id1}>` : 'دیتای کافی برای رتبه‌بندی این دوست وجود ندارد.');

        const embed = new EmbedBuilder()
          .setTitle('friends')
          .setDescription(lines.join('\n'))
          .setColor(0x2f3136);
        await msg.reply({ embeds: [embed] });
        return;
      }

      // اگر دو آی‌دی یکسان باشند، مثل حالت عادی رفتار می‌کنیم (لیست دوستان یک نفر)
      if (pairIds[0]) {
        try {
          const u = await msg.client.users.fetch(pairIds[0]).catch(() => null);
          if (u) msg.mentions.users.set(u.id, u);
        } catch {}
      }
    }

    // حالت عادی: یک کاربر یا بدون آرگومان
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
      const totalSeconds = Math.floor(ms / 1000);
      const days = Math.floor(totalSeconds / 86400);
      const hours = Math.floor((totalSeconds % 86400) / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;
      if (days > 0) return `${days}d ${hours}h`;
      if (hours > 0) return `${hours}h ${minutes}m`;
      if (minutes > 0) return `${minutes}m ${seconds}s`;
      return `${seconds}s`;
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

  // .topfriend / .topfriends / .top / .تاپ — top voice pairs
  if (isCmd('topfriend') || isCmd('topfriends') || isCmd('top') || isCmd('تاپ')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const gId = msg.guildId!;
    const startTime = Date.now();
    
    // ذخیره فوری جلسات فعلی قبل از محاسبه مجموعه
    await saveCurrent(gId);
    
    // نقشه‌ای برای ذخیره جمع مدت‌های هر جفت
    const pairTotals = new Map<string, { a: string; b: string; ms: number }>();
    
    // نقشه‌ای برای تشخیص بات‌ها
    const botFlags = new Map<string, boolean>();
    
    // 1. بررسی مقادیر ذخیره‌شده در پایگاه داده
    const baseGuild = partnerTotals.get(gId);
    if (baseGuild) {
      // ابتدا از شکل ذخیره‌شده در ذهن استفاده می‌کنیم (user -> partner -> ms)
      for (const [a, partners] of baseGuild.entries()) {
        for (const [b, ms] of partners.entries()) {
          // ترتیب منظم برای جلوگیری از تکرار
          const [x, y] = a < b ? [a, b] : [b, a];
          const key = `${x}:${y}`;
          
          // فقط زمانی که a < b است محاسبه می‌کنیم تا دوباره‌کاری نشود
          // چون در partnerTotals هر جفت دو طرفه ذخیره شده (a->b و b->a)
          if (a < b) {
            // دریافت یا ایجاد رکورد برای این جفت
            const pair = pairTotals.get(key) || { a: x, b: y, ms: 0 };
            
            // فقط یکبار زمان را اضافه می‌کنیم (زمان مشترک واقعی)
            pair.ms += ms;
            
            // ذخیره به‌روز شده در مجموعه
            pairTotals.set(key, pair);
          }
        }
      }
    }
    
    // 2. اضافه کردن جلسات جاری (در عمل، با استفاده از ذخیره فوری قبلی، این بخش نادیده گرفته می‌شود)
    const pMap = pairStarts.get(gId);
    if (pMap && pMap.size > 0) {
      const now = Date.now();
      for (const [key, start] of pMap.entries()) {
        try {
          const parts = key.split(':');
          if (parts.length < 3) continue;
          
          const a = parts[0];
          const b = parts[1];
          const [x, y] = a < b ? [a, b] : [b, a];
          const pairKey = `${x}:${y}`;
          
          // ورودی را بگیر یا ایجاد کن
          const pair = pairTotals.get(pairKey) || { a: x, b: y, ms: 0 };
          
          // زمان جاری را اضافه کن
          const delta = now - start;
          if (delta > 0) {
            pair.ms += delta;
            pairTotals.set(pairKey, pair);
          }
        } catch (err) {
          console.error(`[TOPFRIENDS] Error processing active session ${key}:`, err);
        }
      }
    }
    
    // بررسی برای داده‌های خالی
    if (pairTotals.size === 0) {
      await msg.reply({ content: 'هیچ زوجی یافت نشد.' });
      return;
    }
    
    // مرتب‌سازی بر اساس زمان نزولی
    const allPairs = Array.from(pairTotals.values())
      .sort((p, q) => q.ms - p.ms);
    
    // تابع قالب‌بندی زمان با پشتیبانی از روز
    const fmtTop = (ms: number) => {
      const totalSeconds = Math.floor(ms / 1000);
      const days = Math.floor(totalSeconds / 86400);
      const hours = Math.floor((totalSeconds % 86400) / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;
      
      if (days > 0) {
        // اگر روز داشتیم، فقط روز و ساعت نشان می‌دهیم (دقیقه نه)
        return `${days}d ${hours}h`;
      }
      if (hours > 0) {
        return `${hours}h ${minutes}m`;
      }
      if (minutes > 0) {
        return `${minutes}m ${seconds}s`;
      }
      return `${seconds}s`;
    };
    
    // ایجاد خطوط نتیجه برای 10 جفت برتر (بدون بات‌ها)
    const linesTop: string[] = [];
    const processedCount = { total: 0, bots: 0, added: 0, missing: 0 };
    
    // ابتدا سعی کنید بات‌ها را از کش شناسایی کنید تا در ادامه از آن‌ها اجتناب شود
    msg.guild.members.cache.forEach(m => {
      if (m.user.bot) botFlags.set(m.id, true);
    });
    
    // پردازش جفت‌ها برای ایجاد خطوط نتیجه
    for (const p of allPairs) {
      if (linesTop.length >= 10) break;
      processedCount.total++;
      
      // بررسی سریع برای بات‌ها از کش
      if (botFlags.get(p.a) || botFlags.get(p.b)) {
        processedCount.bots++;
        continue;
      }
      
      // تلاش برای دریافت اعضا از کش
      let m1 = msg.guild.members.cache.get(p.a);
      let m2 = msg.guild.members.cache.get(p.b);
      
      // دریافت اعضای کش نشده
      try { 
        if (!m1) {
          const fetchedMember = await msg.guild.members.fetch(p.a).catch(() => undefined);
          m1 = fetchedMember || undefined;
        } 
        if (!m2) {
          const fetchedMember = await msg.guild.members.fetch(p.b).catch(() => undefined);
          m2 = fetchedMember || undefined;
        }
      } catch {}
      
      // بررسی کنید اگر هر یک از اعضا نادرست باشند
      if (!m1 || !m2) {
        processedCount.missing++;
        continue;
      }
      
      // بررسی کنید که هیچ کدام بات نباشند
      if (m1.user.bot || m2.user.bot) {
        botFlags.set(m1.user.bot ? m1.id : m2.id, true);
        processedCount.bots++;
        continue;
      }
      
      // افزودن به نتایج
      linesTop.push(`${linesTop.length + 1}. <@${p.a}> + <@${p.b}> — ${fmtTop(p.ms)}`);
      processedCount.added++;
    }
    
    // بررسی برای نتایج خالی
    if (linesTop.length === 0) {
      await msg.reply({ content: 'هیچ زوج غیر باتی یافت نشد.' });
      return;
    }

    // ساخت و ارسال امبد نتایج
    const embedTop = new EmbedBuilder()
      .setTitle('👥 زوج‌های برتر ویس')
      .setDescription(linesTop.join('\n'))
      .setColor(0x2f3136);
    await msg.reply({ embeds: [embedTop] });

    // لاگ برای تشخیص مشکلات
    const endTime = Date.now();
    console.log(`[TOPFRIENDS] Generated in ${endTime - startTime}ms - Processed ${processedCount.total} pairs: ${processedCount.added} added, ${processedCount.bots} bots, ${processedCount.missing} missing members`);
    
    return;
  }

  if (isCmd('pic') || isCmd('عکس')) {
    const cmdLen = 4;
    let prompt = content.slice(cmdLen).trim();
    if (!prompt) {
      await msg.reply({ content: 'استفاده: `.pic توضیح عکس`\nمثال: `.pic یک گربه در فضا`' });
      return;
    }
    try {
      try {
        await msg.channel.sendTyping();
      } catch {}
      const originalPrompt = prompt;
      let promptForApi = prompt;
      try {
        promptForApi = await translatePromptToEnglishForImage(originalPrompt);
      } catch (translateErr) {
        console.error('[PIC TRANSLATE ERROR]', translateErr);
        promptForApi = originalPrompt;
      }
      const buffer = await generateImageWithStability(promptForApi);
      let caption = originalPrompt;
      if (caption.length > 1800) {
        caption = caption.slice(0, 1800) + '…';
      }
      const attachment = new AttachmentBuilder(buffer, { name: 'ai-image.png' });
      await msg.reply({
        content: `🖼️ ${caption}`,
        files: [attachment],
      });
    } catch (err: any) {
      console.error('[STABILITY PIC ERROR]', err);
      const msgText = err instanceof Error && err.message.includes('STABILITY_API_KEY')
        ? '❌ توکن Stability AI تنظیم نشده است. لطفاً با مالک بات تماس بگیر.'
        : '❌ خطا در تولید تصویر. لطفاً بعداً دوباره تلاش کن.';
      try {
        await msg.reply({ content: msgText });
      } catch {}
    }
    return;
  }

  // .chat / .چت — AI text chat (general questions, with history and reply context)
  if (isCmd('chat') || isCmd('چت')) {
    const cmdLen = content.startsWith('.چت') ? 3 : 5;
    let prompt = content.slice(cmdLen).trim();

    let replyText: string | undefined;
    let replyImageUrl: string | null = null;

    if (msg.reference?.messageId) {
      try {
        const replied = await msg.channel.messages.fetch(msg.reference.messageId);
        if (replied.content) {
          replyText = replied.content;
        }
        const att = replied.attachments.find(a => {
          const ct = a.contentType || '';
          const name = (a.name || '').toLowerCase();
          if (ct.startsWith('image/')) return true;
          return name.endsWith('.png') || name.endsWith('.jpg') || name.endsWith('.jpeg') || name.endsWith('.webp');
        });
        if (att) {
          replyImageUrl = att.url;
        }
      } catch {
        // ignore fetch errors
      }
    }

    if (!prompt && !replyText && !replyImageUrl) {
      await msg.reply({ content: 'استفاده: `.chat سوالت` یا `.چت سوالت` (می‌توانی روی یک پیام یا عکس هم ریپلای کنی)' });
      return;
    }

    // If only reply exists and no explicit prompt, create a generic request
    if (!prompt && (replyText || replyImageUrl)) {
      if (replyText && !replyImageUrl) {
        prompt = 'این پیام را تحلیل و در صورت نیاز ترجمه کن.';
      } else if (replyImageUrl && !replyText) {
        prompt = 'این تصویر را توصیف و تحلیل کن.';
      } else {
        prompt = 'بر اساس این پیام و تصویر پاسخ مناسب بده.';
      }
    }

    try {
      try {
        await msg.channel.sendTyping();
      } catch {}
      const aiText = await generateAiReply(prompt, msg.author.id, msg.channelId, replyText, replyImageUrl);
      let reply = aiText.trim();
      if (!reply) {
        reply = 'پاسخی از هوش مصنوعی دریافت نشد.';
      }
      if (reply.length > 1900) {
        reply = reply.slice(0, 1900) + '\n...';
      }
      await msg.reply({ content: reply });
    } catch (err) {
      console.error('[AI CHAT ERROR]', err);

      const errText = String((err as any)?.message || err || '');
      if (errText.includes('GEMINI_API_KEY is not set')) {
        await msg.reply({
          content: '❌ توکن Gemini تنظیم نشده است. لطفاً `GEMINI_API_KEY` را در تنظیمات ست کن.',
        });
      } else if (errText.includes('429') || errText.includes('quota')) {
        await msg.reply({
          content: '❌ سهمیه (Quota) اکانت Gemini تمام شده یا ریت‌لیمیت شده‌ای. لطفاً بعداً تلاش کن یا کلید را عوض کن.',
        });
      } else {
        await msg.reply({ content: 'خطا در تماس با هوش مصنوعی (Gemini). لطفاً بعداً دوباره تلاش کن.' });
      }
    }
    return;
  }

  // .new — create room with join buttons (supports up to 4 concurrent sessions per channel)
  if (isCmd('new') || isCmd('hokm') || isCmd('حکم')) {
    // ...
    const gId = msg.guildId!; const cId = msg.channelId;
    // Check if this user already has an active table in this channel
    const userActiveTables = getChannelSessions(gId, cId).filter(s => 
      s.ownerId === msg.author.id && s.state !== 'finished'
    );
    if (userActiveTables.length > 0) {
      const tableNum = userActiveTables[0].sessionId;
      await msg.reply(`شما قبلاً میز ${tableNum} را در این کانال ساخته‌اید. لطفاً ابتدا آن بازی را تمام کنید یا با دستور \`.end\` آن را ببندید.`);
      return;
    }
    
    const activeCount = countActiveGames(gId, cId);
    if (activeCount >= 4) {
      await msg.reply('حداکثر ۴ بازی همزمان در این کانال مجاز است. لطفاً منتظر بمانید تا یکی از بازی‌ها تمام شود.');
      return;
    }
    // Create brand-new session for this request
    const sNew = createNewSession(gId, cId, msg.author.id);
    sNew.team1 = []; sNew.team2 = []; sNew.order = []; sNew.hakim = undefined; sNew.hokm = undefined; sNew.deck = []; sNew.hands.clear(); sNew.state = 'waiting'; sNew.tableMsgId = undefined;
    const contentTextNew = `🎮 **میز ${sNew.sessionId}**\n${controlListText(sNew)}`;
    const rowsNew = buildControlButtons(sNew.sessionId);
    const sentNew = await msg.reply({ content: contentTextNew, components: rowsNew });
    sNew.controlMsgId = sentNew.id;
    return;
  }

  // .a1 @user — owner assigns user to Team 1
  if (isCmd('a1') || isCmd('اضافه1')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را اضافه کند.'); return; }
    const raw = content.slice(content.startsWith('.اضافه1') ? 7 : 3).trim();
    if (/^bot\b/i.test(raw)) {
      const added = addBotToTeam(s, 1);
      const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
      const rows = buildControlButtons(s.sessionId);
      try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      const replyMsg = await msg.reply({ content: added? `Bot به تیم 1 افزوده شد (${added.id.replace('BOT','Bot')}).` : 'امکان افزودن Bot به تیم 1 وجود ندارد.' });
      setTimeout(() => replyMsg.delete().catch(()=>{}), 2500);
      return;
    }
    const targets = await resolveTargetIds(msg, content, content.startsWith('.اضافه1') ? '.اضافه1' : '.a1');
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
    const rows = buildControlButtons(s.sessionId);
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
    await msg.reply({ content: `افزوده شد: ${added.join(' , ') || '—'}` });
    return;
  }

  // .a2 @user — owner assigns user to Team 2
  if (isCmd('a2') || isCmd('اضافه2')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را اضافه کند.'); return; }
    const raw = content.slice(content.startsWith('.اضافه2') ? 7 : 3).trim();
    if (/^bot\b/i.test(raw)) {
      const added = addBotToTeam(s, 2);
      const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
      const rows = buildControlButtons(s.sessionId);
      try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      const replyMsg = await msg.reply({ content: added? `Bot به تیم 2 افزوده شد (${added.id.replace('BOT','Bot')}).` : 'امکان افزودن Bot به تیم 2 وجود ندارد.' });
      setTimeout(() => replyMsg.delete().catch(()=>{}), 2500);
      return;
    }
    const targets = await resolveTargetIds(msg, content, content.startsWith('.اضافه2') ? '.اضافه2' : '.a2');
    if (targets.length === 0) { await msg.reply('استفاده: `.a2 @user1 @user2` یا `.a2 bot`'); return; }
    const added: string[] = []; const skipped: string[] = [];
    for (const uid of targets) {
      try { const u = await msg.client.users.fetch(uid); if (u.bot) { skipped.push(`<@${uid}> (bot)`); continue; } } catch { skipped.push(`<@${uid}> (نامعتبر)`); continue; }
      if (s.team2.includes(uid)) { skipped.push(`<@${uid}> (قبلاً تیم 2)`); continue; }
      s.team1 = s.team1.filter(x=>x!==uid); s.team2 = s.team2.filter(x=>x!==uid);
      if (s.team2.length >= 2) { skipped.push(`<@${uid}> (تیم 2 پر است)`); continue; }
      s.team2.push(uid); added.push(`<@${uid}>`);
    }
    const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
    const rows = buildControlButtons(s.sessionId);
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
    {
      const lines: string[] = [];
      lines.push(`افزوده شد: ${added.join(' , ') || '—'}`);
      if (skipped.length > 0) lines.push(`نادیده: ${skipped.join(' , ')}`);
      await msg.reply({ content: lines.join('\n') });
    }
    return;
  }

  // .rem — owner removes a user from teams
  if (isCmd('rem') || isCmd('ح')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    if (s.state !== 'waiting') { await msg.reply('فقط قبل از شروع بازی قابل انجام است.'); return; }
    if (s.ownerId && msg.author.id !== s.ownerId) { await msg.reply('فقط سازنده اتاق می‌تواند اعضا را حذف کند.'); return; }
    // special: remove virtual bots with `.rem bot`
    const rawArg = content.slice(content.startsWith('.ح') ? 3 : 4);
    if (/^\s*bot/i.test(rawArg)) {
      const before1 = [...s.team1];
      const before2 = [...s.team2];
      s.team1 = s.team1.filter(u=>!isVirtualBot(u));
      s.team2 = s.team2.filter(u=>!isVirtualBot(u));
      const removedBots: string[] = [];
      for (const u of before1) if (isVirtualBot(u) && !s.team1.includes(u)) removedBots.push(`<@${u.replace('BOT','Bot')}>`);
      for (const u of before2) if (isVirtualBot(u) && !s.team2.includes(u)) removedBots.push(`<@${u.replace('BOT','Bot')}>`);
      const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
      const rows = buildControlButtons(s.sessionId);
      try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.edit({ content: contentText, components: rows }); } } catch {}
      const replyMsg = await msg.reply({ content: `حذف شد: ${removedBots.join(' , ') || '—'}` });
      setTimeout(()=>replyMsg.delete().catch(()=>{}), 2500);
      return;
    }
    const targets = await resolveTargetIds(msg, content, content.startsWith('.ح') ? '.ح' : '.rem');
    if (targets.length === 0) { await msg.reply('استفاده: `.rem @user1 @user2` یا ریپلای/آیدی'); return; }
    const removed: string[] = []; const notIn: string[] = [];
    for (const uid of targets) {
      const inAny = s.team1.includes(uid) || s.team2.includes(uid);
      s.team1 = s.team1.filter(x=>x!==uid);
      s.team2 = s.team2.filter(x=>x!==uid);
      if (inAny) removed.push(`<@${uid}>`); else notIn.push(`<@${uid}>`);
    }
    const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
    const rows = buildControlButtons(s.sessionId);
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
    // Clear turn timeout before ending game
    clearTurnTimeout(s);
    // delete control and table messages if exist
    try { if (s.controlMsgId) { const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (m) await m.delete().catch(()=>{}); } } catch {}
    try { if (s.tableMsgId) { const m2 = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null); if (m2) await m2.delete().catch(()=>{}); } } catch {}
    // clear session
    s.team1 = []; s.team2 = []; s.order = []; s.hakim = undefined; s.hokm = undefined; s.deck = []; s.hands.clear(); s.state = 'finished'; s.controlMsgId = undefined; s.tableMsgId = undefined;
    await msg.reply('اتاق پایان یافت.');
    return;
  }

  // .change <player1> <player2> — swap players (supports @user or bot1/bot2/bot3)
  if (isCmd('change') || isCmd('عوض')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    
    // Only owner can change players
    if (!s.ownerId || msg.author.id !== s.ownerId) { 
      await msg.reply('فقط سازنده اتاق می‌تواند بازیکنان را جایگزین کند.'); 
      return; 
    }
    
    // Parse arguments - can be @user mentions or "bot1", "bot2", "bot3"
    const args = content.slice(content.startsWith('.عوض') ? 5 : '.change'.length).trim().split(/\s+/).filter(Boolean);
    if (args.length !== 2) {
      await msg.reply('استفاده: `.change @user bot1` یا `.change bot1 @user` یا `.change @user1 @user2` یا `.change bot1 bot2`');
      return;
    }
    
    // Helper to parse player ID from argument
    const parsePlayerId = (arg: string): string | null => {
      // Check if it's a bot reference (bot1, bot2, bot3)
      const botMatch = arg.match(/^bot([123])$/i);
      if (botMatch) {
        return `BOT${botMatch[1]}`;
      }
      
      // Check if it's a user mention <@12345>
      const mentionMatch = arg.match(/^<@!?(\d+)>$/);
      if (mentionMatch) {
        return mentionMatch[1];
      }
      
      // Check if it's a raw user ID
      if (/^\d{17,20}$/.test(arg)) {
        return arg;
      }
      
      return null;
    };
    
    const id1 = parsePlayerId(args[0]);
    const id2 = parsePlayerId(args[1]);
    
    if (!id1 || !id2) {
      await msg.reply('فرمت نامعتبر. استفاده: `.change @user bot1` یا `.change bot1 @user`');
      return;
    }
    
    if (id1 === id2) {
      await msg.reply('نمی‌توانید یک بازیکن را با خودش جایگزین کنید.');
      return;
    }
    
    // Determine which one is in game and which is out/replacement
    const id1InGame = s.order.includes(id1);
    const id2InGame = s.order.includes(id2);
    
    if (id1InGame && id2InGame) {
      await msg.reply('هر دو بازیکن در بازی هستند. یکی باید داخل و یکی باید خارج/جایگزین باشد.');
      return;
    }
    
    if (!id1InGame && !id2InGame) {
      await msg.reply('هیچکدام از بازیکنان در بازی نیستند. یکی باید داخل بازی باشد.');
      return;
    }
    
    // Find in/out players
    const inGameId = id1InGame ? id1 : id2;
    const replacementId = id1InGame ? id2 : id1;
    
    // Check if replacement is a real user (not bot) and validate it
    if (!isVirtualBot(replacementId)) {
      try {
        const u = await msg.client.users.fetch(replacementId);
        if (u.bot && !isVirtualBot(replacementId)) {
          await msg.reply('نمی‌توانید بات دیسکورد واقعی را جایگزین کنید.');
          return;
        }
      } catch {
        await msg.reply('کاربر جایگزین نامعتبر است.');
        return;
      }
    }
    
    // Find team and position
    let team: 1 | 2 | null = null;
    let teamPos = -1;
    if (s.team1.includes(inGameId)) {
      team = 1;
      teamPos = s.team1.indexOf(inGameId);
    } else if (s.team2.includes(inGameId)) {
      team = 2;
      teamPos = s.team2.indexOf(inGameId);
    }
    
    if (team === null) {
      await msg.reply('خطا: بازیکن در بازی یافت نشد.');
      return;
    }
    
    // Swap in teams
    if (team === 1) {
      s.team1[teamPos] = replacementId;
    } else {
      s.team2[teamPos] = replacementId;
    }
    
    // Swap in order (maintain play order position)
    const orderPos = s.order.indexOf(inGameId);
    if (orderPos >= 0) {
      s.order[orderPos] = replacementId;
    }
    
    // Transfer hand if game is in progress
    if (s.state === 'playing' || s.state === 'choosing_hokm') {
      const hand = s.hands.get(inGameId) || [];
      s.hands.delete(inGameId);
      s.hands.set(replacementId, hand);
      
      // Transfer tricksByPlayer for current set
      if (s.tricksByPlayer) {
        const currentTricks = s.tricksByPlayer.get(inGameId) || 0;
        s.tricksByPlayer.delete(inGameId);
        s.tricksByPlayer.set(replacementId, currentTricks);
      }
      
      // Transfer allTricksByPlayer (accumulated tricks across all sets)
      if (s.allTricksByPlayer) {
        const allTricks = s.allTricksByPlayer.get(inGameId) || 0;
        s.allTricksByPlayer.delete(inGameId);
        s.allTricksByPlayer.set(replacementId, allTricks);
      }
      
      // If old player was hakim, transfer to new player
      if (s.hakim === inGameId) {
        s.hakim = replacementId;
        
        // If new hakim is a bot and we're in choosing_hokm state, auto-choose
        if (isVirtualBot(replacementId) && s.state === 'choosing_hokm') {
          await botChooseHokmAndStart(msg.client as Client, msg.channel, s);
        }
      }
      
      // Clear card order cache for new player (if real user)
      if (!isVirtualBot(replacementId)) {
        const orderKey = `__hokm_card_order_${s.guildId}:${s.channelId}:${replacementId}`;
        delete (global as any)[orderKey];
      }
    }
    
    // Update control message if in waiting state
    if (s.state === 'waiting' && s.controlMsgId) {
      const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
      const rows = buildControlButtons(s.sessionId);
      try { 
        const m = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); 
        if (m) await m.edit({ content: contentText, components: rows }); 
      } catch {}
    }
    
    // Refresh table if game is active
    if (s.state !== 'waiting') {
      try {
        // forceRender=true چون بعد از replace/swap
        await refreshTableEmbed({ channel: msg.channel }, s, true); 
        // Trigger bot auto-play if it's now bot's turn
        if (s.state === 'playing') {
          await maybeBotAutoPlay(msg.client as Client, s);
        }
      } catch {}
    }
    
    const oldName = isVirtualBot(inGameId) ? inGameId.replace('BOT', 'Bot') : `<@${inGameId}>`;
    const newName = isVirtualBot(replacementId) ? replacementId.replace('BOT', 'Bot') : `<@${replacementId}>`;
    await msg.reply(`بازیکن ${oldName} با ${newName} جایگزین شد.`);
    return;
  }

  // .list — recreate control list (ONLY between game creation and game start)
  if (isCmd('list')) {
    if (!msg.guild) { return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    
    // Check if game is created (has owner/control message)
    if (!s.ownerId && !s.controlMsgId) {
      return;
    }
    
    // Only work in 'waiting' state (between creation and start)
    if (s.state !== 'waiting') {
      return;
    }
    
    // Recreate control list
    // delete previous control message if exists
    if (s.controlMsgId) {
      try { const prev = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null); if (prev) await prev.delete().catch(()=>{}); } catch {}
      s.controlMsgId = undefined;
    }
    const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
    const rows = buildControlButtons(s.sessionId);
    const sent = await msg.reply({ content: contentText, components: rows });
    s.controlMsgId = sent.id;
    return;
  }

  // .reset — reset game to waiting state (owner only, silent)
  if (isCmd('reset') || isCmd('ریست')) {
    if (!msg.guild) { return; }
    const s = ensureSession(msg.guildId!, msg.channelId);
    
    // Check if user is the room owner
    if (!s.ownerId || msg.author.id !== s.ownerId) {
      return;
    }
    
    // Check if game exists
    if (s.order.length === 0) {
      return;
    }
    
    // Reset game to initial state (like .new) while preserving teams
    // Clear turn timeout before resetting
    clearTurnTimeout(s);
    
    s.state = 'waiting';
    s.hokm = undefined;
    s.hakim = undefined;
    s.deck = [];
    s.hands.clear();
    s.table = undefined;
    s.leadSuit = undefined;
    s.leaderIndex = undefined;
    s.turnIndex = undefined;
    s.tricksTeam1 = 0;
    s.tricksTeam2 = 0;
    s.setsTeam1 = 0;
    s.setsTeam2 = 0;
    // Reset Kot and Hakem Kot counters
    s.kotTeam1 = 0;
    s.kotTeam2 = 0;
    s.hakemKotTeam1 = 0;
    s.hakemKotTeam2 = 0;
    // Reset target sets/tricks to allow reconfiguration
    s.targetSets = undefined;
    s.targetTricks = undefined;
    s.tricksByPlayer = new Map();
    // Reset allTricksByPlayer since this is a complete game reset
    s.allTricksByPlayer = new Map();
    s.lastTrick = undefined;
    s.surrenderVotesTeam1 = new Set();
    s.surrenderVotesTeam2 = new Set();
    
    // Delete table message if exists
    if (s.tableMsgId) {
      try {
        const tableMsg = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
        if (tableMsg) await tableMsg.delete().catch(()=>{});
      } catch {}
      s.tableMsgId = undefined;
    }
    
    // Delete previous control message if exists
    if (s.controlMsgId) {
      try {
        const prev = await (msg.channel as any).messages.fetch(s.controlMsgId).catch(()=>null);
        if (prev) await prev.delete().catch(()=>{});
      } catch {}
      s.controlMsgId = undefined;
    }
    
    // Show updated control list with current teams (silently)
    const contentText = `🎮 **میز ${s.sessionId}**\n${controlListText(s)}`;
    const rows = buildControlButtons(s.sessionId);
    const sent = await msg.reply({ content: contentText, components: rows });
    s.controlMsgId = sent.id;
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
 
  // .miz — نمایش میز آخرین بازی فعال کاربر
  if (isCmd('miz') || isCmd('میز')) {
    if (!msg.guild) { await msg.reply('فقط داخل سرور.'); return; }
    
    // Find user's most recent active session
    const s = findUserActiveSession(msg.guildId!, msg.author.id);
    
    if (!s) {
      await msg.reply('شما در هیچ بازی فعالی نیستید.');
      return;
    }
    
    // Check if we're in the correct channel
    if (s.channelId !== msg.channelId) {
      await msg.reply(`میز بازی شما در کانال <#${s.channelId}> است.`);
      return;
    }
    
    // Delete old table message if exists
    if (s.tableMsgId) {
      try {
        const prev = await (msg.channel as any).messages.fetch(s.tableMsgId).catch(()=>null);
        if (prev) await prev.delete().catch(()=>{});
      } catch {}
      s.tableMsgId = undefined;
    }
    
    // Render fresh table
    // forceRender=true چون .miz برای نمایش مجدد میز است
    try { await refreshTableEmbed({ channel: msg.channel }, s, true); } catch {}
    return;
  }

  // .gif / .گیف — convert replied sticker or custom emoji to GIF/image (or by numeric ID)
  if (isCmd('gif') || isCmd('گیف')) {
    const parts = content.split(/\s+/);
    const arg = parts.length > 1 ? parts[1] : undefined;
    await sendEmojiLikeMedia('gif', arg);
    return;
  }

  // .png — same as .gif but PNG output (or by numeric ID)
  if (isCmd('png')) {
    const parts = content.split(/\s+/);
    const arg = parts.length > 1 ? parts[1] : undefined;
    await sendEmojiLikeMedia('png', arg);
    return;
  }

  // .addem — create guild emojis from replied message media/emoji/sticker or by numeric ID
  if (isCmd('addem')) {
    if (!msg.guild) {
      await msg.reply({ content: 'این دستور فقط داخل سرور قابل استفاده است.' });
      return;
    }
    const member = msg.member;
    if (!member || !member.permissions.has(PermissionsBitField.Flags.ManageGuildExpressions)) {
      await msg.reply({ content: 'برای استفاده از این دستور باید دسترسی مدیریت اموجی/استیکر در سرور داشته باشید.' });
      return;
    }

    const parts = content.split(/\s+/);
    const arg = parts.length > 1 ? parts[1] : undefined;
    const numericId = arg && /^\d+$/.test(arg) ? arg : null;

    if (numericId) {
      const urlsToTry = [
        `https://cdn.discordapp.com/emojis/${numericId}.gif?v=1`,
        `https://cdn.discordapp.com/emojis/${numericId}.png?v=1`,
        `https://cdn.discordapp.com/stickers/${numericId}.gif`,
        `https://cdn.discordapp.com/stickers/${numericId}.png`,
      ];
      let createdFromId = 0;
      for (const url of urlsToTry) {
        try {
          const buf = await fetchBuffer(url);
          const name = makeEmojiOrStickerName('emoji', createdFromId);
          await msg.guild.emojis.create({ attachment: buf, name, reason: `Created via .addem by ${msg.author.tag}` });
          createdFromId++;
          break;
        } catch {}
      }
      if (!createdFromId) {
        await msg.reply({ content: 'هیچ اموجی‌ای با این آیدی پیدا نشد یا فایل آن قابل استفاده برای ساخت اموجی نیست.' });
      } else {
        await msg.reply({ content: `✅ ${createdFromId} اموجی ساخته شد.` });
      }
      return;
    }

    if (!msg.reference?.messageId) {
      await msg.reply({ content: 'برای استفاده از این دستور باید روی یک پیام حاوی اموجی، استیکر یا تصویر ریپلای کنید.' });
      return;
    }

    const replied = await msg.fetchReference().catch(() => null as any);
    if (!replied) {
      await msg.reply({ content: 'پیام مورد نظر پیدا نشد.' });
      return;
    }

    const { customEmoji, stickers, imageUrls } = await collectEmojiLikeMediaDeep(replied as Message);
    const sources: { url: string; nameHint: string }[] = [];

    const seenEmoji = new Set<string>();
    for (const e of customEmoji) {
      if (seenEmoji.has(e.id)) continue;
      seenEmoji.add(e.id);
      const ext = e.animated ? 'gif' : 'png';
      const url = `https://cdn.discordapp.com/emojis/${e.id}.${ext}?v=1`;
      sources.push({ url, nameHint: e.name || 'emoji' });
    }

    for (const st of stickers) {
      if (st && typeof st.url === 'string') {
        sources.push({ url: st.url, nameHint: (st.name as string) || 'sticker' });
      }
    }

    for (const url of imageUrls) {
      let base = 'img';
      try {
        const pathPart = new URL(url).pathname.split('/').pop() || '';
        base = pathPart.split('.')[0] || 'img';
      } catch {}
      sources.push({ url, nameHint: base });
    }

    if (!sources.length) {
      let nameCandidates = collectEmojiNamesFromMessage(replied as Message);
      try {
        const original = await resolveForwardedMessage(replied as Message);
        if (original) {
          nameCandidates = nameCandidates.concat(collectEmojiNamesFromMessage(original as Message));
        }
      } catch {}
      const fromEmojiGg = nameCandidates.length
        ? await findEmojiGgByNames(nameCandidates, false)
        : [];
      for (const item of fromEmojiGg) {
        sources.push({ url: item.url, nameHint: item.name });
      }
    }

    if (!sources.length) {
      await msg.reply({ content: 'در پیامی که ریپلای کردید هیچ منبع مناسبی برای ساخت اموجی پیدا نشد.' });
      return;
    }

    let created = 0;
    let failed = 0;
    const maxCreate = 20;

    for (let i = 0; i < sources.length && created < maxCreate; i++) {
      const src = sources[i];
      try {
        const buf = await fetchBuffer(src.url);
        const name = makeEmojiOrStickerName(src.nameHint, created);
        await msg.guild.emojis.create({ attachment: buf, name, reason: `Created via .addem by ${msg.author.tag}` });
        created++;
      } catch (err) {
        console.error('[ADDEM ERROR]', err);
        failed++;
      }
    }

    if (!created && failed) {
      await msg.reply({ content: 'هیچ اموجی‌ای ساخته نشد. ممکن است فرمت فایل یا محدودیت‌های دیسکورد مانع شده باشد.' });
      return;
    }

    await msg.reply({ content: `✅ ${created} اموجی ساخته شد.${failed ? `\n❌ ${failed} مورد با خطا مواجه شد.` : ''}` });
    return;
  }

  // .addst — create guild stickers from replied message media/emoji/sticker or by numeric ID
  if (isCmd('addst')) {
    if (!msg.guild) {
      await msg.reply({ content: 'این دستور فقط داخل سرور قابل استفاده است.' });
      return;
    }
    const member = msg.member;
    if (!member || !member.permissions.has(PermissionsBitField.Flags.ManageGuildExpressions)) {
      await msg.reply({ content: 'برای استفاده از این دستور باید دسترسی مدیریت اموجی/استیکر در سرور داشته باشید.' });
      return;
    }

    const parts = content.split(/\s+/);
    const arg = parts.length > 1 ? parts[1] : undefined;
    const numericId = arg && /^\d+$/.test(arg) ? arg : null;

    if (numericId) {
      const urlsToTry = [
        `https://cdn.discordapp.com/emojis/${numericId}.png?v=1`,
        `https://cdn.discordapp.com/stickers/${numericId}.png`,
        `https://cdn.discordapp.com/emojis/${numericId}.gif?v=1`,
        `https://cdn.discordapp.com/stickers/${numericId}.gif`,
      ];
      let createdFromId = 0;
      for (const url of urlsToTry) {
        try {
          const buf = await fetchBuffer(url);
          const name = makeEmojiOrStickerName('sticker', createdFromId);
          await msg.guild.stickers.create({ file: buf, name, tags: '🙂', description: `Created via .addst by ${msg.author.tag}` });
          createdFromId++;
          break;
        } catch {}
      }
      if (!createdFromId) {
        await msg.reply({ content: 'هیچ استیکری با این آیدی پیدا نشد یا فایل آن قابل استفاده برای ساخت استیکر نیست.' });
      } else {
        await msg.reply({ content: `✅ ${createdFromId} استیکر ساخته شد.` });
      }
      return;
    }

    if (!msg.reference?.messageId) {
      await msg.reply({ content: 'برای استفاده از این دستور باید روی یک پیام حاوی اموجی، استیکر یا تصویر ریپلای کنید.' });
      return;
    }

    const replied = await msg.fetchReference().catch(() => null as any);
    if (!replied) {
      await msg.reply({ content: 'پیام مورد نظر پیدا نشد.' });
      return;
    }

    const { customEmoji, stickers, imageUrls } = await collectEmojiLikeMediaDeep(replied as Message);
    const sources: { url: string; nameHint: string }[] = [];

    const seenEmojiSt = new Set<string>();
    for (const e of customEmoji) {
      if (seenEmojiSt.has(e.id)) continue;
      seenEmojiSt.add(e.id);
      // برای استیکرها، به‌خاطر محدودیت دیسکورد معمولاً از نسخه PNG استفاده می‌کنیم
      const url = `https://cdn.discordapp.com/emojis/${e.id}.png?v=1`;
      sources.push({ url, nameHint: e.name || 'emoji' });
    }

    for (const st of stickers) {
      if (st && typeof st.url === 'string') {
        sources.push({ url: st.url, nameHint: (st.name as string) || 'sticker' });
      }
    }

    for (const url of imageUrls) {
      let base = 'img';
      try {
        const pathPart = new URL(url).pathname.split('/').pop() || '';
        base = pathPart.split('.')[0] || 'img';
      } catch {}
      sources.push({ url, nameHint: base });
    }

    if (!sources.length) {
      let nameCandidates = collectEmojiNamesFromMessage(replied as Message);
      try {
        const original = await resolveForwardedMessage(replied as Message);
        if (original) {
          nameCandidates = nameCandidates.concat(collectEmojiNamesFromMessage(original as Message));
        }
      } catch {}
      const fromEmojiGg = nameCandidates.length
        ? await findEmojiGgByNames(nameCandidates, false)
        : [];
      for (const item of fromEmojiGg) {
        sources.push({ url: item.url, nameHint: item.name });
      }
    }

    if (!sources.length) {
      await msg.reply({ content: 'در پیامی که ریپلای کردید هیچ منبع مناسبی برای ساخت استیکر پیدا نشد.' });
      return;
    }

    let createdSt = 0;
    let failedSt = 0;
    const maxCreateSt = 10; // ظرفیت استیکر محدود است، پس تعداد را کمتر نگه می‌داریم

    for (let i = 0; i < sources.length && createdSt < maxCreateSt; i++) {
      const src = sources[i];
      try {
        const buf = await fetchBuffer(src.url);
        const name = makeEmojiOrStickerName(src.nameHint, createdSt);
        await msg.guild.stickers.create({ file: buf, name, tags: '🙂', description: `Created via .addst by ${msg.author.tag}` });
        createdSt++;
      } catch (err) {
        console.error('[ADDST ERROR]', err);
        failedSt++;
      }
    }

    if (!createdSt && failedSt) {
      await msg.reply({ content: 'هیچ استیکری ساخته نشد. ممکن است فرمت فایل (PNG/APNG/Lottie) یا محدودیت‌های دیسکورد مانع شده باشد.' });
      return;
    }

    await msg.reply({ content: `✅ ${createdSt} استیکر ساخته شد.${failedSt ? `\n❌ ${failedSt} مورد با خطا مواجه شد.` : ''}` });
    return;
  }

  // .deleteem — delete guild emojis referenced in replied message or by numeric ID
  if (isCmd('deleteem')) {
    if (!msg.guild) {
      await msg.reply({ content: 'این دستور فقط داخل سرور قابل استفاده است.' });
      return;
    }
    const member = msg.member;
    if (!member || !member.permissions.has(PermissionsBitField.Flags.ManageGuildExpressions)) {
      await msg.reply({ content: 'برای استفاده از این دستور باید دسترسی مدیریت اموجی/استیکر در سرور داشته باشید.' });
      return;
    }

    const parts = content.split(/\s+/);
    const arg = parts.length > 1 ? parts[1] : undefined;
    const numericId = arg && /^\d+$/.test(arg) ? arg : null;

    if (numericId) {
      let emoji = msg.guild.emojis.cache.get(numericId);
      if (!emoji) {
        try {
          emoji = await msg.guild.emojis.fetch(numericId).catch(() => null as any);
        } catch {
          emoji = null as any;
        }
      }
      if (!emoji) {
        await msg.reply({ content: 'هیچ اموجی‌ای با این آیدی در این سرور پیدا نشد.' });
        return;
      }
      try {
        await emoji.delete(`Deleted via .deleteem by ${msg.author.tag}`);
        await msg.reply({ content: '✅ اموجی مورد نظر از سرور حذف شد.' });
      } catch (err) {
        console.error('[DELETEEM ERROR]', err);
        await msg.reply({ content: '❌ حذف اموجی با خطا مواجه شد. ممکن است به‌خاطر محدودیت دسترسی یا محدودیت‌های دیسکورد باشد.' });
      }
      return;
    }

    if (!msg.reference?.messageId) {
      await msg.reply({ content: 'برای استفاده از این دستور باید روی پیامی که اموجی سرور در آن استفاده شده ریپلای کنید.' });
      return;
    }

    const replied = await msg.fetchReference().catch(() => null as any);
    if (!replied) {
      await msg.reply({ content: 'پیام مورد نظر پیدا نشد.' });
      return;
    }

    const { customEmoji } = await collectEmojiLikeMediaDeep(replied as Message);
    if (!customEmoji.length) {
      await msg.reply({ content: 'هیچ اموجی سروری در پیام ریپلای‌شده پیدا نشد.' });
      return;
    }

    let deleted = 0;
    let notOwned = 0;
    let failed = 0;

    const seenDel = new Set<string>();
    for (const e of customEmoji) {
      if (seenDel.has(e.id)) continue;
      seenDel.add(e.id);
      const emoji = msg.guild.emojis.cache.get(e.id);
      if (!emoji) {
        notOwned++;
        continue;
      }
      try {
        await emoji.delete(`Deleted via .deleteem by ${msg.author.tag}`);
        deleted++;
      } catch (err) {
        console.error('[DELETEEM ERROR]', err);
        failed++;
      }
    }

    if (!deleted && (notOwned || failed)) {
      await msg.reply({ content: 'هیچ اموجی‌ای از این سرور برای حذف پیدا نشد (امکان دارد اموجی‌ها متعلق به سرورهای دیگر باشند).' });
      return;
    }

    await msg.reply({ content: `✅ ${deleted} اموجی از سرور حذف شد.${notOwned ? `\nℹ️ ${notOwned} اموجی متعلق به این سرور نبود.` : ''}${failed ? `\n❌ ${failed} مورد با خطا مواجه شد.` : ''}` });
    return;
  }

  // .deletest — delete guild sticker from replied message or by numeric ID
  if (isCmd('deletest')) {
    if (!msg.guild) {
      await msg.reply({ content: 'این دستور فقط داخل سرور قابل استفاده است.' });
      return;
    }
    const member = msg.member;
    if (!member || !member.permissions.has(PermissionsBitField.Flags.ManageGuildExpressions)) {
      await msg.reply({ content: 'برای استفاده از این دستور باید دسترسی مدیریت اموجی/استیکر در سرور داشته باشید.' });
      return;
    }

    const parts = content.split(/\s+/);
    const arg = parts.length > 1 ? parts[1] : undefined;
    const numericId = arg && /^\d+$/.test(arg) ? arg : null;

    if (numericId) {
      let sticker = msg.guild.stickers.cache.get(numericId);
      if (!sticker) {
        try {
          sticker = await msg.guild.stickers.fetch(numericId).catch(() => null as any);
        } catch {
          sticker = null as any;
        }
      }
      if (!sticker) {
        await msg.reply({ content: 'هیچ استیکری با این آیدی در این سرور پیدا نشد.' });
        return;
      }
      if (sticker.guildId && sticker.guildId !== msg.guild.id) {
        await msg.reply({ content: 'این استیکر متعلق به این سرور نیست و قابل حذف نیست.' });
        return;
      }
      try {
        await msg.guild.stickers.delete(sticker.id, `Deleted via .deletest by ${msg.author.tag}`);
        await msg.reply({ content: '✅ استیکر مورد نظر از سرور حذف شد.' });
      } catch (err) {
        console.error('[DELETEST ERROR]', err);
        await msg.reply({ content: '❌ حذف استیکر با خطا مواجه شد. ممکن است به‌خاطر محدودیت دسترسی یا محدودیت‌های دیسکورد باشد.' });
      }
      return;
    }

    if (!msg.reference?.messageId) {
      await msg.reply({ content: 'برای استفاده از این دستور باید روی پیامی که استیکر سرور در آن استفاده شده ریپلای کنید.' });
      return;
    }

    const replied = await msg.fetchReference().catch(() => null as any);
    if (!replied) {
      await msg.reply({ content: 'پیام مورد نظر پیدا نشد.' });
      return;
    }

    const sticker = replied.stickers?.first();
    if (!sticker) {
      await msg.reply({ content: 'هیچ استیکری در پیام ریپلای‌شده پیدا نشد.' });
      return;
    }

    if (sticker.guildId && sticker.guildId !== msg.guild.id) {
      await msg.reply({ content: 'این استیکر متعلق به این سرور نیست و قابل حذف نیست.' });
      return;
    }

    try {
      await msg.guild.stickers.delete(sticker.id, `Deleted via .deletest by ${msg.author.tag}`);
      await msg.reply({ content: '✅ استیکر مورد نظر از سرور حذف شد.' });
    } catch (err) {
      console.error('[DELETEST ERROR]', err);
      await msg.reply({ content: '❌ حذف استیکر با خطا مواجه شد. ممکن است به‌خاطر محدودیت دسترسی یا محدودیت‌های دیسکورد باشد.' });
    }
    return;
  }

  // .av [@user|userId]
  if (isCmd('av')) {
    const arg = content.slice(content.startsWith('.عکس') ? 5 : 3).trim();
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

  if (isCmd('ba') || isCmd('بنر')) {
    const arg = content.slice(content.startsWith('.بنر') ? 5 : 3).trim();
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


  // .llset — owner only
  if (isCmd('llset')) {
    // Check if user is bot owner (no message if not)
    if (msg.author.id !== ownerId) {
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

  // .llunset — owner only
  if (isCmd('llunset')) {
    // Check if user is bot owner (no message if not)
    if (msg.author.id !== ownerId) {
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

  // .dmset — owner only, grant DM command permission to a user
  if (isCmd('dmset')) {
    if (msg.author.id !== ownerId) {
      return;
    }
    const arg = content.slice(6).trim();
    let targetUser = msg.mentions.users.first() || null;
    if (!targetUser && arg) {
      // Try to parse user ID
      let id: string | null = null;
      const m = arg.match(/^<@!?(\d+)>$/);
      if (m) id = m[1];
      else if (/^\d+$/.test(arg)) id = arg;
      if (id) {
        try { targetUser = await msg.client.users.fetch(id); } catch {}
      }
    }
    if (!targetUser) {
      await msg.reply({ content: 'استفاده: `.dmset @user` یا `.dmset userId`' });
      return;
    }
    dmAllowedUsersSet.add(targetUser.id);
    saveDMAllowedUsers();
    await msg.reply({ content: `✅ <@${targetUser.id}> اکنون می‌تواند از دستورات .dm و .dmh استفاده کند.` });
    return;
  }

  // .dmunset — owner only, revoke DM command permission from a user
  if (isCmd('dmunset')) {
    if (msg.author.id !== ownerId) {
      return;
    }
    const arg = content.slice(8).trim();
    let targetUser = msg.mentions.users.first() || null;
    if (!targetUser && arg) {
      // Try to parse user ID
      let id: string | null = null;
      const m = arg.match(/^<@!?(\d+)>$/);
      if (m) id = m[1];
      else if (/^\d+$/.test(arg)) id = arg;
      if (id) {
        try { targetUser = await msg.client.users.fetch(id); } catch {}
      }
    }
    if (!targetUser) {
      await msg.reply({ content: 'استفاده: `.dmunset @user` یا `.dmunset userId`' });
      return;
    }
    dmAllowedUsersSet.delete(targetUser.id);
    saveDMAllowedUsers();
    await msg.reply({ content: `✅ دسترسی .dm و .dmh از <@${targetUser.id}> حذف شد.` });
    return;
  }

  // .dm — send DM to user (owner + allowed users)
  if (isCmd('dm')) {
    if (!canUseDMCommands(msg.author.id)) {
      return;
    }
    const arg = content.slice(3).trim();
    let targetUser = msg.mentions.users.first() || null;
    let message = '';
    
    if (targetUser) {
      // Remove mention from message
      message = arg.replace(/<@!?\d+>/g, '').trim();
    } else {
      // Try to parse user ID at the start
      const parts = arg.split(/\s+/);
      if (parts.length >= 2 && /^\d+$/.test(parts[0])) {
        const id = parts[0];
        try {
          targetUser = await msg.client.users.fetch(id);
          message = parts.slice(1).join(' ');
        } catch {}
      }
    }
    
    if (!targetUser || !message) {
      await msg.reply({ content: 'استفاده: `.dm @user پیام` یا `.dm userId پیام`' });
      return;
    }
    
    try {
      await targetUser.send(message);
      await msg.reply({ content: `✅ پیام به <@${targetUser.id}> ارسال شد.` });
    } catch (err) {
      await msg.reply({ content: `دایرکت <@${targetUser.id}> بسته است ❌` });
    }
    return;
  }

  // .dmh — send DM to user with sender mention (owner + allowed users)
  if (isCmd('dmh')) {
    if (!canUseDMCommands(msg.author.id)) {
      return;
    }
    const arg = content.slice(4).trim();
    let targetUser = msg.mentions.users.first() || null;
    let message = '';
    
    if (targetUser) {
      // Remove mention from message
      message = arg.replace(/<@!?\d+>/g, '').trim();
    } else {
      // Try to parse user ID at the start
      const parts = arg.split(/\s+/);
      if (parts.length >= 2 && /^\d+$/.test(parts[0])) {
        const id = parts[0];
        try {
          targetUser = await msg.client.users.fetch(id);
          message = parts.slice(1).join(' ');
        } catch {}
      }
    }
    
    if (!targetUser || !message) {
      await msg.reply({ content: 'استفاده: `.dmh @user پیام` یا `.dmh userId پیام`' });
      return;
    }
    
    try {
      const fullMessage = `<@${msg.author.id}> : ${message}`;
      await targetUser.send(fullMessage);
      await msg.reply({ content: `✅ پیام به <@${targetUser.id}> ارسال شد (با منشن شما).` });
    } catch (err) {
      await msg.reply({ content: `دایرکت <@${targetUser.id}> بسته است ❌` });
    }
    return;
  }

  // .idlist — list user IDs from voice/text channels or server (owner + allowed users)
  if (isCmd('idlist')) {
    if (!canUseDMCommands(msg.author.id)) {
      return;
    }
    
    const arg = content.slice(7).trim();
    if (!arg) {
      await msg.reply({ content: 'استفاده:\n`.idlist channelId` - لیست کاربران فعلی (ویس) یا 24 ساعت گذشته (تکست)\n`.idlist serverId` - لیست تمام اعضای سرور (فایل TXT)\n`.idlist 7d channelId` - فعالیت 7 روز گذشته (فایل TXT)\n`.idlist 45m channelId` - فعالیت 45 دقیقه گذشته (فایل TXT)' });
      return;
    }
    
    // Parse arguments: [duration] targetId
    const parts = arg.split(/\s+/);
    let duration: number | null = null;
    let targetId: string;
    
    if (parts.length === 2) {
      // Try to parse first part as duration
      duration = parseDuration(parts[0]);
      targetId = parts[1];
      if (!duration) {
        await msg.reply({ content: 'فرمت مدت زمان نامعتبر. نمونه: 7d, 45m, 2h' });
        return;
      }
    } else if (parts.length === 1) {
      targetId = parts[0];
    } else {
      await msg.reply({ content: 'استفاده: `.idlist [duration] targetId`' });
      return;
    }
    
    try {
      // Try to fetch as channel first
      let channel: any = null;
      try {
        channel = await msg.client.channels.fetch(targetId);
      } catch {}
      
      if (channel) {
        // It's a channel
        if (channel.isVoiceBased()) {
          // Voice channel
          if (duration) {
            // Time-filtered voice activity
            const now = Date.now();
            const cutoff = now - duration;
            const voiceLog = voiceActivityLog.get(channel.guildId)?.get(targetId) || [];
            const recentUsers = new Set<string>();
            for (const entry of voiceLog) {
              if (entry.timestamp >= cutoff) {
                recentUsers.add(entry.userId);
              }
            }
            const userIds = Array.from(recentUsers).join(' ');
            const buffer = Buffer.from(userIds, 'utf8');
            const attachment = new AttachmentBuilder(buffer, { name: 'user_ids.txt' });
            await msg.reply({ content: `لیست یوزر آیدی‌های فعال در ${parts[0]} گذشته:`, files: [attachment] });
          } else {
            // Current voice members
            const voiceChannel = channel as any;
            const members = voiceChannel.members?.map((m: any) => m.id) || [];
            if (members.length === 0) {
              await msg.reply({ content: 'هیچ کاربری در این چنل ویس نیست.' });
            } else {
              const userIds = members.join(' ');
              await msg.reply({ content: `\`\`\`${userIds}\`\`\`` });
            }
          }
        } else if (channel.isTextBased()) {
          // Text channel
          const timeFilter = duration || (24 * 60 * 60 * 1000); // Default 24h
          const now = Date.now();
          const cutoff = now - timeFilter;
          const textLog = textActivityLog.get(channel.guildId)?.get(targetId) || [];
          const recentUsers = new Set<string>();
          for (const entry of textLog) {
            if (entry.timestamp >= cutoff) {
              recentUsers.add(entry.userId);
            }
          }
          const userIds = Array.from(recentUsers).join(' ');
          
          if (duration) {
            // Send as file if duration was specified
            const buffer = Buffer.from(userIds, 'utf8');
            const attachment = new AttachmentBuilder(buffer, { name: 'user_ids.txt' });
            await msg.reply({ content: `لیست یوزر آیدی‌های فعال در ${parts[0]} گذشته:`, files: [attachment] });
          } else {
            // Send inline for default 24h
            if (userIds.length === 0) {
              await msg.reply({ content: 'هیچ فعالیتی در 24 ساعت گذشته یافت نشد.' });
            } else {
              await msg.reply({ content: `\`\`\`${userIds}\`\`\`` });
            }
          }
        } else {
          await msg.reply({ content: 'این نوع چنل پشتیبانی نمی‌شود.' });
        }
      } else {
        // Try to fetch as guild
        let guild: any = null;
        try {
          guild = await msg.client.guilds.fetch(targetId);
        } catch {}
        
        if (guild) {
          // It's a server/guild
          if (duration) {
            // Time-filtered server activity (voice + text)
            const now = Date.now();
            const cutoff = now - duration;
            const activeUsers = new Set<string>();
            
            // Check voice activity across all channels
            const voiceLog = voiceActivityLog.get(targetId);
            if (voiceLog) {
              for (const channelLog of voiceLog.values()) {
                for (const entry of channelLog) {
                  if (entry.timestamp >= cutoff) {
                    activeUsers.add(entry.userId);
                  }
                }
              }
            }
            
            // Check text activity across all channels
            const textLog = textActivityLog.get(targetId);
            if (textLog) {
              for (const channelLog of textLog.values()) {
                for (const entry of channelLog) {
                  if (entry.timestamp >= cutoff) {
                    activeUsers.add(entry.userId);
                  }
                }
              }
            }
            
            const userIds = Array.from(activeUsers).join(' ');
            const buffer = Buffer.from(userIds, 'utf8');
            const attachment = new AttachmentBuilder(buffer, { name: 'user_ids.txt' });
            await msg.reply({ content: `لیست یوزر آیدی‌های فعال در ${parts[0]} گذشته:`, files: [attachment] });
          } else {
            // All server members
            const statusMsg = await msg.reply({ content: '⏳ در حال دریافت لیست اعضای سرور...' });
            try {
              // Fetch with timeout (30 seconds max for large servers)
              await Promise.race([
                guild.members.fetch(),
                new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 30000))
              ]);
              const allMembers = guild.members.cache.map((m: any) => m.id);
              const userIds = allMembers.join(' ');
              const buffer = Buffer.from(userIds, 'utf8');
              const attachment = new AttachmentBuilder(buffer, { name: 'user_ids.txt' });
              await statusMsg.edit({ content: `✅ لیست تمام اعضای سرور (${allMembers.length} نفر):`, files: [attachment] });
            } catch (fetchErr) {
              // Fallback to cached members if fetch times out
              const cachedMembers = guild.members.cache.map((m: any) => m.id);
              if (cachedMembers.length > 0) {
                const userIds = cachedMembers.join(' ');
                const buffer = Buffer.from(userIds, 'utf8');
                const attachment = new AttachmentBuilder(buffer, { name: 'user_ids.txt' });
                await statusMsg.edit({ content: `⚠️ لیست اعضای cache شده (${cachedMembers.length} نفر - ممکن است ناقص باشد):`, files: [attachment] });
              } else {
                await statusMsg.edit({ content: '❌ خطا در دریافت لیست اعضا. سرور خیلی بزرگ است یا timeout رخ داد.' });
              }
            }
          }
        } else {
          await msg.reply({ content: 'چنل یا سرور یافت نشد. ID را بررسی کنید.' });
        }
      }
    } catch (err) {
      console.error('[IDLIST ERROR]:', err);
      await msg.reply({ content: '❌ خطا در پردازش درخواست.' });
    }
    return;
  }

  // .cancel — cancel ongoing .dmall workflow (owner + allowed users)
  if (isCmd('cancel')) {
    if (!canUseDMCommands(msg.author.id)) {
      return;
    }
    const state = dmallStates.get(msg.author.id);
    if (state) {
      dmallStates.delete(msg.author.id);
      await msg.reply({ content: '✅ عملیات .dmall لغو شد.' });
    } else {
      await msg.reply({ content: 'هیچ عملیات فعالی برای لغو وجود ندارد.' });
    }
    return;
  }

  // .dmhelp — show DM commands help with beautiful embed
  if (isCmd('dmhelp')) {
    if (!canUseDMCommands(msg.author.id)) {
      return;
    }
    
    const isOwner = msg.author.id === ownerId;
    
    const helpEmbed = new EmbedBuilder()
      .setColor('#5865F2')
      .setTitle('📬 راهنمای دستورات DM')
      .setDescription('سیستم کامل مدیریت پیام‌های خصوصی و لیست کاربران')
      .setTimestamp();
    
    // Single DM Commands
    helpEmbed.addFields({
      name: '💬 ارسال DM به یک کاربر',
      value: '**`.dm @user پیام`** یا **`.dm userID پیام`**\n' +
             '→ ارسال پیام به دایرکت کاربر بدون نمایش فرستنده\n' +
             '📝 مثال: `.dm @کاربر سلام چطوری؟`',
      inline: false
    });
    
    helpEmbed.addFields({
      name: '👤 ارسال DM با منشن فرستنده',
      value: '**`.dmh @user پیام`** یا **`.dmh userID پیام`**\n' +
             '→ ارسال پیام به دایرکت با نمایش فرستنده\n' +
             '📝 مثال: `.dmh @کاربر به این موضوع توجه کن`\n' +
             '📤 در DM کاربر: `@شما : به این موضوع توجه کن`',
      inline: false
    });
    
    // ID List Commands
    helpEmbed.addFields({
      name: '📋 لیست یوزر آیدی‌ها',
      value: '**`.idlist channelID`** - لیست کاربران فعلی (ویس) یا 24 ساعت گذشته (تکست)\n' +
             '**`.idlist serverID`** - لیست تمام اعضای سرور (فایل TXT)\n' +
             '**`.idlist 7d channelID`** - فعالیت 7 روز گذشته (فایل TXT)\n' +
             '**`.idlist 45m channelID`** - فعالیت 45 دقیقه گذشته (فایل TXT)\n' +
             '📝 فرمت زمانی: `7d`, `24h`, `45m`, `30s`',
      inline: false
    });
    
    // Broadcast Command
    helpEmbed.addFields({
      name: '📢 ارسال گروهی DM',
      value: '**`.dmall`** - ارسال پیام به چندین کاربر همزمان\n' +
             '**مراحل:**\n' +
             '1️⃣ `.dmall` را بزنید\n' +
             '2️⃣ پیام مورد نظر را ارسال کنید\n' +
             '3️⃣ لیست یوزر آیدی‌ها یا منشن‌ها را بفرستید (یا فایل TXT آپلود کنید)\n' +
             '4️⃣ بات شروع به ارسال می‌کند (با تاخیر 1.5 ثانیه)\n' +
             '✅ گزارش نهایی: آمار موفق/ناموفق + لیست کاربران با DM بسته',
      inline: false
    });
    
    // Utility Commands
    helpEmbed.addFields({
      name: '🛠️ دستورات کمکی',
      value: '**`.cancel`** - لغو عملیات `.dmall` در حین انجام\n' +
             '**`.dmstatus`** - نمایش وضعیت دسترسی و محدودیت‌های Discord\n' +
             '**`.dmhelp`** - نمایش این راهنما',
      inline: false
    });
    
    // Owner-only commands
    if (isOwner) {
      helpEmbed.addFields({
        name: '👑 دستورات مالک بات',
        value: '**`.dmset @user`** یا **`.dmset userID`**\n' +
               '→ دادن دسترسی به تمام دستورات DM به کاربر\n\n' +
               '**`.dmunset @user`** یا **`.dmunset userID`**\n' +
               '→ گرفتن دسترسی از کاربر\n\n' +
               `📊 تعداد کاربران با دسترسی: **${dmAllowedUsersSet.size} نفر**`,
        inline: false
      });
    }
    
    // Rate Limits & Safety
    helpEmbed.addFields({
      name: '⚙️ محدودیت‌ها و ایمنی',
      value: '🔹 Rate Limit: 1 DM هر 1.5 ثانیه\n' +
             '🔹 حداکثر طول پیام: 2000 کاراکتر\n' +
             '🔹 حداکثر منشن در پیام: ~65 نفر\n' +
             '🔹 Timeout سرور بزرگ: 30 ثانیه\n' +
             '✅ بات به صورت خودکار از مسدود شدن جلوگیری می‌کند',
      inline: false
    });
    
    helpEmbed.setFooter({ 
      text: `درخواست شده توسط ${msg.author.tag}`,
      iconURL: msg.author.displayAvatarURL()
    });
    
    await msg.reply({ embeds: [helpEmbed] });
    return;
  }

  // .dmstatus — show DM permissions and rate limit info
  if (isCmd('dmstatus')) {
    if (!canUseDMCommands(msg.author.id)) {
      return;
    }
    
    const hasAccess = canUseDMCommands(msg.author.id);
    const isOwner = msg.author.id === ownerId;
    
    let statusMsg = `**📊 وضعیت دسترسی DM**\n\n`;
    
    if (isOwner) {
      statusMsg += `✅ شما مالک بات هستید - دسترسی کامل\n\n`;
      statusMsg += `**کاربران با دسترسی:** ${dmAllowedUsersSet.size} نفر\n`;
      if (dmAllowedUsersSet.size > 0) {
        const users = Array.from(dmAllowedUsersSet).slice(0, 10);
        statusMsg += users.map(id => `• <@${id}>`).join('\n');
        if (dmAllowedUsersSet.size > 10) {
          statusMsg += `\n... و ${dmAllowedUsersSet.size - 10} نفر دیگر`;
        }
      }
    } else if (hasAccess) {
      statusMsg += `✅ شما دسترسی به دستورات DM دارید\n`;
    } else {
      statusMsg += `❌ شما دسترسی به دستورات DM ندارید\n`;
      statusMsg += `برای دریافت دسترسی، مالک بات باید از دستور \`.dmset @${msg.author.tag}\` استفاده کند.\n`;
    }
    
    statusMsg += `\n**📝 دستورات قابل دسترسی:**\n`;
    statusMsg += hasAccess ? '✅ `.dm` - ارسال DM به یک کاربر\n' : '❌ `.dm`\n';
    statusMsg += hasAccess ? '✅ `.dmh` - ارسال DM با منشن فرستنده\n' : '❌ `.dmh`\n';
    statusMsg += hasAccess ? '✅ `.idlist` - لیست یوزر آیدی‌ها\n' : '❌ `.idlist`\n';
    statusMsg += hasAccess ? '✅ `.dmall` - ارسال گروهی DM\n' : '❌ `.dmall`\n';
    statusMsg += hasAccess ? '✅ `.cancel` - لغو عملیات .dmall\n' : '❌ `.cancel`\n';
    statusMsg += hasAccess ? '✅ `.dmstatus` - نمایش وضعیت\n' : '❌ `.dmstatus`\n';
    statusMsg += hasAccess ? '✅ `.dmhelp` - راهنمای کامل\n' : '❌ `.dmhelp`\n';
    
    statusMsg += `\n**⚙️ محدودیت‌های Discord:**\n`;
    statusMsg += `• Rate Limit DM: ~1 پیام/ثانیه (بات از 1.5 ثانیه استفاده می‌کند)\n`;
    statusMsg += `• حداکثر طول پیام: 2000 کاراکتر\n`;
    statusMsg += `• حداکثر منشن در پیام: ~65 نفر (بات خودکار پیام‌های اضافی می‌فرستد)\n`;
    statusMsg += `• Timeout سرور بزرگ: 30 ثانیه (با fallback به cache)\n`;
    
    await msg.reply({ content: statusMsg, allowedMentions: { parse: [] } });
    return;
  }

  // .dmall — broadcast message to multiple users via DM (owner + allowed users)
  if (isCmd('dmall')) {
    if (!canUseDMCommands(msg.author.id)) {
      return;
    }
    
    // Start the workflow
    dmallStates.set(msg.author.id, {
      step: 'awaiting_message',
      timestamp: Date.now()
    });
    
    await msg.reply({ content: '📝 لطفاً پیام مورد نظر برای ارسال به دایرکت کاربران را ارسال کنید.\n\n(برای لغو از دستور `.cancel` استفاده کنید)' });
    return;
  }

  const isFootballEnDot = isCmd('football');
  const isFootballFaDot = isCmd('فوتبال');
  const isFootballEnDollar = /^\$football(?:\s|$)/.test(content);
  const isFootballFaDollar = /^\$فوتبال(?:\s|$)/.test(content);

  // .football / .فوتبال / $football / $فوتبال command
  if (isFootballEnDot || isFootballFaDot || isFootballEnDollar || isFootballFaDollar) {
    if (footballInFlight.has(msg.id)) return;
    footballInFlight.add(msg.id);
    try {
      if (!canvasAvailable || !createCanvas || !loadImage) {
        await msg.reply({ content: 'امکان ساخت تصویر فوتبال در این سرور فعال نیست.' });
        return;
      }

      const cmdLen = isFootballEnDot || isFootballEnDollar
        ? content.indexOf('football') + 'football'.length
        : content.indexOf('فوتبال') + 'فوتبال'.length;
      const rawQuery = content.slice(cmdLen).trim();
      if (!rawQuery) {
        let reply = 'لیگ مورد نظرت رو از لیست زیر انتخاب کن و بعد اسم تیم رو با دستور `.فوتبال <نام تیم>` بنویس:\n\n';
        reply += FOOTBALL_POPULAR_LEAGUES.map(l => `• ${l.flag} ${l.title}`).join('\n');
        reply += '\n\nمثال: `.فوتبال Liverpool` یا `.football Real Madrid`';
        await msg.reply({ content: reply });
        return;
      }

      const statusMsg = await msg.reply({ content: '⏳ در حال دریافت اطلاعات بازی...' });
      const team = await findFootballTeamByQuery(rawQuery);
      if (!team) {
        await statusMsg.edit({ content: '❌ تیم پیدا نشد. اسم تیم را دقیق‌تر بنویس.' });
        return;
      }
      
      console.log(`[FOOTBALL] Found team: ${team.name} (ID: ${team.id}) for query "${rawQuery}"`);

      const match = await getMatchDataForTeam(team);
      if (!match) {
        console.log(`[FOOTBALL] No match found for team ${team.name} (${team.id})`);
        await statusMsg.edit({ content: `❌ برای تیم **${team.name}** بازی بعدی پیدا نشد.` });
        return;
      }
      
      console.log(`[FOOTBALL] Match found for ${team.name}: ${match.event.strEvent}`);

      const buffer = await renderFootballMatchImage(team, match);
      const attachment = new AttachmentBuilder(buffer, { name: 'football.png' });
      const summary = footballFormatMatchSummary(team, match);
      await statusMsg.edit({ content: summary, files: [attachment] });
      return;
    } catch (err) {
      console.error('Error in .football command:', err);
      await msg.reply({ content: '❌ خطا در دریافت/ساخت گزارش فوتبال. لطفاً کمی بعد دوباره تلاش کنید.' });
      return;
    } finally {
      footballInFlight.delete(msg.id);
    }
  }

  // .ll command
  if (isCmd('ll')) {
    if (llInFlight.has(msg.id)) return;
    llInFlight.add(msg.id);
    try {
      if (!canvasAvailable || !createCanvas || !loadImage) {
        await msg.reply({ content: 'امکان ساخت تصویر عشق در این سرور فعال نیست، ولی می‌تونی از دستور به‌صورت متنی استفاده کنی.' });
        return;
      }
      const arg = content.slice(3).trim();
      let userA = msg.author;
      let userB = msg.mentions.users.first() || null;
      
      // Check if two users are mentioned: .ll @user1 @user2
      if (msg.mentions.users.size >= 2) {
        const mentioned = Array.from(msg.mentions.users.values());
        userA = mentioned[0];
        userB = mentioned[1];
      } else if (!userB && arg) {
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
      
      // Send names as text above image
      const replyText = `**${aName}** & **${bName}**`;
      await msg.reply({ content: replyText, files: [attachment] });
      return;
    } catch (err) {
      console.error('Error in .ll command:', err);
      await msg.reply({ content: 'خطا در ساخت تصویر عشق. لطفاً کمی بعد دوباره تلاش کنید.' });
      return;
    } finally {
      llInFlight.delete(msg.id);
    }
  }

  // .servers — list all servers with invite links (owner only)
  if (isCmd('servers')) {
    // Check if user is bot owner (no message if not)
    if (msg.author.id !== ownerId) {
      return;
    }
    
    const guilds = msg.client.guilds.cache;
    if (guilds.size === 0) {
      await msg.reply({ content: 'بات در هیچ سروری عضو نیست.' });
      return;
    }
    
    let serverList = `**لیست سرورها (${guilds.size}):**\n\n`;
    
    for (const [guildId, guild] of guilds) {
      let inviteLink = 'لینک موجود نیست';
      
      try {
        // Try to get vanity URL first
        if (guild.vanityURLCode) {
          inviteLink = `https://discord.gg/${guild.vanityURLCode}`;
        } else {
          // Try to create an invite from a text channel
          const channel = guild.channels.cache.find(ch => 
            ch.isTextBased() && 
            ch.permissionsFor(guild.members.me!)?.has('CreateInstantInvite')
          );
          
          if (channel && 'createInvite' in channel) {
            const invite = await channel.createInvite({ 
              maxAge: 0, // never expires
              maxUses: 0, // unlimited uses
              reason: 'Server list for bot owner'
            });
            inviteLink = invite.url;
          }
        }
      } catch (err) {
        // If we can't create an invite, keep the default message
      }
      
      serverList += `**${guild.name}** (ID: ${guildId})\n`;
      serverList += `└ اعضا: ${guild.memberCount}\n`;
      serverList += `└ لینک: ${inviteLink}\n\n`;
    }
    
    // Split message if too long (Discord limit is 2000 characters)
    if (serverList.length > 2000) {
      const chunks: string[] = [];
      let currentChunk = `**لیست سرورها (${guilds.size}):**\n\n`;
      
      for (const [guildId, guild] of guilds) {
        let inviteLink = 'لینک موجود نیست';
        
        try {
          if (guild.vanityURLCode) {
            inviteLink = `https://discord.gg/${guild.vanityURLCode}`;
          } else {
            const channel = guild.channels.cache.find(ch => 
              ch.isTextBased() && 
              ch.permissionsFor(guild.members.me!)?.has('CreateInstantInvite')
            );
            
            if (channel && 'createInvite' in channel) {
              const invite = await channel.createInvite({ 
                maxAge: 0,
                maxUses: 0,
                reason: 'Server list for bot owner'
              });
              inviteLink = invite.url;
            }
          }
        } catch (err) {
          // Keep default
        }
        
        const serverInfo = `**${guild.name}** (ID: ${guildId})\n└ اعضا: ${guild.memberCount}\n└ لینک: ${inviteLink}\n\n`;
        
        if ((currentChunk + serverInfo).length > 2000) {
          chunks.push(currentChunk);
          currentChunk = serverInfo;
        } else {
          currentChunk += serverInfo;
        }
      }
      
      if (currentChunk) chunks.push(currentChunk);
      
      // Send all chunks
      for (const chunk of chunks) {
        await msg.reply({ content: chunk });
      }
    } else {
      await msg.reply({ content: serverList });
    }
    
    return;
  }

  // .leave <serverId> — leave a server (owner only)
  if (isCmd('leave')) {
    // Check if user is bot owner (no message if not)
    if (msg.author.id !== ownerId) {
      return;
    }
    
    const arg = content.slice(6).trim();
    if (!arg) {
      await msg.reply({ content: 'استفاده: `.leave <serverId>`\nبرای دیدن لیست سرورها از `.servers` استفاده کنید.' });
      return;
    }
    
    const serverId = arg.split(/\s+/)[0];
    
    try {
      const guild = msg.client.guilds.cache.get(serverId);
      
      if (!guild) {
        await msg.reply({ content: `❌ سروری با ID \`${serverId}\` پیدا نشد.\nبرای دیدن لیست سرورها از \`.servers\` استفاده کنید.` });
        return;
      }
      
      const guildName = guild.name;
      const memberCount = guild.memberCount;
      
      // Leave the guild
      await guild.leave();
      
      await msg.reply({ content: `✅ بات با موفقیت از سرور **${guildName}** (ID: \`${serverId}\`, ${memberCount} عضو) خارج شد.` });
    } catch (error) {
      console.error('[LEAVE ERROR]', error);
      await msg.reply({ content: `❌ خطا در خروج از سرور: ${error instanceof Error ? error.message : 'خطای نامشخص'}` });
    }
    
    return;
  }

  // .send <channelId> <message> — send message to channel (owner only)
  if (isCmd('send')) {
    if (msg.author.id !== ownerId) {
      return;
    }
    
    const arg = content.slice(5).trim();
    const parts = arg.split(' ');
    
    if (parts.length < 2) {
      await msg.reply({ content: 'استفاده: `.send <channelId> <پیام>`\nمثال: `.send 123456789 سلام!`' });
      return;
    }
    
    const channelId = parts[0];
    const messageContent = parts.slice(1).join(' ');
    
    try {
      const channel = await msg.client.channels.fetch(channelId);
      if (channel && channel.isTextBased() && 'send' in channel) {
        await channel.send({ content: messageContent });
        await msg.reply({ content: `✅ پیام به کانال <#${channelId}> ارسال شد.` });
      } else {
        await msg.reply({ content: '❌ کانال متنی یافت نشد یا قابل دسترسی نیست.' });
      }
    } catch (err) {
      await msg.reply({ content: `❌ خطا در ارسال پیام: ${err}` });
    }
    
    return;
  }

  // .reply <channelId> <messageId> <message> — reply to message (owner only)
  if (isCmd('reply')) {
    if (msg.author.id !== ownerId) {
      return;
    }
    
    const arg = content.slice(6).trim();
    const parts = arg.split(' ');
    
    if (parts.length < 3) {
      await msg.reply({ content: 'استفاده: `.reply <channelId> <messageId> <پیام>`\nمثال: `.reply 123456789 987654321 سلام!`' });
      return;
    }
    
    let channelId = parts[0];
    let messageId = parts[1];
    const messageContent = parts.slice(2).join(' ');
    
    // Try to smart detect which is channel and which is message
    let success = false;
    
    // Try first order: channelId, messageId
    try {
      const channel = await msg.client.channels.fetch(channelId);
      if (channel && channel.isTextBased() && 'messages' in channel) {
        const targetMsg = await channel.messages.fetch(messageId);
        await targetMsg.reply({ content: messageContent });
        await msg.reply({ content: `✅ ریپلای به پیام در کانال <#${channelId}> ارسال شد.` });
        success = true;
      }
    } catch (err) {
      // First order failed, try swapped order
    }
    
    // Try swapped order: messageId, channelId
    if (!success) {
      try {
        // Swap the IDs
        const temp = channelId;
        channelId = messageId;
        messageId = temp;
        
        const channel = await msg.client.channels.fetch(channelId);
        if (channel && channel.isTextBased() && 'messages' in channel) {
          const targetMsg = await channel.messages.fetch(messageId);
          await targetMsg.reply({ content: messageContent });
          await msg.reply({ content: `✅ ریپلای به پیام در کانال <#${channelId}> ارسال شد. (ترتیب ID ها اصلاح شد)` });
          success = true;
        }
      } catch (err) {
        // Both orders failed
      }
    }
    
    if (!success) {
      await msg.reply({ content: '❌ خطا: کانال یا پیام یافت نشد. لطفاً ID ها را بررسی کنید.' });
    }
    
    return;
  }

  // .payam <duration> <serverId> — list messages in a server within a timeframe (owner only)
  if (isCmd('payam')) {
    if (msg.author.id !== ownerId) {
      return;
    }
    
    const arg = content.slice(6).trim();
    if (!arg) {
      await msg.reply({ content: 'استفاده: `.payam <duration> <serverId>` مثلا: `.payam 48h 1374041823793774662`' });
      return;
    }
    
    // Parse arguments: duration serverId
    const parts = arg.split(/\s+/);
    if (parts.length !== 2) {
      await msg.reply({ content: 'استفاده: `.payam <duration> <serverId>` مثلا: `.payam 48h 1374041823793774662`' });
      return;
    }
    
    const durationStr = parts[0];
    const serverId = parts[1];
    
    // Parse duration
    const duration = parseDuration(durationStr);
    if (!duration || duration < 1000) {
      await msg.reply({ content: 'فرمت مدت زمان نامعتبر. نمونه: 48h, 7d, 120m, 3600s' });
      return;
    }
    
    try {
      // Try to fetch the guild
      let guild: any = null;
      try {
        guild = await msg.client.guilds.fetch(serverId);
      } catch (err) {
        await msg.reply({ content: `❌ سرور با آیدی ${serverId} یافت نشد یا بات در آن حضور ندارد.` });
        return;
      }
      
      const statusMsg = await msg.reply({ content: `⏳ در حال جمع‌آوری پیام‌های سرور **${guild.name}** در ${durationStr} گذشته...` });
      
      // Calculate cutoff time
      const now = Date.now();
      const cutoff = now - duration;
      
      // Store messages by channel and user
      const messagesByChannel: Map<string, Map<string, string[]>> = new Map();
      let totalMessages = 0;
      
      // Fetch all text channels
      const textChannels = guild.channels.cache.filter((ch: any) => ch.isTextBased() && !ch.isDMBased());
      
      // Create a counter to track completed channel fetches
      let completedChannels = 0;
      const totalChannels = textChannels.size;
      let limitHitChannels = 0;
      
      // Process channels in sequence to avoid rate limits
      for (const [channelId, channel] of textChannels) {
        try {
          // Try to get messages from this channel within the time period
          const messages = await channel.messages.fetch({ limit: 100 });
          const filteredMessages = messages.filter((m: any) => m.createdTimestamp >= cutoff && !m.author.bot);
          
          if (filteredMessages.size > 0) {
            // Group messages by user
            const userMessages = new Map<string, string[]>();
            
            filteredMessages.forEach((m: any) => {
              const userId = m.author.id;
              const username = m.author.username;
              const content = m.content || "[محتوای خالی یا غیرمتنی]";
              const timestamp = new Date(m.createdTimestamp).toLocaleString('fa-IR');
              const messageInfo = `${timestamp}: ${content}`;
              
              if (!userMessages.has(userId)) {
                userMessages.set(userId, [`👤 **${username}** (ID: ${userId}):`]);
              }
              
              userMessages.get(userId)?.push(`- ${messageInfo}`);
              totalMessages++;
            });
            
            // Add to channel map
            messagesByChannel.set(channelId, userMessages);
          }
          
          completedChannels++;
          
          // Update status message every 5 channels or at 25%, 50%, 75% progress points
          if (completedChannels % 5 === 0 || completedChannels / totalChannels >= 0.25 || 
              completedChannels / totalChannels >= 0.5 || completedChannels / totalChannels >= 0.75) {
            await statusMsg.edit({ content: `⏳ در حال جمع‌آوری پیام‌های سرور **${guild.name}** در ${durationStr} گذشته...
${completedChannels}/${totalChannels} کانال بررسی شد (${totalMessages} پیام یافت شد)` });
          }
          
          // Add a small delay to avoid rate limits
          await new Promise(resolve => setTimeout(resolve, 300));
          
        } catch (channelErr) {
          console.error(`[PAYAM ERROR] Could not fetch messages from channel ${channelId}:`, channelErr);
          limitHitChannels++;
        }
      }
      
      // If no messages found
      if (totalMessages === 0) {
        await statusMsg.edit({ content: `❌ هیچ پیامی در سرور **${guild.name}** در ${durationStr} گذشته یافت نشد.` });
        return;
      }
      
      // Generate the report
      let report = `# گزارش پیام‌های سرور ${guild.name} در ${durationStr} گذشته\n\n`;
      report += `- تعداد کل پیام‌ها: ${totalMessages}\n`;
      report += `- تعداد کانال‌های بررسی شده: ${completedChannels}/${totalChannels}\n`;
      if (limitHitChannels > 0) {
        report += `- تعداد کانال‌هایی که با خطای دسترسی مواجه شدند: ${limitHitChannels}\n`;
      }
      report += `\n`;
      
      // Add channel and user details
      for (const [channelId, users] of messagesByChannel) {
        const channel = guild.channels.cache.get(channelId);
        const channelName = channel ? channel.name : 'کانال ناشناخته';
        
        report += `## #${channelName} (ID: ${channelId})\n\n`;
        
        for (const [, messages] of users) {
          report += messages.join('\n') + '\n\n';
        }
      }
      
      // Send report as a file
      if (report.length > 1950) {
        const buffer = Buffer.from(report, 'utf8');
        const attachment = new AttachmentBuilder(buffer, { name: `messages_${guild.name}_${durationStr}.txt` });
        await statusMsg.edit({
          content: `✅ گزارش پیام‌های سرور **${guild.name}** در ${durationStr} گذشته (${totalMessages} پیام)`,
          files: [attachment]
        });
      } else {
        await statusMsg.edit({ content: `✅ گزارش پیام‌های سرور **${guild.name}** در ${durationStr} گذشته:\n\n${report}` });
      }
      
    } catch (err) {
      console.error('[PAYAM ERROR]:', err);
      await msg.reply({ content: '❌ خطا در پردازش درخواست.' });
    }
    return;
  }
  
  // .edit <channelId> <messageId> <newMessage> — edit bot message (owner only)
  if (isCmd('edit')) {
    if (msg.author.id !== ownerId) {
      return;
    }
    
    const arg = content.slice(5).trim();
    const parts = arg.split(' ');
    
    if (parts.length < 3) {
      await msg.reply({ content: 'استفاده: `.edit <channelId> <messageId> <پیام جدید>`\nمثال: `.edit 123456789 987654321 سلام جدید!`' });
      return;
    }
    
    let channelId = parts[0];
    let messageId = parts[1];
    const newContent = parts.slice(2).join(' ');
    
    // Try to smart detect which is channel and which is message
    let success = false;
    
    // Try first order: channelId, messageId
    try {
      const channel = await msg.client.channels.fetch(channelId);
      if (channel && channel.isTextBased() && 'messages' in channel) {
        const targetMsg = await channel.messages.fetch(messageId);
        
        // Check if message is from the bot
        if (targetMsg.author.id !== msg.client.user?.id) {
          await msg.reply({ content: '❌ فقط می‌توانید پیام‌های بات را ویرایش کنید.' });
          return;
        }
        
        await targetMsg.edit({ content: newContent });
        await msg.reply({ content: `✅ پیام در کانال <#${channelId}> ویرایش شد.` });
        success = true;
      }
    } catch (err) {
      // First order failed, try swapped order
    }
    
    // Try swapped order: messageId, channelId
    if (!success) {
      try {
        // Swap the IDs
        const temp = channelId;
        channelId = messageId;
        messageId = temp;
        
        const channel = await msg.client.channels.fetch(channelId);
        if (channel && channel.isTextBased() && 'messages' in channel) {
          const targetMsg = await channel.messages.fetch(messageId);
          
          // Check if message is from the bot
          if (targetMsg.author.id !== msg.client.user?.id) {
            await msg.reply({ content: '❌ فقط می‌توانید پیام‌های بات را ویرایش کنید.' });
            return;
          }
          
          await targetMsg.edit({ content: newContent });
          await msg.reply({ content: `✅ پیام در کانال <#${channelId}> ویرایش شد. (ترتیب ID ها اصلاح شد)` });
          success = true;
        }
      } catch (err) {
        // Both orders failed
      }
    }
    
    if (!success) {
      await msg.reply({ content: '❌ خطا: کانال یا پیام یافت نشد. لطفاً ID ها را بررسی کنید.' });
    }
    
    return;
  }

  // .prefix <newPrefix> — change timer prefix (owner only, global)
  if (isCmd('prefix')) {
    // Check if user is bot owner (no message if not)
    if (msg.author.id !== ownerId) {
      return;
    }
    const arg = content.slice(7).trim();
    if (!arg || arg.length !== 1) {
      const current = getTimerPrefix();
      await msg.reply({ content: `پرفیکس فعلی دستور زمان (در همه سرورها): \`${current}\`\nبرای تغییر: \`.prefix <کاراکتر>\`\nمثال: \`.prefix ?\` تا دستور ${current}t به ?t تبدیل شود` });
      return;
    }
    // Validate prefix character (prevent conflicts with common characters)
    if (arg === ' ' || /[a-zA-Z0-9@#]/.test(arg)) {
      await msg.reply({ content: 'پرفیکس نمی‌تواند فاصله یا حرف/عدد باشد. از نمادهایی مثل . ! ? $ استفاده کنید.' });
      return;
    }
    globalTimerPrefix = arg;
    saveTimerPrefix();
    await msg.reply({ content: `✅ پرفیکس دستور زمان در همه سرورها تغییر کرد!\nاز این به بعد: \`${arg}t <مدت>\`\nمثال: \`${arg}t 10m\`` });
    return;
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

  // .esm — convert text to small caps or change user nickname
  if (isCmd('esm')) {
    if (!msg.guild) {
      await msg.reply({ content: 'این دستور فقط در سرور کار می‌کند.' });
      return;
    }

    const arg = content.slice(4).trim(); // Remove '.esm'
    
    // Check if there's a mention or reply
    let targetMember: GuildMember | null = null;
    let textToConvert = arg;

    // Priority 1: Check for mention
    if (msg.mentions.members && msg.mentions.members.size > 0) {
      targetMember = msg.mentions.members.first()!;
      // Remove the mention from the text
      textToConvert = arg.replace(/<@!?\d+>/g, '').trim();
    }
    // Priority 2: Check for reply
    else if (msg.reference) {
      try {
        const repliedMsg = await msg.channel.messages.fetch(msg.reference.messageId!);
        if (repliedMsg.member) {
          targetMember = repliedMsg.member;
          textToConvert = arg; // Use all text after .esm
        }
      } catch {}
    }

    // If no text provided, show usage
    if (!textToConvert) {
      await msg.reply({ content: 'استفاده:\n`.esm @user نام جدید` - تغییر نیک نیم\n`.esm متن` - تبدیل متن به فونت Small Caps' });
      return;
    }

    // Convert text to small caps
    const convertedText = toSmallCaps(textToConvert);

    // If target member exists, change their nickname
    if (targetMember) {
      try {
        await targetMember.setNickname(convertedText);
        await msg.reply({ content: `✅ نیک نیم <@${targetMember.id}> تغییر کرد به: ${convertedText}` });
      } catch (err) {
        await msg.reply({ content: `❌ خطا در تغییر نیک نیم. ممکن است مجوز کافی وجود نداشته باشد یا کاربر دسترسی بالاتری داشته باشد.` });
      }
    } else {
      // Just send the converted text
      await msg.reply({ content: convertedText });
    }
    return;
  }

  // .sort — randomize names or pair two groups
  if (isCmd('sort') || isCmd('sortpv') || isCmd('رندوم')) {
    const isDM = isCmd('sortpv');
    
    // Remove command from content properly
    const cmdLength = isDM ? 7 : content.startsWith('.رندوم') ? 6 : 5; // '.sortpv' or '.sort' or '.رندوم'
    const restContent = content.slice(cmdLength).trimStart();
    const lines = restContent.split('\n').map(l => l.trim()).filter(l => l.length > 0);
    
    // Helper function to parse items with ! grouping
    const parseItems = (line: string): string[] => {
      const items: string[] = [];
      let current = '';
      let inGroup = false;
      const tokens = line.split(/\s+/);
      
      for (const token of tokens) {
        if (token.startsWith('!') && token.endsWith('!') && token.length > 2) {
          // Single token wrapped in ! like !word!
          items.push(token.slice(1, -1));
        } else if (token.startsWith('!')) {
          // Start of multi-word group
          inGroup = true;
          current = token.slice(1); // Remove leading !
        } else if (token.endsWith('!') && inGroup) {
          // End of multi-word group
          current += ' ' + token.slice(0, -1); // Remove trailing !
          items.push(current);
          current = '';
          inGroup = false;
        } else if (inGroup) {
          // Middle of multi-word group
          current += ' ' + token;
        } else {
          // Regular token
          items.push(token);
        }
      }
      
      // If we're still in a group at the end, add what we have
      if (current) {
        items.push(current);
      }
      
      return items.filter(item => item.length > 0);
    };
    
    // Check if single line or double line
    if (lines.length === 1) {
      // Single line mode: randomize list
      const names = parseItems(lines[0]);
      if (names.length === 0) {
        await msg.reply({ content: 'استفاده: `.sort name1 name2 name3 ...`' });
        return;
      }
      
      // Shuffle
      const shuffled = [...names];
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
      }
      
      // Format output in embed
      const output = shuffled.map((name, i) => `${i + 1}. ${name}`).join('\n');
      const embed = new EmbedBuilder()
        .setDescription(output)
        .setColor(0x2f3136);
      
      if (isDM) {
        try {
          await msg.author.send({ embeds: [embed] });
          await msg.reply({ content: '✅ نتایج در DM شما ارسال شد.' });
        } catch {
          await msg.reply({ content: '❌ نمی‌توانم DM بفرستم. لطفاً DM های خود را باز کنید.' });
        }
      } else {
        await msg.reply({ embeds: [embed] });
      }
      return;
    }
    
    // Double line mode: pair two groups
    if (lines.length === 2) {
      const group1 = parseItems(lines[0]);
      const group2 = parseItems(lines[1]);
      
      if (group1.length !== group2.length) {
        await msg.reply({ content: 'دو گروه باهم برابر نیستند ❌' });
        return;
      }
      
      if (group1.length === 0) {
        await msg.reply({ content: 'لطفاً حداقل یک جفت وارد کنید.' });
        return;
      }
      
      // Check if group1 items are all mentions
      const mentionRegex = /<@!?(\d+)>/;
      const group1Mentions = group1.map(item => {
        const match = item.match(mentionRegex);
        return match ? match[1] : null;
      });
      const group1AllMentions = group1Mentions.every(id => id !== null);
      
      // Check if group2 items are all mentions
      const group2Mentions = group2.map(item => {
        const match = item.match(mentionRegex);
        return match ? match[1] : null;
      });
      const group2AllMentions = group2Mentions.every(id => id !== null);
      
      // Shuffle both groups
      const shuffled1 = [...group1];
      for (let i = shuffled1.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled1[i], shuffled1[j]] = [shuffled1[j], shuffled1[i]];
      }
      
      const shuffled2 = [...group2];
      for (let i = shuffled2.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled2[i], shuffled2[j]] = [shuffled2[j], shuffled2[i]];
      }
      
      // Format output with arrow in embed
      const output = shuffled1.map((name, i) => `${i + 1}. ${name} ⮕ ${shuffled2[i]}`).join('\n');
      const embed = new EmbedBuilder()
        .setDescription(output)
        .setColor(0x2f3136);
      
      if (isDM) {
        // Check if we should send individual DMs to mentioned users
        const shouldSendIndividualDMs = group1AllMentions;
        
        if (shouldSendIndividualDMs) {
          // Send individual DMs to mentioned users
          const failedDMs: string[] = [];
          
          // Send to group1 users
          for (let i = 0; i < shuffled1.length; i++) {
            const userMention = shuffled1[i];
            const pairedItem = shuffled2[i];
            const match = userMention.match(mentionRegex);
            
            if (match) {
              const userId = match[1];
              try {
                const user = await msg.client.users.fetch(userId);
                const individualEmbed = new EmbedBuilder()
                  .setDescription(`${i + 1}. ${userMention} ⮕ ${pairedItem}`)
                  .setColor(0x2f3136);
                
                await user.send({ embeds: [individualEmbed] });
              } catch {
                failedDMs.push(userMention);
              }
            }
          }
          
          // If group2 is also all mentions (scenario 4), send to them too
          if (group2AllMentions) {
            for (let i = 0; i < shuffled2.length; i++) {
              const userMention = shuffled2[i];
              const pairedItem = shuffled1[i];
              const match = userMention.match(mentionRegex);
              
              if (match) {
                const userId = match[1];
                try {
                  const user = await msg.client.users.fetch(userId);
                  const individualEmbed = new EmbedBuilder()
                    .setDescription(`${i + 1}. ${pairedItem} ⮕ ${userMention}`)
                    .setColor(0x2f3136);
                  
                  await user.send({ embeds: [individualEmbed] });
                } catch {
                  failedDMs.push(userMention);
                }
              }
            }
          }

          // Also send the full combined result to the command sender's DM
          try {
            await msg.author.send({ embeds: [embed] });
          } catch {
            const authorMention = `<@${msg.author.id}>`;
            failedDMs.push(authorMention);
          }
          
          // Send confirmation or error messages
          if (failedDMs.length === 0) {
            await msg.reply({ content: '✅ نتایج در DM تمام افراد منشن شده ارسال شد.' });
          } else {
            const failedList = failedDMs.map(mention => `دایرکت ${mention} بسته است ❌`).join('\n');
            await msg.reply({ content: failedList });
          }
        } else {
          // Original behavior: send to command sender only
          try {
            await msg.author.send({ embeds: [embed] });
            await msg.reply({ content: '✅ نتایج در DM شما ارسال شد.' });
          } catch {
            await msg.reply({ content: '❌ نمی‌توانم DM بفرستم. لطفاً DM های خود را باز کنید.' });
          }
        }
      } else {
        await msg.reply({ embeds: [embed] });
      }
      return;
    }
    
    // Invalid format
    await msg.reply({ content: 'استفاده:\n**یک خط:** `.sort name1 name2 ...`\n**دو خط:** `.sort group1...\ngroup2...`' });
    return;
  }

  // .esm1 — convert text to Mathematical Sans-Serif Bold or change user nickname
  if (isCmd('esm1')) {
    if (!msg.guild) {
      await msg.reply({ content: 'این دستور فقط در سرور کار می‌کند.' });
      return;
    }

    const arg = content.slice(5).trim(); // Remove '.esm1'
    
    // Check if there's a mention or reply
    let targetMember: GuildMember | null = null;
    let textToConvert = arg;

    // Priority 1: Check for mention
    if (msg.mentions.members && msg.mentions.members.size > 0) {
      targetMember = msg.mentions.members.first()!;
      // Remove the mention from the text
      textToConvert = arg.replace(/<@!?\d+>/g, '').trim();
    }
    // Priority 2: Check for reply
    else if (msg.reference) {
      try {
        const repliedMsg = await msg.channel.messages.fetch(msg.reference.messageId!);
        if (repliedMsg.member) {
          targetMember = repliedMsg.member;
          textToConvert = arg; // Use all text after .esm1
        }
      } catch {}
    }

    // If no text provided, show usage
    if (!textToConvert) {
      await msg.reply({ content: 'استفاده:\n`.esm1 @user نام جدید` - تغییر نیک نیم\n`.esm1 متن` - تبدیل متن به فونت Mathematical Sans-Serif Bold' });
      return;
    }

    // Convert text to Mathematical Sans-Serif Bold
    const convertedText = toMathSansSerifBold(textToConvert);

    // If target member exists, change their nickname
    if (targetMember) {
      try {
        await targetMember.setNickname(convertedText);
        await msg.reply({ content: `✅ نیک نیم <@${targetMember.id}> تغییر کرد به: ${convertedText}` });
      } catch (err) {
        await msg.reply({ content: `❌ خطا در تغییر نیک نیم. ممکن است مجوز کافی وجود نداشته باشد یا کاربر دسترسی بالاتری داشته باشد.` });
      }
    } else {
      // Just send the converted text
      await msg.reply({ content: convertedText });
    }
    return;
  }

  // Timer command with custom prefix
  const timerPrefix = getTimerPrefix();
  const escapedPrefix = timerPrefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const timerCmdPattern = new RegExp(`^${escapedPrefix}t(?:\\s|$)`);
  const altTimer1 = /^!t(?:\s|$)/.test(content); // !t alternative
  const altTimer2 = /^\.ت(?:\s|$)/.test(content); // .ت alternative
  if (!timerCmdPattern.test(content) && !altTimer1 && !altTimer2) return;

  let cmdLength = timerPrefix.length + 1; // default: prefix + 't'
  if (altTimer1) cmdLength = 2; // !t
  else if (altTimer2) cmdLength = 3; // .ت
  const args = content.slice(cmdLength).trim();
  if (!args) {
    await msg.reply({ content: `استفاده: \`${timerPrefix}t 10m [دلیل]\` یا \`${timerPrefix}t 60 [دلیل]\` (عدد = ثانیه)` });
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

// HTTP server for Railway health checks - Start BEFORE Discord login
const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ 
      status: 'ok', 
      uptime: process.uptime(),
      bot: botReady ? 'online' : 'connecting',
      timestamp: new Date().toISOString()
    }));
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
  }
});

server.listen(PORT, () => {
  console.log(`[HTTP] Health check server listening on port ${PORT}`);
  console.log(`[HTTP] Health endpoint: http://0.0.0.0:${PORT}/health`);
});

// Graceful shutdown handlers
process.on('SIGTERM', async () => {
  console.log('[SHUTDOWN] Received SIGTERM signal, shutting down gracefully...');
  try {
    // Close HTTP server first
    server.close(() => {
      console.log('[SHUTDOWN] HTTP server closed');
    });
    
    // Data is saved in realtime to database, no need to save on shutdown
    // Destroy the client
    if (client) {
      client.destroy();
      console.log('[SHUTDOWN] Discord client destroyed successfully');
    }
    
    // Exit cleanly after a short delay to allow cleanup
    setTimeout(() => {
      console.log('[SHUTDOWN] Exiting process...');
      process.exit(0);
    }, 1000);
  } catch (err: any) {
    console.error('[SHUTDOWN] Error during shutdown:', err);
    process.exit(1);
  }
});

process.on('SIGINT', async () => {
  console.log('[SHUTDOWN] Received SIGINT signal, shutting down gracefully...');
  try {
    // Close HTTP server
    server.close(() => {
      console.log('[SHUTDOWN] HTTP server closed');
    });
    
    // Data is saved in realtime to database, no need to save on shutdown
    if (client) {
      client.destroy();
      console.log('[SHUTDOWN] Discord client destroyed successfully');
    }
    
    setTimeout(() => {
      console.log('[SHUTDOWN] Exiting process...');
      process.exit(0);
    }, 1000);
  } catch (err: any) {
    console.error('[SHUTDOWN] Error during shutdown:', err);
    process.exit(1);
  }
});

// Login to Discord AFTER HTTP server is ready
client.login(token);
