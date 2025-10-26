import { ChatInputCommandInteraction, Client, EmbedBuilder, GuildTextBasedChannel, SlashCommandBuilder } from 'discord.js';
import ms from 'ms';

export const timerCommand = new SlashCommandBuilder()
  .setName('timer')
  .setDescription('Set, list, or cancel timers')
  .addSubcommand((sub: import('discord.js').SlashCommandSubcommandBuilder) =>
    sub
      .setName('set')
      .setDescription('Set a timer')
      .addStringOption((opt: import('discord.js').SlashCommandStringOption) =>
        opt
          .setName('duration')
          .setDescription('Duration (e.g., 10m, 2h, 1d)')
          .setRequired(true)
      )
      .addStringOption((opt: import('discord.js').SlashCommandStringOption) =>
        opt
          .setName('reason')
          .setDescription('Optional reason for the timer')
          .setRequired(false)
      )
  )
  .addSubcommand((sub: import('discord.js').SlashCommandSubcommandBuilder) =>
    sub
      .setName('list')
      .setDescription('List your active timers in this server')
  )
  .addSubcommand((sub: import('discord.js').SlashCommandSubcommandBuilder) =>
    sub
      .setName('cancel')
      .setDescription('Cancel a timer by its ID (from /timer list)')
      .addStringOption((opt: import('discord.js').SlashCommandStringOption) =>
        opt
          .setName('id')
          .setDescription('Timer ID to cancel')
          .setRequired(true)
      )
  );

export type ActiveTimer = {
  id: string;
  userId: string;
  guildId: string;
  channelId: string;
  reason?: string | null;
  endsAt: number; // epoch ms
  timeout: NodeJS.Timeout;
  messageId?: string;
  interval?: NodeJS.Timeout | null;
  totalMs?: number;
  createdAt: number;
};

export class TimerManager {
  private client: Client;
  // Map guildId -> Map timerId -> ActiveTimer
  private timers: Map<string, Map<string, ActiveTimer>> = new Map();

  constructor(client: Client) {
    this.client = client;
  }

  public async extendLast(guildId: string, userId: string, deltaMs: number): Promise<ActiveTimer | null> {
    const g = this.timers.get(guildId);
    if (!g) return null;
    const candidates = Array.from(g.values()).filter(t => t.userId === userId);
    if (candidates.length === 0) return null;
    candidates.sort((a, b) => b.createdAt - a.createdAt);
    const t = candidates[0];
    if (!t) return null;
    t.endsAt += Math.max(0, deltaMs);

    // reschedule timeout
    clearTimeout(t.timeout);
    const remaining = Math.max(t.endsAt - Date.now(), 0);
    t.timeout = setTimeout(async () => {
      try {
        const channel = await this.client.channels.fetch(t.channelId);
        if (channel && channel.isTextBased()) {
          const c = channel as GuildTextBasedChannel;
          const reasonText = t.reason ?? '';
          const users = reasonText ? extractMentionUserIds(reasonText) : [];
          await c.send({
            content: reasonText || undefined,
            allowedMentions: users.length ? { users, parse: [] } : { parse: [] },
            embeds: [makeFinalEmbed()],
          });
          if (t.messageId) {
            const m = await c.messages.fetch(t.messageId).catch(() => null);
            if (m) {
              await m.edit({ embeds: [new EmbedBuilder().setTitle('پایان')] }).catch(() => {});
            }
          }
        }
      } catch {}
      finally {
        const g2 = this.timers.get(guildId);
        if (g2) {
          g2.delete(t.id);
          if (g2.size === 0) this.timers.delete(guildId);
        }
      }
    }, remaining);

    // update start embed relative timestamp to reflect new endsAt
    if (t.messageId) {
      try {
        const ch = await this.client.channels.fetch(t.channelId);
        if (ch && ch.isTextBased()) {
          const c = ch as GuildTextBasedChannel;
          const m = await c.messages.fetch(t.messageId).catch(() => null);
          if (m) {
            const unix = Math.floor(t.endsAt / 1000);
            await m.edit({ embeds: [new EmbedBuilder().setTitle(`<t:${unix}:R>`)] });
          }
        }
      } catch {}
    }
    return t;
  }

  public list(guildId: string, userId?: string): ActiveTimer[] {
    const g = this.timers.get(guildId);
    if (!g) return [];
    const values = Array.from(g.values());
    return userId ? values.filter(t => t.userId === userId) : values;
  }

  public findById(guildId: string, id: string): ActiveTimer | undefined {
    const g = this.timers.get(guildId);
    return g?.get(id);
  }

  public cancel(guildId: string, id: string): boolean {
    const g = this.timers.get(guildId);
    if (!g) return false;
    const t = g.get(id);
    if (!t) return false;
    clearTimeout(t.timeout);
    if (t.interval) clearInterval(t.interval);
    g.delete(id);
    if (g.size === 0) this.timers.delete(guildId);
    return true;
  }

  public setTimer(opts: { guildId: string; channelId: string; userId: string; durationMs: number; reason?: string | null; }): ActiveTimer {
    const id = `${Date.now()}-${Math.floor(Math.random() * 100000)}`;
    const endsAt = Date.now() + opts.durationMs;

    const timeout = setTimeout(async () => {
      try {
        const channel = await this.client.channels.fetch(opts.channelId);
        if (channel && channel.isTextBased()) {
          const c = channel as GuildTextBasedChannel;
          const reasonText = opts.reason ?? '';
          const users = reasonText ? extractMentionUserIds(reasonText) : [];
          await c.send({
            content: reasonText || undefined,
            allowedMentions: users.length ? { users, parse: [] } : { parse: [] },
            embeds: [makeFinalEmbed()],
          });
          // Edit the start message to a fixed minimal text to avoid 'ago'
          const g = this.timers.get(opts.guildId);
          const t = g?.get(id);
          if (t?.messageId) {
            const m = await c.messages.fetch(t.messageId).catch(() => null);
            if (m) {
              await m.edit({ embeds: [new EmbedBuilder().setTitle('پایان')] }).catch(() => {});
            }
          }
        }
      } catch {}
      finally {
        const g = this.timers.get(opts.guildId);
        if (g) {
          g.delete(id);
          if (g.size === 0) this.timers.delete(opts.guildId);
        }
      }
    }, opts.durationMs);

    const at: ActiveTimer = {
      id,
      userId: opts.userId,
      guildId: opts.guildId,
      channelId: opts.channelId,
      reason: opts.reason ?? null,
      endsAt,
      timeout,
      interval: null,
      totalMs: opts.durationMs,
      createdAt: Date.now(),
    };

    if (!this.timers.has(opts.guildId)) this.timers.set(opts.guildId, new Map());
    this.timers.get(opts.guildId)!.set(id, at);
    return at;
  }

  // Legacy extend by id (kept for compatibility if used somewhere)
  public extend(guildId: string, id: string, deltaMs: number): ActiveTimer | null {
    const g = this.timers.get(guildId);
    if (!g) return null;
    const t = g.get(id);
    if (!t) return null;
    const clamped = Math.max(0, Math.min(deltaMs, 30_000));
    if (clamped <= 0) return null;
    t.endsAt += clamped;

    // Reschedule timeout
    clearTimeout(t.timeout);
    const remaining = Math.max(t.endsAt - Date.now(), 0);
    t.timeout = setTimeout(async () => {
      try {
        const channel = await this.client.channels.fetch(t.channelId);
        if (channel && channel.isTextBased()) {
          const c = channel as GuildTextBasedChannel;
          const reasonText = t.reason ?? '';
          const users = reasonText ? extractMentionUserIds(reasonText) : [];
          await c.send({
            content: reasonText || undefined,
            allowedMentions: users.length ? { users, parse: [] } : { parse: [] },
            embeds: [makeFinalEmbed()],
          });
          if (t.messageId) {
            const m = await c.messages.fetch(t.messageId).catch(() => null);
            if (m) {
              await m.edit({ embeds: [new EmbedBuilder().setTitle('پایان')] }).catch(() => {});
            }
          }
        }
      } catch {}
      finally {
        const g2 = this.timers.get(guildId);
        if (g2) {
          g2.delete(id);
          if (g2.size === 0) this.timers.delete(guildId);
        }
      }
    }, remaining);
    return t;
  }
}

export async function handleTimerInteraction(interaction: ChatInputCommandInteraction, manager: TimerManager) {
  const sub = interaction.options.getSubcommand();

  if (sub === 'set') {
    const durationRaw = interaction.options.getString('duration', true).trim();
    const reason = interaction.options.getString('reason') ?? null;

    const durationMs = parseDuration(durationRaw);
    if (!durationMs || durationMs < 1000) {
      await interaction.reply({ content: 'مدت زمان نامعتبر. نمونه: 10m یا 2h یا 1d یا فقط عدد (ثانیه): 45', ephemeral: true });
      return;
    }

    if (!interaction.guildId || !interaction.channelId) {
      await interaction.reply({ content: 'این دستور باید داخل سرور اجرا شود.', ephemeral: true });
      return;
    }

    const at = manager.setTimer({
      guildId: interaction.guildId,
      channelId: interaction.channelId,
      userId: interaction.user.id,
      durationMs,
      reason,
    });

    const embed = makeTimerSetEmbed(at);
    const replied = await interaction.reply({ embeds: [embed], fetchReply: true });
    if ('id' in (replied as any)) {
      const msg = replied as any;
      at.messageId = msg.id;
    }
    return;
  }

  if (sub === 'list') {
    if (!interaction.guildId) {
      await interaction.reply({ content: 'این دستور باید داخل سرور اجرا شود.', ephemeral: true });
      return;
    }
    const timers = manager.list(interaction.guildId, interaction.user.id);
    if (timers.length === 0) {
      await interaction.reply({ content: 'هیچ تایمری فعال نیست.', ephemeral: true });
      return;
    }
    const lines = timers
      .sort((a, b) => a.endsAt - b.endsAt)
      .map(t => `• ID: ${t.id} | پایان: <t:${Math.floor(t.endsAt / 1000)}:R>${t.reason ? ` | دلیل: ${t.reason}` : ''}`);
    const embed = new EmbedBuilder()
      .setTitle('تایمرهای فعال شما')
      .setDescription(lines.join('\n'))
      .setColor(0x2f3136);
    await interaction.reply({ embeds: [embed], ephemeral: true });
    return;
  }

  if (sub === 'cancel') {
    if (!interaction.guildId) {
      await interaction.reply({ content: 'این دستور باید داخل سرور اجرا شود.', ephemeral: true });
      return;
    }
    const id = interaction.options.getString('id', true);
    const ok = manager.cancel(interaction.guildId, id);
    if (ok) {
      const embed = new EmbedBuilder().setDescription(`❌ تایمر با ID 
${id}
 لغو شد.`).setColor(0xff5555);
      await interaction.reply({ embeds: [embed] });
    } else {
      await interaction.reply({ content: 'شناسه تایمر پیدا نشد.', ephemeral: true });
    }
    return;
  }
}

export function parseDuration(input: string): number | null {
  const trimmed = input.trim();
  // If input is purely numeric, treat as seconds (override ms default which would treat as ms)
  if (/^\d+$/.test(trimmed)) {
    const n = Number(trimmed);
    if (n > 0) return n * 1000;
    return null;
  }
  // Else, use ms package (supports 1h, 10m, 30s, etc.)
  const v = ms(trimmed);
  if (typeof v === 'number' && isFinite(v) && v > 0) return v;
  return null;
}

export function makeTimerSetEmbed(at: ActiveTimer): EmbedBuilder {
  const unix = Math.floor(at.endsAt / 1000);
  return new EmbedBuilder().setTitle(`<t:${unix}:R>`);
}

// No live countdown; we rely on Discord relative time. Formatter below remains for other uses.

function formatHMS(msNum: number): string {
  let s = Math.floor(msNum / 1000);
  const hrs = Math.floor(s / 3600); s -= hrs * 3600;
  const mins = Math.floor(s / 60); s -= mins * 60;
  const sec = s;
  const hh = hrs > 0 ? String(hrs).padStart(2, '0') + ':' : '';
  const mm = String(mins).padStart(2, '0');
  const ss = String(sec).padStart(2, '0');
  return `${hh}${mm}:${ss}`;
}

function makeFinalEmbed(): EmbedBuilder {
  return new EmbedBuilder().setTitle('⏰ پایان زمان');
}

function extractMentionUserIds(text: string): string[] {
  const ids: string[] = [];
  const regex = /<@!?([0-9]+)>/g;
  let m: RegExpExecArray | null;
  while ((m = regex.exec(text)) !== null) {
    ids.push(m[1]);
  }
  return Array.from(new Set(ids));
}
