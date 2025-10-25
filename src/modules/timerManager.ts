import {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ChatInputCommandInteraction,
  Client,
  EmbedBuilder,
  GuildTextBasedChannel,
  SlashCommandBuilder,
  userMention,
} from 'discord.js';
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
          .setDescription('Duration (e.g., 10m, 2h, 1d) or pure number as seconds (e.g., 45)')
          .setRequired(true),
      )
      .addStringOption((opt: import('discord.js').SlashCommandStringOption) =>
        opt.setName('reason').setDescription('Optional reason for the timer').setRequired(false),
      ),
  )
  .addSubcommand((sub: import('discord.js').SlashCommandSubcommandBuilder) =>
    sub.setName('list').setDescription('List your active timers in this server'),
  )
  .addSubcommand((sub: import('discord.js').SlashCommandSubcommandBuilder) =>
    sub
      .setName('cancel')
      .setDescription('Cancel a timer by its ID (from /timer list)')
      .addStringOption((opt: import('discord.js').SlashCommandStringOption) =>
        opt.setName('id').setDescription('Timer ID to cancel').setRequired(true),
      ),
  );

export type ActiveTimer = {
  id: string;
  userId: string;
  guildId: string;
  channelId: string;
  reason?: string | null;
  endsAt: number; // epoch ms
  // Visuals
  messageId?: string;
  totalMs?: number;
  lastPaintSec?: number; // برای جلوگیری از ادیت‌های بیهوده
};

export class TimerManager {
  private client: Client;
  // Map guildId -> Map timerId -> ActiveTimer
  private timers: Map<string, Map<string, ActiveTimer>> = new Map();
  private tickHandle: NodeJS.Timeout;

  constructor(client: Client) {
    this.client = client;
    // حلقه تکی برای همه تایمرها (هر ۱ ثانیه)
    this.tickHandle = setInterval(() => {
      this.tick().catch(() => {});
    }, 1000);
  }

  public list(guildId: string, userId?: string): ActiveTimer[] {
    const g = this.timers.get(guildId);
    if (!g) return [];
    const values = Array.from(g.values());
    return userId ? values.filter((t) => t.userId === userId) : values;
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
    g.delete(id);
    if (g.size === 0) this.timers.delete(guildId);
    return true;
  }

  public setTimer(opts: {
    guildId: string;
    channelId: string;
    userId: string;
    durationMs: number;
    reason?: string | null;
  }): ActiveTimer {
    const id = `${Date.now()}-${Math.floor(Math.random() * 100000)}`;
    const endsAt = Date.now() + opts.durationMs;

    const at: ActiveTimer = {
      id,
      userId: opts.userId,
      guildId: opts.guildId,
      channelId: opts.channelId,
      reason: opts.reason ?? null,
      endsAt,
      totalMs: opts.durationMs,
      lastPaintSec: undefined,
    };

    if (!this.timers.has(opts.guildId)) this.timers.set(opts.guildId, new Map());
    this.timers.get(opts.guildId)!.set(id, at);
    return at;
  }

  public extend(guildId: string, id: string, deltaMs: number): ActiveTimer | null {
    if (deltaMs <= 0) return null;
    const g = this.timers.get(guildId);
    if (!g) return null;
    const t = g.get(id);
    if (!t) return null;
    t.endsAt += deltaMs;
    t.totalMs = (t.totalMs ?? 0) + deltaMs;
    t.lastPaintSec = undefined; // فورس رندر فریم بعدی
    return t;
  }

  private async tick() {
    const now = Date.now();
    for (const [guildId, gmap] of this.timers) {
      for (const [id, t] of gmap) {
        const remainingMs = Math.max(t.endsAt - now, 0);
        const remainingSec = Math.ceil(remainingMs / 1000);

        // پایان تایمر
        if (remainingMs <= 0) {
          try {
            const channel = await this.client.channels.fetch(t.channelId);
            if (channel && channel.isTextBased()) {
              const c = channel as GuildTextBasedChannel;
              const mention = userMention(t.userId);
              const reasonText = t.reason ? `Reason: ${t.reason}` : '';
              await c.send(`⏰ ${mention} زمان تموم شد! ${reasonText}`.trim());
              if (t.messageId) {
                const m = await c.messages.fetch(t.messageId).catch(() => null);
                if (m) await m.edit({ embeds: [buildFinishedEmbed(t)], components: [] });
              }
            }
          } catch {}
          // حذف تایمر
          gmap.delete(id);
          continue;
        }

        // آپدیت ویژوال فقط زمانی که ثانیه عوض شده
        if (t.messageId && t.lastPaintSec !== remainingSec) {
          t.lastPaintSec = remainingSec;
          try {
            const channel = await this.client.channels.fetch(t.channelId);
            if (channel && channel.isTextBased()) {
              const c = channel as GuildTextBasedChannel;
              const m = await c.messages.fetch(t.messageId).catch(() => null);
              if (m) {
                await m.edit({
                  embeds: [buildCountdownEmbed(t)],
                  components: [buildAddTimeRow(t.id)],
                });
              }
            }
          } catch {
            // اگر ادیت خطا داد، صرفاً رها کن تا تیک بعدی دوباره تلاش کند
          }
        }
      }
      if (gmap.size === 0) this.timers.delete(guildId);
    }
  }
}

export async function handleTimerInteraction(
  interaction: ChatInputCommandInteraction,
  manager: TimerManager,
) {
  const sub = interaction.options.getSubcommand();

  if (sub === 'set') {
    const durationRaw = interaction.options.getString('duration', true).trim();
    const reason = interaction.options.getString('reason') ?? null;

    const durationMs = parseDuration(durationRaw);
    if (!durationMs || durationMs < 1000) {
      await interaction.reply({
        content: 'مدت زمان نامعتبر. نمونه: 10m یا 2h یا 1d یا فقط عدد (ثانیه): 45',
        ephemeral: true,
      });
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
      at.messageId = msg.id; // حلقه تکی کار آپدیت را به عهده می‌گیرد
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
      .map(
        (t) =>
          `• ID: ${t.id} | پایان: ${formatHMS(Math.max(t.endsAt - Date.now(), 0))}${
            t.reason ? ` | دلیل: ${t.reason}` : ''
          }`,
      );
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
      const embed = new EmbedBuilder().setDescription(`❌ تایمر لغو شد.`).setColor(0xff5555);
      await interaction.reply({ embeds: [embed] });
    } else {
      await interaction.reply({ content: 'شناسه تایمر پیدا نشد.', ephemeral: true });
    }
    return;
  }
}

export function parseDuration(input: string): number | null {
  const trimmed = input.trim();
  if (/^\d+$/.test(trimmed)) {
    const n = Number(trimmed);
    if (n > 0) return n * 1000;
    return null;
  }
  const v = ms(trimmed);
  if (typeof v === 'number' && isFinite(v) && v > 0) return v;
  return null;
}

export function makeTimerSetEmbed(at: ActiveTimer): EmbedBuilder {
  return new EmbedBuilder()
    .setTitle('⏳ تایمر تنظیم شد')
    .setColor(0x5865f2)
    .addFields(
      { name: 'پایان', value: formatHMS(Math.max(at.endsAt - Date.now(), 0)), inline: true },
      { name: 'دلیل', value: at.reason ?? '—', inline: true },
    );
}

export function buildCountdownEmbed(at: ActiveTimer): EmbedBuilder {
  const remaining = Math.max(at.endsAt - Date.now(), 0);
  const total = Math.max(at.totalMs ?? remaining, 1);
  const bar = makeBar(remaining, total);
  const embed = new EmbedBuilder()
    .setTitle('⏳ شمارش معکوس')
    .setColor(0x5865f2)
    .setDescription(`${bar}\nباقی‌مانده: ${formatHMS(remaining)}`);
  if (at.reason) embed.addFields({ name: 'دلیل', value: at.reason, inline: true });
  return embed;
}

export function buildFinishedEmbed(at: ActiveTimer): EmbedBuilder {
  const embed = new EmbedBuilder()
    .setTitle('⏰ تایمر به پایان رسید')
    .setColor(0xff5555)
    .setDescription('تمام شد.');
  if (at.reason) embed.addFields({ name: 'دلیل', value: at.reason, inline: true });
  return embed;
}

export function buildAddTimeRow(timerId: string) {
  return new ActionRowBuilder<ButtonBuilder>().addComponents(
    new ButtonBuilder().setCustomId(`timer:add:${timerId}`).setStyle(ButtonStyle.Primary).setLabel('افزودن زمان'),
  );
}

function makeBar(remainingMs: number, totalMs: number): string {
  const width = 20;
  const clamped = Math.max(0, Math.min(1, remainingMs / Math.max(totalMs, 1)));
  const filled = Math.round((1 - clamped) * width);
  const empty = width - filled;
  return `【${'▰'.repeat(filled)}${'▱'.repeat(empty)}】`;
}

function formatHMS(msNum: number): string {
  let s = Math.floor(msNum / 1000);
  const hrs = Math.floor(s / 3600);
  s -= hrs * 3600;
  const mins = Math.floor(s / 60);
  s -= mins * 60;
  const sec = s;
  const hh = hrs > 0 ? String(hrs).padStart(2, '0') + ':' : '';
  const mm = String(mins).padStart(2, '0');
  const ss = String(sec).padStart(2, '0');
  return `${hh}${mm}:${ss}`;
}
