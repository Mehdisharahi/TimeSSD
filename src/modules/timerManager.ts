import { ChatInputCommandInteraction, Client, EmbedBuilder, GuildTextBasedChannel, SlashCommandBuilder, userMention } from 'discord.js';
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
};

export class TimerManager {
  private client: Client;
  // Map guildId -> Map timerId -> ActiveTimer
  private timers: Map<string, Map<string, ActiveTimer>> = new Map();

  constructor(client: Client) {
    this.client = client;
  }

  public list(guildId: string, userId?: string): ActiveTimer[] {
    const g = this.timers.get(guildId);
    if (!g) return [];
    const values = Array.from(g.values());
    return userId ? values.filter(t => t.userId === userId) : values;
  }

  public cancel(guildId: string, id: string): boolean {
    const g = this.timers.get(guildId);
    if (!g) return false;
    const t = g.get(id);
    if (!t) return false;
    clearTimeout(t.timeout);
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
          const mention = userMention(opts.userId);
          const reasonText = opts.reason ? `Reason: ${opts.reason}` : '';
          await c.send(`⏰ ${mention} زمان تموم شد! ${reasonText}`.trim());
        }
      } catch (e) {
        // swallow
      } finally {
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
    };

    if (!this.timers.has(opts.guildId)) this.timers.set(opts.guildId, new Map());
    this.timers.get(opts.guildId)!.set(id, at);
    return at;
  }
}

export async function handleTimerInteraction(interaction: ChatInputCommandInteraction, manager: TimerManager) {
  const sub = interaction.options.getSubcommand();

  if (sub === 'set') {
    const durationRaw = interaction.options.getString('duration', true).trim();
    const reason = interaction.options.getString('reason') ?? null;

    const durationMs = parseDuration(durationRaw);
    if (!durationMs || durationMs < 1000) {
      await interaction.reply({ content: 'مدت زمان نامعتبر. نمونه: 10m یا 2h یا 1d', ephemeral: true });
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
    await interaction.reply({ embeds: [embed] });
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
  // Try ms package first (supports 1h, 10m, 30s, etc.)
  const v = ms(input);
  if (typeof v === 'number' && isFinite(v)) return v;

  // Try numeric minutes fallback
  const n = Number(input);
  // Treat plain numbers as seconds per user request
  if (!isNaN(n) && isFinite(n) && n > 0) return n * 1000;
  return null;
}

export function makeTimerSetEmbed(at: ActiveTimer): EmbedBuilder {
  return new EmbedBuilder()
    .setTitle('⏳ تایمر تنظیم شد')
    .setColor(0x5865f2)
    .addFields(
      { name: 'ID', value: at.id, inline: false },
      { name: 'پایان', value: `<t:${Math.floor(at.endsAt / 1000)}:R>`, inline: true },
      { name: 'دلیل', value: at.reason ?? '—', inline: true },
    );
}
