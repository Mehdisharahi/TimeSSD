import 'dotenv/config';
import { ActionRowBuilder, Client, GatewayIntentBits, Interaction, Message, ModalBuilder, TextInputBuilder, TextInputStyle } from 'discord.js';
import { handleTimerInteraction, TimerManager, parseDuration, makeTimerSetEmbed, startCountdown, buildCountdownEmbed, buildAddTimeRow } from './modules/timerManager';

const token = process.env.BOT_TOKEN;
if (!token) {
  console.error('Missing BOT_TOKEN in .env');
  process.exit(1);
}

const client = new Client({ intents: [
  GatewayIntentBits.Guilds,
  GatewayIntentBits.GuildMessages,
  GatewayIntentBits.MessageContent,
] });

// Create a single TimerManager instance for the bot lifecycle
export const timerManager = new TimerManager(client);

client.once('ready', () => {
  console.log(`TimeSSD is online as ${client.user?.tag}`);
});

client.on('interactionCreate', async (interaction: Interaction) => {
  if (interaction.isChatInputCommand()) {
    if (interaction.commandName === 'timer') {
      await handleTimerInteraction(interaction, timerManager);
    }
    return;
  }

  if (interaction.isButton()) {
    const id = interaction.customId;
    if (!id.startsWith('timer:add:')) return;
    const timerId = id.split(':')[2];
    const modal = new ModalBuilder()
      .setCustomId(`timer:addmodal:${timerId}`)
      .setTitle('افزودن زمان');
    const input = new TextInputBuilder()
      .setCustomId('delta')
      .setLabel('مدت زمان اضافه (مثال: 30s, 2m, 45)')
      .setRequired(true)
      .setStyle(TextInputStyle.Short);
    const row = new ActionRowBuilder<TextInputBuilder>().addComponents(input);
    modal.addComponents(row);
    await interaction.showModal(modal);
    return;
  }

  if (interaction.isModalSubmit()) {
    const id = interaction.customId;
    if (!id.startsWith('timer:addmodal:')) return;
    const timerId = id.split(':')[2];
    const deltaRaw = interaction.fields.getTextInputValue('delta').trim();
    const deltaMs = parseDuration(deltaRaw);
    if (!deltaMs || deltaMs <= 0) {
      await interaction.reply({ content: 'مقدار نامعتبر. نمونه: 30s یا 2m یا 45 (ثانیه).', ephemeral: true });
      return;
    }
    if (!interaction.guildId) {
      await interaction.reply({ content: 'این عمل باید داخل سرور انجام شود.', ephemeral: true });
      return;
    }
    const t = timerManager.extend(interaction.guildId, timerId, deltaMs);
    if (!t) {
      await interaction.reply({ content: 'تایمر پیدا نشد یا پایان یافته است.', ephemeral: true });
      return;
    }
    try {
      const ch = await interaction.client.channels.fetch(t.channelId);
      if (ch && ch.isTextBased() && t.messageId) {
        const c = ch as any;
        const m = await c.messages.fetch(t.messageId).catch(() => null);
        if (m) {
          await m.edit({ embeds: [buildCountdownEmbed(t)], components: [buildAddTimeRow(t.id)] });
        }
      }
    } catch {}
    await interaction.reply({ content: 'زمان اضافه شد.', ephemeral: true });
    return;
  }
});

// Dot-prefix command: .t <duration> [reason]
client.on('messageCreate', async (msg: Message) => {
  if (!msg.inGuild()) return;
  if (msg.author.bot) return;
  const content = msg.content.trim();
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
    await msg.reply({ content: 'مدت زمان نامعتبر. نمونه: 10m یا 2h یا 1d یا فقط عدد (ثانیه): 45' });
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
  await startCountdown(client, at);
});

client.login(token);
