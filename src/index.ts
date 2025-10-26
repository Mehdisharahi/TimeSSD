import 'dotenv/config';
import { Client, GatewayIntentBits, Interaction, Message, EmbedBuilder } from 'discord.js';
import { handleTimerInteraction, TimerManager, parseDuration, makeTimerSetEmbed } from './modules/timerManager';

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
});

// Dot-prefix command: .t <duration> [reason]
client.on('messageCreate', async (msg: Message) => {
  if (!msg.inGuild()) return;
  if (msg.author.bot) return;
  const content = msg.content.trim();

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
    const embed = new EmbedBuilder()
      .setTitle(`Avatar: ${user.tag}`)
      .setImage(url)
      .setURL(url);
    await msg.reply({ embeds: [embed] });
    return;
  }

  // .e <seconds> — extend last timer of this user (silent)
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
});

client.login(token);
