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
          .setDescription('Duration (e.g., 10m, 2h, 1d) or pure number as seconds (e.g., 45)')
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
