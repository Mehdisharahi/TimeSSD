import 'dotenv/config';
import { REST, Routes } from 'discord.js';
import { timerCommand } from './modules/timerManager';

const token = process.env.BOT_TOKEN;
const appId = process.env.APPLICATION_ID; // Discord Application (Client) ID
const guildId = process.env.GUILD_ID; // Optional: for faster dev (guild-scoped)

if (!token || !appId) {
  console.error('Missing BOT_TOKEN or APPLICATION_ID in .env');
  process.exit(1);
}

const rest = new REST({ version: '10' }).setToken(token as string);

async function main() {
  const commands = [timerCommand.toJSON()];
  const appIdStr = appId as string; // safe due to guard above
  const guildIdStr = guildId as string | undefined;

  try {
    if (guildIdStr) {
      console.log(`Registering guild commands for guild ${guildIdStr}...`);
      await rest.put(Routes.applicationGuildCommands(appIdStr, guildIdStr), { body: commands });
      console.log('Guild commands registered.');
    } else {
      console.log('Registering global commands (may take up to 1 hour to propagate)...');
      await rest.put(Routes.applicationCommands(appIdStr), { body: commands });
      console.log('Global commands registered.');
    }
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
