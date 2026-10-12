import amqp from "amqplib";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";

async function main() {
  console.log("Starting Peril client...");

  const connString = "amqp://guest:guest@localhost:5672/";
  const ch = await amqp.connect(connString);
  const clientName = await clientWelcome();

  const [channel, queue] = await declareAndBind(ch, 
    ExchangePerilDirect, 
    `pause.${clientName}`, 
    PauseKey, 
    SimpleQueueType.Transient);

  const gameState = new GameState(clientName);

  while (true) {
    printClientHelp();
    const words = await getInput();

    switch (words[0]) {
      case "spawn": {
        try {
          commandSpawn(gameState, words);
        } catch (err) {
          console.log(err);
        };

        break;
      };
      case "move": {
        try {
          commandMove(gameState, words);
        } catch (err) {
          console.log(err);
        }
        break;
      };
      case "status": {
        commandStatus(gameState);
      };
      case "spam": {
        console.log("Spamming not allowed yet!");
      };
      case "help": {
        printClientHelp();
        break;
      };
      case "quit": {
        printQuit();
        process.exit(0);
      };
    };
  };
};

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
