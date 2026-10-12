import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
  const connString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(connString);
  console.log("Sucessful conection!");

  const ch = await conn.createConfirmChannel();

  const [topicCh, queue] = await declareAndBind(
    conn, 
    ExchangePerilTopic, 
    "game_logs", 
    "game_logs.*", 
    SimpleQueueType.Durable);

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    }),
  );

  printServerHelp();
  while (true) {
    const words = await getInput();
    if (words.length === 0) continue;

    switch (words[0]) {
      case "pause": {
        console.log("sending a pause message!");
        publishJSON(ch, ExchangePerilDirect, PauseKey, {
          isPaused: true
        } satisfies PlayingState);
        break;
      };
      case "resume": {
        console.log("sending a resume message!");
        publishJSON(ch, ExchangePerilDirect, PauseKey, {
          isPaused: false,
        } satisfies PlayingState);
        break;
      };
      case "quit": {
        console.log("Goodbye!");
        process.exit(0);
      }
      default: {
        console.log("don't understand the command!")
      }
    }
  };
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
