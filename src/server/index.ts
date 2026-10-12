import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";

async function main() {
  const connString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(connString);
  console.log("Sucessful conection!");

  const ch = await conn.createConfirmChannel();
  publishJSON(ch, ExchangePerilDirect, PauseKey, {
    isPaused: true
  } satisfies PlayingState);

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
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
