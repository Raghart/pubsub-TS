import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

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
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
