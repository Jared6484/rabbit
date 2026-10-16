import amqp from "amqplib";
import { createConnection } from "net";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { type PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
  console.log("Starting Peril server...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  if(conn){ 
    console.log("Successfull Connection");
  }

  const confChannel = await conn.createConfirmChannel();
  
  const state: PlayingState = { isPaused: true};
  await publishJSON(confChannel, ExchangePerilDirect, PauseKey, state);

  process.on("SIGINT", () => {
    console.log('Received SIGINT. Press CTRL C to terminate');
    conn.close();
  });

  const queueName = "game_logs";
  await declareAndBind(conn,ExchangePerilTopic ,queueName, "game_logs.*",SimpleQueueType.Durable)
  
  printServerHelp();
  while(true){
    const input = await getInput();
    if(input.length === 0) continue;
    switch(input[0]){
      case "pause":
        console.log("Pausing the game");
        try{
          await publishJSON(confChannel, ExchangePerilDirect, PauseKey, {isPaused: true});
        } catch(err){
            console.error("Game caught an error", err);
        }        
        break;
      case "resume":
          console.log("Resuming the game");
        try{
          await publishJSON(confChannel, ExchangePerilDirect, PauseKey, {isPaused: false});
        }catch(err){
          console.error("Game caught an error", err);
        }        
        break;
      case "quit":
        console.log("Exiting the game");
        process.exit(0);
      default:
        console.log("Command Not Understood. Please try again");
        break;
    }
  }
  
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});


  // while (true) {
  //   const words = await getInput();
  //   if (words.length === 0) continue;

  //   const command = words[0];
  //   if (command === "pause") {
  //     console.log("Publishing paused game state");
  //     try {
  //       await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
  //         isPaused: true,
  //       });
  //     } catch (err) {
  //       console.error("Error publishing pause message:", err);
  //     }
  //   } else if (command === "resume") {
  //     console.log("Publishing resumed game state");
  //     try {
  //       await publishJSON(publishCh, ExchangePerilDirect, PauseKey, {
  //         isPaused: false,
  //       });
  //     } catch (err) {
  //       console.error("Error publishing resume message:", err);
  //     }
  //   } else if (command === "quit") {
  //     console.log("Goodbye!");
  //     process.exit(0);
  //   } else {
  //     console.log("Unknown command");
  //   }