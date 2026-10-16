import amqp from "amqplib";
import { createConnection } from "net";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import {clientWelcome, commandStatus, getInput, printClientHelp, printQuit} from "../internal/gamelogic/gamelogic.js"
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove, handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handlerPause } from "./handlers.js";
import { handlerMove } from "./handlers.js";
import { channel } from "diagnostics_channel";

async function main() {
  const userName = await clientWelcome();
  console.log("Starting Peril client...");
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  if(conn){
    console.log("Connection Successful");
  }

  const confChannel = await conn.createConfirmChannel();

  process.on("SIGINT", () => {
   console.log('Received SIGINT. Press CTL C to terminate' );
   conn.close();
  });

  const queueName = `${PauseKey}.${userName}`
  //await declareAndBind(conn, ExchangePerilDirect, queueName, PauseKey, SimpleQueueType.Transient);

  const gameState: GameState = new GameState("username"); // don't really need the annotation :GameState, but can still do it.
  await subscribeJSON(
    conn, 
    ExchangePerilDirect, 
    `pause.${userName}`, 
    PauseKey, 
    SimpleQueueType.Transient, 
    handlerPause(gameState));
  
  await subscribeJSON(
    conn,
    ExchangePerilTopic, 
    `army_moves.${userName}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gameState));

  while(true){
    
    const input = await getInput();

    switch(input[0]){
      case "spawn":
        try{
          commandSpawn(gameState, input);
        } catch(err){
          console.error(`Error caught: ${err} `);
          continue;
        };
        break;

      case "move":
        try{
          let move = commandMove(gameState, input);
          publishJSON(confChannel, ExchangePerilTopic, `army_moves.${userName}`, move);
          console.log("Move completed successfully");
        } catch(err){
          console.error(`Error caught: ${err}`);
          continue;
        };
        break;

      case "status":
        commandStatus(gameState);
        break;

      case "help":
        printClientHelp();
        break;

      case "spam":
        console.log("Spamming not allowed yet!");
        break;

      case "quit":
        printQuit();
        break;

      default: 
        console.log("Unknown Command");
        break;

    };
  }

};


main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

// Back in the src/client/index.ts file, use these parameters to call declareAndBind:
// exchange: ExchangePerilDirect (from src/internal/routing/routing.ts)
// queueName: pause.<username> where <username> is the user's input. Routing key constants can be found in src/internal/routing/routing.ts.
// routingKey: PauseKey (from src/internal/routing/routing.ts)
// queueType: SimpleQueueType.Transient
// Run the client with npm run client. Enter suntzu (all lowercase) as the username.
// It will print out some nonsense about possible commands, but you can ignore that. You can't run them yet because we haven't implemented them.
// Check the RabbitMQ management UI to see if the queue was created and bound to the exchange.
// Close the client. If all goes well, the queue should automatically be deleted after a few seconds.
// Run the client as suntzu again, making sure the queue is recreated.