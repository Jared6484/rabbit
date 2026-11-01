import amqp, { type Channel } from "amqplib";
import { resourceLimits } from "worker_threads";
import {decode} from "@msgpack/msgpack";

export enum AckType {
  Ack = "Ack",
  NackRequeue = "NackRequeue",
  NackDiscard = "NackDiscard",
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]>{

  const channel = await conn.createChannel();
  const q = await channel.assertQueue(queueName,{
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
    arguments: {"x-dead-letter-exchange":"peril_dlx"},
  });

  await channel.bindQueue(q.queue,exchange, key );
  return [channel, q];
};

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data) =>
    JSON.parse(data.toString()),
  );
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T)  => Promise<AckType> | AckType,
  ): Promise<void>{
    return subscribe(
      conn,
      exchange,
      queueName,
      key,
      queueType,
      handler,
      (data) => decode(data) as T,
    );
  }


export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  SimpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [ch, queue] = await declareAndBind(conn, exchange, queueName, routingKey, SimpleQueueType);

  await ch.prefetch(10);
  
  await ch.consume(queueName, async (msg: amqp.ConsumeMessage | null) =>{
    if(msg === null){
      return;
    } 
    
    let data: T;
    try{
      data = deserializer(msg.content);
    } catch(err){
      console.error("Could not decode message:", err);
      return;
    }

    try {
      const ackType = await handler(data);
      switch(ackType){
        case AckType.Ack:
          ch.ack(msg);
          break;
        case AckType.NackRequeue:
          ch.nack(msg, false, true);
          break;
        case AckType.NackDiscard:
          ch.nack(msg, false, false);
          break;
        default:
          const unreachable: never = ackType;
          console.error("Unexpected ack type", unreachable);
      } 
    }catch (err) {
      console.error("Error in handler", err);
      ch.nack(msg, false, false);
    }
  },
  {noAck: false},
  );
}
