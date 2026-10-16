import amqp, { type Channel } from "amqplib";

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
  handler: (data: T)  => AckType,
  ): Promise<void>{
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

    ch.consume(queueName, (msg) =>{
    if(msg === null){
      return;
    } else{
      const msgStr = msg.content.toString();
      const parsedMsg = JSON.parse(msgStr);
      const ackType = handler(parsedMsg);
      switch(ackType){
        case AckType.Ack:
          console.log(ackType);
          ch.ack(msg);
          break;
        case AckType.NackRequeue:
          console.log(ackType);
          ch.nack(msg, false, true);
          break;
        case AckType.NackDiscard:
          console.log(ackType);
          ch.nack(msg, false, false);
          break;
      }
    }
    })
  }
