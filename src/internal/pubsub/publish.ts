import { appendFile } from "fs";
import { type ConfirmChannel } from "amqplib";
import {encode} from "@msgpack/msgpack";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void>{
    const str_value = JSON.stringify(value);
    const byte_value = Buffer.from(str_value);
    return new Promise((resolve, reject) =>{ 
        ch.publish(
            exchange, 
            routingKey, 
            byte_value, 
            {contentType: "application/json"},
            (err) => {
                if(err != null){
                    reject(new Error("Message was NACKed by the broker"));
                } else {
                    resolve();
                }
            },
        )
    })
}

export function publishMsgPack<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const encoded: Uint8Array = encode(value);
    const buffer = Buffer.from(encoded);
    return new Promise((resolve, reject) =>{ 
        ch.publish(
            exchange, 
            routingKey, 
            buffer,
            {contentType: "application/x-msgpack"},
            (err) => {
                if(err != null){
                    reject(new Error("Message was NACKed by the broker"));
                } else {
                    resolve();
                }
            },
        )
    })
}
