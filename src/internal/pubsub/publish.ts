import { encode } from "@msgpack/msgpack";
import type { ConfirmChannel } from "amqplib"

export function publishJSON<T>(
    ch: ConfirmChannel, exchange: string, routingKey: string, value: T
) {
    const bufferValue = Buffer.from(JSON.stringify(value));
    ch.publish(exchange, routingKey, bufferValue, {
        contentType: "application/json"
    });
}

export function publishMsgPack<T>(
    ch: ConfirmChannel, exchange: string, routingKey: string, value: T
) {
    const encodedMsg = encode(value);
    const bufferMsg = Buffer.from(encodedMsg.buffer);

    ch.publish(exchange, routingKey, bufferMsg, {
        contentType: "application/x-msgpack"
    });
};