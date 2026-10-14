import { decode } from "@msgpack/msgpack";
import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
};

export const declareAndBind = async (
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType
) : Promise<[Channel, amqp.Replies.AssertQueue]> => {
    const ch = await conn.createChannel();
    
    const queue = await ch.assertQueue(queueName, {
        durable: queueType === SimpleQueueType.Durable ? true : false,
        autoDelete: queueType === SimpleQueueType.Transient ? true : false,
        exclusive: queueType === SimpleQueueType.Transient ? true : false,
        arguments: {
            "x-dead-letter-exchange": "peril_dlx"
        }
    });

    await ch.bindQueue(queue.queue, exchange, key);
    return [ch, queue];
};

export async function subscribeJSON<T> (
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
) {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    await ch.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
        if (!msg) {
            return;
        }

        let data: T;
        try {
            data = JSON.parse(msg.content.toString());
        } catch (err) {
            console.log(err);
            return;
        }

        try {
            const acktype = await handler(data);
            switch (acktype) {
                case AckType.Ack: {
                    ch.ack(msg);
                    break;
                };
                case AckType.NackRequeue: {
                    ch.nack(msg, false, true);
                    break;
                };
                case AckType.NackDiscard: {
                    ch.nack(msg, false, false);
                    break;
                };
            };   
        } catch (err) {
            console.error(`error handling message: ${err}`);
            ch.nack(msg, false, false);
            return;
        }
    });
}

export async function subscribeMsgPack<T> (
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
) {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    await ch.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
        if (!msg) {
            return;
        }

        let data: T;
        try {
            data = decode(msg.content) as T;
        } catch (err) {
            console.log(err);
            return;
        }

        try {
            const acktype = await handler(data);
            switch (acktype) {
                case AckType.Ack: {
                    ch.ack(msg);
                    break;
                };
                case AckType.NackRequeue: {
                    ch.nack(msg, false, true);
                    break;
                };
                case AckType.NackDiscard: {
                    ch.nack(msg, false, false);
                    break;
                };
            };   
        } catch (err) {
            console.error(`error handling message: ${err}`);
            ch.nack(msg, false, false);
            return;
        }
    });
}