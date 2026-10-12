import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
    Durable,
    Transient,
}

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
    });

    await ch.bindQueue(queue.queue, exchange, key);
    return [ch, queue];
};