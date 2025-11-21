
import asyncio
import aio_pika
import json

class FastStreamClient:
    def __init__(self, amqp_url, queue='corridas_queue'):
        self.amqp_url = amqp_url
        self.queue = queue
        self._connection = None
        self._channel = None

    async def _connect(self):
        if self._connection and not self._connection.is_closed:
            return
        self._connection = await connect_with_retry(self.amqp_url)

        self._channel = await self._connection.channel()

    async def publish(self, message: dict):
        await self._connect()
        exchange = await self._channel.declare_exchange('transflow_exchange', aio_pika.ExchangeType.FANOUT)
        body = json.dumps(message).encode()
        await exchange.publish(aio_pika.Message(body=body), routing_key='')

    async def consume(self, callback):
        await self._connect()
        exchange = await self._channel.declare_exchange('transflow_exchange', aio_pika.ExchangeType.FANOUT)
        queue = await self._channel.declare_queue(self.queue, durable=True)
        await queue.bind(exchange)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    await callback(message.body)
