import asyncio
import json
import os
from dotenv import load_dotenv
import aio_pika
from aio_pika import connect_robust, ExchangeType

if os.path.exists("app/.env.local"):
    load_dotenv("app/.env.local")
else:
    load_dotenv("app/.env")

RABBIT_URL = os.getenv('RABBITMQ_URL', "amqp://admin:admin@rabbitmq:5672/")
QUEUE = os.getenv('RABBITMQ_QUEUE', 'corridas_queue')
EXCHANGE_NAME = "transflow_exchange"


class FastStreamClient:
    def __init__(self, amqp_url, queue=QUEUE):
        self.amqp_url = amqp_url
        self.queue = queue
        self.connection = None
        self.channel = None

    async def connect(self):
        self.connection = await connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()
        await self.channel.declare_exchange(EXCHANGE_NAME, ExchangeType.FANOUT, durable=True)

    async def publish(self, message: dict):
        if self.connection is None:
            await self.connect()

        exchange = await self.channel.get_exchange(EXCHANGE_NAME)
        body = json.dumps(message).encode()

        await exchange.publish(aio_pika.Message(body=body), routing_key='')

    async def close(self):
        if self.connection:
            await self.connection.close()


async def publish_corrida_event(corrida: dict):
    payload = {"event": "corrida_finalizada", "data": corrida}
    client = FastStreamClient(RABBIT_URL, QUEUE)

    try:
        await client.publish(payload)
    finally:
        await client.close()
