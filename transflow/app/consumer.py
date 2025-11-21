import asyncio
import json
import aio_pika
from pathlib import Path
import os

from aio_pika import connect_robust, ExchangeType
from dotenv import load_dotenv

# Carrega .env local
if Path("app/.env.local").exists():
    load_dotenv("app/.env.local")
elif Path(".env.local").exists():
    load_dotenv(".env.local")
else:
    load_dotenv("app/.env")
    load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://admin:admin@rabbitmq:5672/")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "corridas_queue")
EXCHANGE_NAME = "transflow_exchange"

from database.mongo_client import get_mongo_collection
from database.redis_client import get_redis_client

mongo = get_mongo_collection()
redis = get_redis_client()


async def connect_with_retry(url, retries=30, delay=2):
    for attempt in range(1, retries + 1):
        try:
            conn = await connect_robust(url)
            print(f"✅ Conectado ao RabbitMQ (tentativa {attempt})")
            return conn
        except Exception as e:
            print(f"⚠️ Tentativa {attempt} falhou: {e}")
            await asyncio.sleep(delay)

    raise RuntimeError("❌ Não foi possível conectar ao RabbitMQ.")


async def handle_message(message: aio_pika.IncomingMessage):
    async with message.process():
        raw = message.body.decode()
        print(f"📩 Mensagem recebida RAW: {raw}")

        try:
            payload = json.loads(raw)
        except:
            print("❌ JSON inválido!")
            return

        data = payload["data"] if "data" in payload else payload
        print(f"📦 Dados da corrida: {data}")

        # --------------------------
        # SALVAR NO MONGO
        # --------------------------
        try:
            doc = dict(data)
            doc.pop("_id", None)
            mongo.insert_one(doc)
            print("💾 Corrida salva no MongoDB!")
        except Exception as e:
            print(f"❌ Erro MongoDB: {e}")

        # --------------------------
        # ATUALIZAR SALDO REDIS
        # --------------------------
        motorista_raw = data.get("motorista")

        motorista = None
        if isinstance(motorista_raw, dict):
            motorista = motorista_raw.get("nome")
        elif isinstance(motorista_raw, str):
            motorista = motorista_raw

        if motorista:
            motorista = motorista.lower()
        else:
            print("⚠️ Corrida sem nome de motorista — saldo não será atualizado.")
            return

        try:
            valor = float(data.get("valor_corrida", 0))
        except:
            valor = 0.0

        key = f"saldo:{motorista}"

        try:
            redis.incrbyfloat(key, valor)
            print(f"💰 Saldo atualizado de {motorista}: +{valor}")
        except:
            before = float(redis.get(key) or 0)
            redis.set(key, before + valor)
            print(f"💰 Saldo atualizado (fallback) de {motorista}: +{valor}")

        print("✅ Mensagem processada.\n")


async def main():
    print("🚀 Iniciando consumidor...")
    connection = await connect_with_retry(RABBITMQ_URL)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.FANOUT, durable=True)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange)

    print(f"🎯 Consumindo mensagens da fila '{QUEUE_NAME}'...")

    await queue.consume(handle_message)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
