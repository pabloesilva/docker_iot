import asyncio
import logging
import os
from aiomqtt import Client

# configuracion mensajes de logging 
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

async def subscribe_topic(client, topic, name):
    logger = logging.getLogger(name)
    async with client.messages() as messages:
        await client.subscribe(topic)
        async for message in messages:
            logger.info(f"Mensaje recibido en '{topic}': {message.payload.decode()}")

async def counter_incrementer(counter, lock):
    logger = logging.getLogger("incrementer")
    while True:
        await asyncio.sleep(3)
        async with lock:
            counter["value"] += 1
            logger.info(f"Contador incrementado: {counter['value']}")

async def counter_publisher(client, counter, lock, pub_topic):
    logger = logging.getLogger("publisher")
    while True:
        await asyncio.sleep(5)
        async with lock:
            value = counter["value"]
        await client.publish(pub_topic, str(value))
        logger.info(f"Contador publicado: {value}")

async def main():
    broker = os.getenv("SERVIDOR")
    topic1 = os.getenv("TOPICO1")
    topic2 = os.getenv("TOPICO2")
    pub_topic = os.getenv("TOPICOPUB")

    counter = {"value": 0}
    lock = asyncio.Lock()

    async with Client(broker, port=8883, tls_context=True) as client:
        await asyncio.gather(
            subscribe_topic(client, topic1, "SUB1"),
            subscribe_topic(client, topic2, "SUB2"),
            counter_incrementer(counter, lock),
            counter_publisher(client, counter, lock, pub_topic)
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("main").info("Aplicación detenida por el usuario (Ctrl+C).")
