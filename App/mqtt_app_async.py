import asyncio
import logging
import os
import ssl
import aiomqtt

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(taskName)s] %(message)s",
    datefmt='%d/%m/%Y %H:%M:%S'
)

# Colas para enrutar mensajes por tópico
topic1_queue = asyncio.Queue()
topic2_queue = asyncio.Queue()

async def subscriber(client, topic):
    await client.subscribe(topic)
    logging.info(f"Suscripto a: {topic}")

async def topic1_consumer():
    while True:
        message = await topic1_queue.get()
        logging.info(f"{message.payload.decode()}")

async def topic2_consumer():
    while True:
        message = await topic2_queue.get()
        logging.info(f"{message.payload.decode()}")

async def message_distributor(client):
    async for message in client.messages:
        if message.topic.matches(os.getenv("TOPICO1")):
            topic1_queue.put_nowait(message)
            logging.info(f"Enviando a cola de {os.getenv("TOPICO1")}")
        elif message.topic.matches(os.getenv("TOPICO2")):
            topic2_queue.put_nowait(message)
            logging.info(f"Enviando a cola de {os.getenv("TOPICO2")}")

async def counter_incrementer(counter, lock):
    while True:
        await asyncio.sleep(3)
        async with lock:
            counter["value"] += 1
            logging.info(f"Contador: {counter['value']}")

async def counter_publisher(client, counter, lock, pub_topic):
    while True:
        await asyncio.sleep(5)
        async with lock:
            value = counter["value"]
        await client.publish(pub_topic, str(value))
        logging.info(f"Contador publicado: {value}")

async def main():
    broker = os.getenv("SERVIDOR")
    topic1 = os.getenv("TOPICO1")
    topic2 = os.getenv("TOPICO2")
    pub_topic = os.getenv("TOPICOPUB")

    counter = {"value": 0}
    lock = asyncio.Lock()

    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()

    async with aiomqtt.Client(
        broker,
        port=8883,
        tls_context=tls_context,
    ) as client:

        task_group = asyncio.TaskGroup()
        async with task_group:
            task_group.create_task(subscriber(client, topic1), name="sub1")
            task_group.create_task(subscriber(client, topic2), name="sub2")
            task_group.create_task(message_distributor(client), name="distribuidor")
            task_group.create_task(topic1_consumer(), name="temperatura")
            task_group.create_task(topic2_consumer(), name="humedad")
            task_group.create_task(counter_incrementer(counter, lock), name="incrementar")
            task_group.create_task(counter_publisher(client, counter, lock, pub_topic), name="pub1")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("main").info("Aplicación detenida por el usuario (Ctrl+C).")
