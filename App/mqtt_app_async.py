import asyncio
import logging
import os
import ssl
import aiomqtt 

# configuracion mensajes de logging 
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt='%d/%m/%Y %H:%M:%S')

async def message_dispatcher(client, topics):
    logger = logging.getLogger("subs")
    for topic in topics:
        await client.subscribe(topic)
        logger.info(f"Suscripto a: {topic}")
    
    async for message in client.messages:
        logger.info(f"Mensaje en '{message.topic}': {message.payload.decode()}")

async def counter_incrementer(counter, lock):
    logger = logging.getLogger("incrementar")
    while True:
        await asyncio.sleep(3)
        async with lock:
            counter["value"] += 1
            logger.info(f"Contador: {counter['value']}")

async def counter_publisher(client, counter, lock, pub_topic):
    logger = logging.getLogger("pubs")
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

    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()

    async with aiomqtt.Client(
        os.environ['SERVIDOR'],
        port=8883,
        tls_context=tls_context,
    ) as client:
        await asyncio.gather(
        message_dispatcher(client, [topic1, topic2]),
        counter_incrementer(counter, lock),
        counter_publisher(client, counter, lock, pub_topic)
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("main").info("Aplicación detenida por el usuario (Ctrl+C).")
