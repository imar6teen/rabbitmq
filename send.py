#! python

import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

channel = connection.channel()

channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

messages = [
    {
        "key": "message.hello",
        "value": "Halo host"
    },
    {
        "key": "rabbit.hello.message",
        "value": "Halo host ini dari rabbit"
    },
    {
        "key": "rabbit.close.message",
        "value": "Halo host ini pesan tutup dari rabbit"
    },
]

for message in messages:
    channel.basic_publish(
        exchange="topic_logs",
        routing_key=message['key'],
        body=message['value'],
    )
    print(f"[x] sent {message['value']}")

connection.close()
