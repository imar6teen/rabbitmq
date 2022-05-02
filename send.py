#! python

import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

channel = connection.channel()

# channel.queue_declare(queue="task_queue", durable=True)

channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

severities = [
    {
        'sev': 'info',
        'msg': 'ini hanya info'
    },
    {
        'sev': 'warning',
        'msg': 'ini warning yaa.'
    },
    {
        'sev': 'dead',
        'msg': 'u r ded lol'
    }
]

for severity in severities:
    channel.basic_publish(
        exchange="direct_logs",
        routing_key=severity['sev'],
        body=severity['msg'],
    )
    print(f"[x] sent {severity['msg']}")


connection.close()
