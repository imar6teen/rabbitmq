#! python

import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))

channel = connection.channel()

channel.queue_declare(queue="task_queue", durable=True)

message = ' '.join(sys.argv[1:2])


for i in range(int(sys.argv[2])):
    dots = '.' * i
    load_message = message + dots
    channel.basic_publish(
        exchange="",
        routing_key="task_queue",
        body=load_message,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
    )
    print(f"[x] sent {load_message}")


connection.close()
