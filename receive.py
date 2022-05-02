#! python

from ast import ExceptHandler
import queue
import pika
import sys
import os
import time


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost"))

    channel = connection.channel()

    # channel.queue_declare("task_queue", durable=True)
    channel.exchange_declare(exchange="logs", exchange_type='fanout')

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(queue=queue_name, exchange='logs')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body.decode('utf-8'))
        time.sleep(body.count(b'.'))
        print(" [x] done")

    print("Connected to rabbitmq")

    # FAIR DISPATCH
    # Make rabbitmq send to other consumer when the consumer busy
    # rather than send to consumer fairly
    # channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=queue_name, auto_ack=True,
                          on_message_callback=callback)

    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('interuppted')
        try:
            sys.exit()
        except SystemExit:
            os._exit()
