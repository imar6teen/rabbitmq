#! python

import pika
import sys
import os
import time


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost"))

    channel = connection.channel()

    channel.queue_declare("task_queue", durable=True)

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body.decode('utf-8'))
        time.sleep(body.count(b'.'))
        time.sleep(int(sys.argv[1]) if sys.argv[1] else 0)
        print(" [x] done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    print("Connected to rabbitmq")

    # FAIR DISPATCH
    # Make rabbitmq send to other consumer when the consumer busy
    # rather than send to consumer fairly
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue="task_queue", auto_ack=False,
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
