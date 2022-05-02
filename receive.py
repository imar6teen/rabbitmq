#! python
import pika
import sys
import os
import time

# ! READ THIS! u have a problem before about acknowledgement
# ! in case u forgot, the root cause is the queue is exclusive
# ! so the messages inside the queue are not durable anymore
# ! the queue will be deleted after the host disconnect (exclusive=True)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost"))

    channel = connection.channel()

    # channel.queue_declare("task_queue", durable=True)
    channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

    result = channel.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue

    severities = sys.argv[1:]

    for severity in severities:
        channel.queue_bind(
            queue=queue_name, exchange='direct_logs', routing_key=severity)

    def callback(ch, method, properties, body):
        time.sleep(2)
        print(f" [x] Received {method.routing_key} : {body.decode('utf-8')}")
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
