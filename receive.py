#! python
import pika
import sys
import os

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))


def main():

    channel = connection.channel()

    channel.exchange_declare('topic_logs', 'topic')

    queue = channel.queue_declare('', exclusive=True)

    queue_name = queue.method.queue

    if(len(sys.argv) > 1):
        for topic_key in sys.argv[1:]:
            channel.queue_bind(
                queue=queue_name, exchange='topic_logs', routing_key=topic_key)
            print(f"{queue_name} bound to topic_logs with routing key {topic_key}")
    else:
        print("you need to specify at least one topic key")
        sys.exit(1)

    def callback(ch, method, properties, body):
        print(
            f" [x] Received {body.decode('utf-8')} from {method.routing_key}")

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Closing connection")
        connection.close()
        print("Connection closed")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
