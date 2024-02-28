import os
import pika

from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            self.exchange_name,
            exchange_type="topic"
        )

    def publishOrder(self, message: str) -> None:
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )
        print(f"[x] Sent Orders")
    
    def __del__(self) -> None:
        print(f"Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()