import os
import pika
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        self.m_binding_key = binding_key
        self.m_queue_name = queue_name
        self.m_exchange_name = exchange_name

        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
         # Establish Channel
        self.channel = self.connection.channel()
        # Create Queue if not already present
        self.channel.queue_declare(queue=self.m_queue_name)
        # Create the topic exchange if not already present
        self. channel.exchange_declare(
            self.m_exchange_name, exchange_type="topic"
        )
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue= self.m_queue_name,
            routing_key= self.m_binding_key,
            exchange= self.m_exchange_name,
        )
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            self.m_queue_name, self.on_message_callback, auto_ack=False
        )
        
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)
        #Print message
        print(f" [x] Received Message: {body}")

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.channel.start_consuming()
    