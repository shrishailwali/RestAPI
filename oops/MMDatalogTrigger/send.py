import json
import smtplib
from email.message import EmailMessage
import os
import pika
import logging

file_path = os.path.join(os.path.dirname(__file__), 'backendcred.json')
dlog_cred = json.load(open(file_path))


qurl = dlog_cred.get("RABBIT_MQ_CRED").get("QUEUE_PROCESS")
backend_exchange = "mm_dlog_exchange"

# Create a logger instance
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)  # Set the logging level to ERROR or any other desired level

# Create a file handler to save logs to a file
file_handler = logging.FileHandler('error.log')
file_handler.setLevel(logging.ERROR)

# Create a formatter for the log messages
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)


def send_event(data):
    """
        The function allows us for making a connection to
        the queue that is hosted and send the data to the Queue.
     """
    try:
        credentials = pika.URLParameters(qurl)
        connection = pika.BlockingConnection(credentials)
        channel = connection.channel()
        backend_exchange = 'mm_dlog_exchange'
        print('polokokook@@@@@@@@@@@@@@@@@@@@@@')
        data = json.dumps(data)
        print('9999999999999999999')
        # publish the received data to the exchange.
        channel.basic_publish(exchange=backend_exchange, body=data, routing_key='mm_event_sequence',
                              properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
        print('loki******************')
        channel.close()  # closes the channel.
        connection.close()  # closes the connection.
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {send_event.__name__}: {error_info}")
        return error_info

