import json
import pika
import uuid
from datetime import datetime, timedelta
rurl = "amqp://devsfactory:Devsfactory123@20.235.25.154:5672/vmmhost"


def send_mail(data: dict):
    """
        The function allows us for making a connection to
        the queue that is hosted and send the data to the Queue.
     """
    credentials = pika.URLParameters(rurl)
    connection = pika.BlockingConnection(credentials)
    channel = connection.channel()
    backend_exchange = 'mm_dlog_exchange'
    """
        declaring an exchange of type fanout so that the
        data can be duplicated and sent to the 2 queues.
    """
    channel.exchange_declare(exchange=backend_exchange,
                             exchange_type='direct', durable=True)
    data = json.dumps(data)
    # publish the received data to the exchange.
    channel.basic_publish(exchange=backend_exchange, body=data, routing_key='mm_dlog',
                          properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
    channel.close()  # closes the channel.
    connection.close()  # closes the connection.


def add_dlog():
    # updates ComRegistration and UserCredentials tables.
    mail_data = {"task": "dlog_process",
                 "data": {"org_id": 1059, "tenant_id": 4339, "application_id": 1703, "gateway_id": 2136, "client_id": "1059-1684-1703-2136", "data_received_time": '2023-07-06 22:00:00' , "created_by": "motor",
                 "payload": '{"asset_status": "up", "cycle_time": 4 , "product_code":"biscket", "event_code":666, "quantity_produced":5 }', "tag": "production", "dlog_uuid": "Bgtr7687ijo0pppi9u8", "source": "Simulator", "is_gtw": True, "dlog_id": "fizo125"},
                 "_metadata": {"tenant_type": "Enterprise"}}
    send_mail(mail_data)
    print('good to go')
    return {'succesfully': "sent mail"}


add_dlog()