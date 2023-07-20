import json
import pika
import uuid





rurl = "amqp://devsfactory:Devsfactory123@52.140.56.56:5672/vmmhost"


def send_mail(data: dict):
    """
        The function allows us for making a connection to
        the queue that is hosted and send the data to the Queue.
     """
    credentials = pika.URLParameters(rurl)
    connection = pika.BlockingConnection(credentials)
    channel = connection.channel()
    backend_exchange = 'mm_backend_exchange'
    """
        declaring an exchange of type fanout so that the
        data can be duplicated and sent to the 2 queues.
    """
    channel.exchange_declare(exchange=backend_exchange,
                             exchange_type='direct', durable=True)
    data = json.dumps(data)
    # publish the received data to the exchange.
    channel.basic_publish(exchange=backend_exchange, body=data, routing_key='mail_task_route',
                          properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
    channel.close()  # closes the channel.
    connection.close()  # closes the connection.


def add_user():
    # updates ComRegistration and UserCredentials tables.
    mail_data = dict(task="mm_addadmin",
                     data=dict(fw_tenant_id=1091, email='ravanasuran333@gmail.com',
                               role='tenant_user', by_email='shrishail', domain='machine_monotering', org_name='machine_monotering', HMT='shbcus8'))
    print(mail_data)
    send_mail(mail_data)
    print('good to go')
    return {'succesfully': "sent mail"}


def update_user():
    mail_data = dict(task="mm_updateduser",
                     data=dict(fw_tenant_id=1091, email='ravanasuran333@gmail.com',
                               role='account_user', by_email='ravanasuran333@gmail.com', domain='machine_monotering', org_name='machine_monotering'))
    send_mail(mail_data)
    print('good to go')
    return {'succesfully': "sent mail"}



def delete_user():
    mail_data = dict(task="mm_deleteduser", data={"fw_tenant_id": 1234, "email": 'ravanasuran333@gmail.com',
                                                   "by_email": 'ravanasuran333@gmail.com', 'org_name':'machine_monotering'})
    send_mail(mail_data)
    print('good to go')
    return {'succesfully': "sent mail"}


def otp():
    otp = uuid.uuid4().hex.lower()[0:8]
    try:
        mail_data = dict(task="fsf_reqpasmail",
                    data={"fw_tenant_id": 1423, "email": 'ravanasuran333@gmail.com', "otp": otp, 'org_name':'machine_monotering'})
    except:
        mail_data = dict(task="mm_req_otp", data={"fw_tenant_id": 0, "email": 'ravanasuran333@gmail.com', "otp": otp, 'org_name':'machine_monotering'})
    send_mail(mail_data)
    print('good to go')
    return {'succesfully': "sent mail"}


def activation():
    otp = uuid.uuid4().hex.lower()[0:8]
    mail_data = dict(task="mm_updateusercred",
                data={"fw_tenant_id": 1423, "email": 'ravanasuran333@gmail.com', 'org_name':'machine_monotering'})

    send_mail(mail_data)
    print('good to go')
    return {'succesfully': "sent mail"}

otp()