import json
import pika
from pika import exceptions
from semdmail import SendEmail
import os


file_path = os.path.join(os.path.dirname(__file__), 'backendcred.json')
# with open('backendcred.json', "r") as cred:
dlog_cred = json.load(open(file_path))

mail_path = os.path.join(os.path.dirname(__file__), 'mailformat.json')
mail_format = json.load(open(mail_path))

qurl = dlog_cred.get("RABBIT_MQ_CRED").get("QUEUE_PROCESS")
backend_exchange = "mm_backend_exchange"


def _queue_connector(type_, queue_route, key):
    credentials = pika.URLParameters(qurl)
    connection = pika.BlockingConnection(credentials)
    channel = connection.channel()
    channel.confirm_delivery()
    channel.exchange_declare(exchange=backend_exchange, exchange_type=type_)
    channel.queue_declare(queue=queue_route, durable=True)
    channel.queue_bind(exchange=backend_exchange, queue=queue_route, routing_key=key)
    return channel


def mail_receiver():
    type_ = "direct"
    queue = "mm_mail_task"
    key = "mail_task_route"

    try:
        channel = _queue_connector(type_, queue, key)
        def callback(ch, method, properties, body):
            try:
                data = json.loads(body.decode("utf-8"))
                task = data.get("task")
                data = data.get("data")
                data["sf_event_code"] = task

                if task and data:
                    execution = mail_task(task, data)

                    if execution:
                        channel.basic_ack(method.delivery_tag)

            except Exception as error_info:
                return {"unsuccesfull": error_info}

        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
        channel.start_consuming()
        raise pika.exceptions.AMQPConnectionError

    except pika.exceptions.AMQPConnectionError as error_info:
        return {"unsuccesfull":error_info}

    except Exception as error_info:
        return error_info

# mm_requestOTP
def mail_task(task: str, data: dict):
    mail_tasks = {
        'mm_addadmin': mm_addeduser,
        'mm_updateduser': mm_updateduser,
        'mm_deleteduser': mm_deleteduser,
        'mm_updateusercred': mm_updateCred,
        'mm_req_otp': mm_req_otp,
        'mm_updatetopro': mm_updatepro,
        'mm_user_regestration': mm_user_regestration,
        'mm_resetPassword': mm_req_changepwd
    }
    res = mail_tasks[task](data)

    return True


def mm_addeduser(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_addeduser(data.get('fw_tenant_id'), data.get('by_email'), data.get('role'),data.get('HMI'), data.get('domain'),
                          data.get('email'), data.get('password'), data.get('org_name'))
    print('popij')
    process.send()
    print('mail delivered')
    return "Machine Monitoring: Added {} as a user".format(data.get('email'))


def mm_updateduser(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_updateduser(data.get('fw_tenant_id'), data.get('by_email'), data.get('role'), data.get('domain'), data.get('org_name'))
    process.send()
    return "Machine Monitoring: Updated {} as a user".format(data.get('email'))


def mm_deleteduser(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_deleteduser(data.get('fw_tenant_id'), data.get('by_email'),data.get('org_name'))
    process.send()
    return "Machine Monitoring: Deleted {} as a user".format(data.get('email'))


def mm_updatepro(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_updatepro(data.get('fw_tenant_id'), data.get('email'), data.get('domain'), data.get('org_name'))
    process.send()
    return "Machine Monitoring: Profile is updated for {}".format(data.get('email'))


def mm_updateCred(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_updateCred(data.get('fw_tenant_id'),data.get('org_name'))
    process.send()
    return "Machine Monitoring: Registered {} as a user".format(data.get('email'),data.get('org_name'))


def mm_req_otp(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_req_otp(data.get('fw_tenant_id'), data.get('email'), data.get('otp'),data.get('org_name'))
    process.send()
    return "Machine Monitoring: Requested OTP for {}".format(data.get('email'))


def mm_user_regestration(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_new_register(data.get('HML'), data.get('link'))
    process.send()
    return "Registration mail has sent to {}".format(data.get('email'))


def mm_req_changepwd(data: dict):
    process = SendEmail(to=data.get('email'))
    process.mm_req_changepwd(data.get('fw_tenant_id'), data.get('email'), data.get('domain'), data.get('org_name'))
    process.send()
    return "Machine Monitoring: Successfully updated password for {}".format(data.get('email'))




# def update_user():
#     mail_data = dict(task="mm_updateduser",
#                      data=dict(fw_tenant_id=1091, email='ravanasuran333@gmail.com',
#                                role='account_user', by_email='ravanasuran333@gmail.com', domain='machine_monotering', org_name='machine_monotering'))
#     send_mail(mail_data)
#     print('good to go')
#     return {'succesfully': "sent mail"}
#
#
#
# def delete_user():
#     mail_data = dict(task="mm_deleteduser", data={"fw_tenant_id": 1234, "email": 'ravanasuran333@gmail.com',
#                                                    "by_email": 'ravanasuran333@gmail.com', 'org_name':'machine_monotering'})
#     send_mail(mail_data)
#     print('good to go')
#     return {'succesfully': "sent mail"}
#
#
# def otp():
#     otp = uuid.uuid4().hex.lower()[0:8]
#     try:
#         mail_data = dict(task="mm_req_otp",
#                     data={"fw_tenant_id": 1423, "email": 'ravanasuran333@gmail.com', "otp": otp, 'org_name':'machine_monotering'})
#     except:
#         mail_data = dict(task="mm_req_otp", data={"fw_tenant_id": 0, "email": 'ravanasuran333@gmail.com', "otp": otp, 'org_name':'machine_monotering'})
#     send_mail(mail_data)
#     print('good to go')
#     return {'succesfully': "sent mail"}
