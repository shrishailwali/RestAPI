import json
import pika
import psycopg2
from datetime import datetime, timedelta
from send import send_event, logger
import os

file_path = os.path.join(os.path.dirname(__file__), 'backendcred.json')
dlog_cred = json.load(open(file_path))

qurl = dlog_cred.get("RABBIT_MQ_CRED").get("QUEUE_PROCESS")
backend_exchange = "mm_dlog_exchange"

Dburl = dlog_cred.get("DB_CRED_SFACTORY").get("SF_DB_URL")


def dlog_receiver():
    type_ = "direct"
    queue = "mm_datalog"
    key = "mm_dlog"
    try:
        credentials = pika.URLParameters(qurl)
        with pika.BlockingConnection(credentials) as connection:
            channel = connection.channel()
            channel.confirm_delivery()
            queues = channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(exchange=backend_exchange, queue=queue, routing_key=key)
            queue_msg_count = queues.method.message_count
            print('la')
            if queue_msg_count:
                print('okolokl')
                for method_frame, properties, body in channel.consume(queue):
                    if body != '':
                        data = json.loads(body.decode('utf-8'))
                        print(data)
                        task = data.get('task')
                        payload = data.get('data')
                        data['event_code'] = task
                        print('chill bill')
                        if task and payload:
                            print(task, payload)
                            print('check')
                            if payload['tag'] == 'production' and data['task'] == "dlog_process":
                                print('chelo')
                                conn = psycopg2.connect(Dburl)
                                cursor = conn.cursor()
                                cursor.execute(f'''select sf_asset_id, sf_plant_id from sf_asset where sf_asset_eui = '{payload['created_by']}' and
                                     fw_tenant_id = {payload['tenant_id']} and is_active_data = True order by sf_asset_id desc;''')
                                asset_info = cursor.fetchone()
                                print('1')
                                if asset_info:
                                    cursor.execute(f'''select sf_asset_id, sf_plant_id from sf_asset where sf_asset_eui = '{payload['created_by']}' and
                                                                         fw_tenant_id = {payload['tenant_id']} and is_active_data = True order by sf_asset_id desc;''')
                                    asset_info = cursor.fetchone()
                                    cursor.execute(f'''INSERT INTO public.mm_datalog(fw_tenant_id, fw_org_id, fw_gateway_id, fw_application_id, 
                                    sf_asset_id, device_eui_id, fw_dlog_uuid, mm_payload_attribute, source, data_recived_time, is_active, created_by, created_bytime)
                                    VALUES ({payload['tenant_id']}, {payload['org_id']},{payload['gateway_id']}, {payload['application_id']},
                                    {asset_info[0]},'{payload['created_by']}', '{payload['dlog_uuid']}', '{payload['payload']}', '{payload['source']}', 
                                    '{payload['data_received_time']}', True, '{payload['created_by']}', now()::timestamp) ;''')
                                    print('2')
                                    attribute_info = eval(payload.get('payload'))
                                    attribute_info['fw_tenant_id'] = payload.get('tenant_id')
                                    attribute_info['fw_eui_id'] = payload.get('created_by')
                                    attribute_info['fw_application_id'] = payload.get('application_id')
                                    attribute_info['data_received_time'] = payload.get('data_received_time')
                                    attribute_info['sf_asset_id'] = asset_info[0]
                                    attribute_info['sf_plant_id'] = asset_info[1]
                                    attribute_info['task'] = 'event_log'
                                    send_event({"data": attribute_info, "task": "event_log"})
                                    print('********************************')
                                    channel.basic_ack(method_frame.delivery_tag)
                                    print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
                                else:
                                    print('9')
                                    channel.basic_ack(method_frame.delivery_tag)
                                conn.commit()
                                conn.close()
                            else:
                                channel.basic_ack(method_frame.delivery_tag)
                        if method_frame.delivery_tag == queue_msg_count:
                            break
                        else:
                            channel.basic_ack(method_frame.delivery_tag)
            channel.close()
            connection.close()
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {dlog_receiver.__name__}: {error_info}")
        ack(type_, queue, key)
        return error_info


def ack(type_, queue, key):
    credentials = pika.URLParameters(qurl)
    with pika.BlockingConnection(credentials) as connection:
        channel = connection.channel()
        channel.confirm_delivery()
        queues = channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(exchange=backend_exchange, queue=queue, routing_key=key)
        queue_msg_count = queues.method.message_count
        if queue_msg_count:
            for method_frame, properties, body in channel.consume(queue):
                channel.basic_ack(method_frame.delivery_tag)
    channel.close()
    connection.close()


def event_receiver():
    type_ = "direct"
    queue = "mm_event_sequence"
    key = "mm_event_sequence"
    try:
        credentials = pika.URLParameters(qurl)
        with pika.BlockingConnection(credentials) as connection:
            channel = connection.channel()
            channel.confirm_delivery()
            queues = channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(exchange=backend_exchange, queue=queue, routing_key=key)
            queue_msg_count = queues.method.message_count
            if queue_msg_count:
                for method_frame, properties, body in channel.consume(queue):
                    if body != '':
                        data = json.loads(body.decode('utf-8'))
                        task = data.get('task')
                        payload = data.get('data')
                        if task and payload:
                            if data['task'] == "event_log" and payload['event_code']:
                                conn = psycopg2.connect(Dburl)
                                cursor = conn.cursor()
                                cursor.execute(f'''select mm_event_starttime, mm_event_endtime,mm_asset_status, mm_downtime_reason_code, mm_fault_reason_code
                                 from mm_eventSequence where fw_eui_id ='{payload['fw_eui_id']}' and fw_tenant_id = {payload['fw_tenant_id']}
                                               and sf_asset_id = {payload['sf_asset_id']} and mm_event_date <='{payload['data_received_time']}' order by mm_eventSequence_id desc limit 1;''')
                                event_info = cursor.fetchone()
                                print(event_info)
                                execute = event_log(event_info,payload, conn)
                                print('lokifgukhijokpjljklpjllokjhklopojlk7890-', execute)
                                conn.commit()
                                conn.close()
                                if execute:
                                    channel.basic_ack(method_frame.delivery_tag)
                                else:
                                    channel.basic_ack(method_frame.delivery_tag)
                            else:
                                channel.basic_ack(method_frame.delivery_tag)
                        if method_frame.delivery_tag == queue_msg_count:
                            break
                    else:
                        channel.basic_ack(method_frame.delivery_tag)
            channel.close()
            connection.close()
    except Exception as error_info:
        ack(type_, queue, key)
        logger.error(f"An error occurred in {__file__}, function {event_receiver.__name__}: {error_info}")
        return error_info


def event_log(event_info, payload, conn):
    try:
        cursor = conn.cursor()
        downtime_reason = downtime(payload.get('downtime_reason_barcode'), conn) if payload.get('downtime_reason_barcode') else None
        fault_reason = fault(payload.get('fault_reason_barcode'), conn) if payload.get('fault_reason_barcode') else None
        rejection_re = rejection_reason(payload.get('rejection_reason_barcode'), conn) if payload.get('rejection_reason_barcode') else None
        if event_info:
            asset_pre_status = event_info[2]
            quantity_produced = payload.get('quantity_produced') if payload.get('quantity_produced') else 0
            quantity_rejected = payload.get('quantity_rejected') if payload.get('quantity_rejected') else 0
            payload['previous_downtime_reason_code'] = event_info[3]
            payload['previous_fault_reason_code'] = event_info[4]
            payload['asset_previous_status'] = asset_pre_status
            payload['asset_previous_stime'] = event_info[1] if event_info[1] else event_info[0]
            payload['rejection_reson_id'] = rejection_reason[0] if rejection_re else None
            payload['rejection_reson_code'] = rejection_reason[1] if rejection_re else None
            downtime_reason_code = downtime_reason[1] if downtime_reason else None
            downtime_reason_id = downtime_reason[0] if downtime_reason else None
            fault_reason_code = fault_reason[1] if fault_reason else None
            fault_reason_id = fault_reason[0] if fault_reason else None
            cursor.execute(f''' select timezone(case when sf_plant_tzone is not null then sf_plant_tzone else 'UTC' end, timezone('UTC','{payload['data_received_time']}'::timestamp))
                            from sf_plant where fw_tenant_id = {payload['fw_tenant_id']}and sf_plant_id = {payload['sf_plant_id']}''')
            converted_time = cursor.fetchone()
            print(converted_time)
            print(payload['asset_previous_stime'])
            payload['tz_start_time'] = payload['asset_previous_stime']
            payload['tz_end_time'] = converted_time[0]
            print('in', payload['tz_start_time'].date()== payload['tz_end_time'].date(), type(payload['tz_start_time']), type(payload['tz_end_time']))
            if asset_pre_status == 'up':
                print('up')
                if payload.get('asset_status') == 'down':
                    print('in',payload['tz_start_time'].date(), payload['tz_end_time'].date())
                    payload['quantity_produced'] = 0
                    payload['quantity_rejected'] = 0
                    if payload['tz_start_time'].date() == payload['tz_end_time'].date():
                        payload['start_time'] = payload['tz_start_time'] if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['asset_status'] = 'up'
                        payload['quantity_produced'] = quantity_produced
                        payload['quantity_rejected'] = quantity_rejected
                        payload['asset_runtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    else:
                        payload['quantity_produced'] = None
                        payload['quantity_rejected'] = None
                        payload['asset_status'] = 'up'
                        payload['start_time'] = payload['tz_start_time'] if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['start_time'].replace(hour=0, minute=0, second=0)
                        payload['asset_runtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                        payload['start_time'] = payload['tz_start_time'].replace(hour=0, minute=0, second=0) + timedelta(days=1) if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['tz_end_time']
                        payload['asset_runtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                    payload['end_time'] = None
                    payload['asset_status'] = 'down'
                    payload['quantity_produced'] = 0
                    payload['quantity_rejected'] = 0
                    payload['downtime_reason_code'] = downtime_reason[1] if downtime_reason else None
                    payload['downtime_reason_id'] = downtime_reason[0] if downtime_reason else None
                    payload['fault_reason_code'] = fault_reason[1] if fault_reason else None
                    payload['fault_reason_id'] = fault_reason[0] if fault_reason else None
                    payload['asset_runtime'] = 0
                    eventSequence = insert_eventSequence(payload, conn)
                    if quantity_produced or quantity_rejected:
                        payload['quantity_produced'] = quantity_produced
                        payload['quantity_rejected'] = quantity_rejected
                        payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(
                            payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['end_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(
                            payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['asset_runtime'] = 0
                        payload['asset_downtime'] = 0
                        eventSequence = insert_eventSequence(payload, conn)
                    return True if eventSequence else False
                elif payload.get('asset_status') == 'up':
                    print('down')
                    payload['quantity_produced'] = 0
                    payload['quantity_rejected'] = 0
                    if payload['tz_start_time'].date() == payload['tz_end_time'].date():
                        payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(
                        payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['end_time'] = None
                        payload['asset_status'] = 'up'
                        payload['asset_runtime'] = (payload['start_time'] - payload['tz_start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    else:
                        payload['quantity_produced'] = None
                        payload['quantity_rejected'] = None
                        payload['asset_status'] = 'up'
                        payload['start_time'] = payload['tz_start_time'] if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['start_time'].replace(hour=0, minute=0, second=0) + timedelta(days=1)
                        payload['asset_runtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                        payload['start_time'] = payload['tz_start_time'].replace(hour=0, minute=0,second=0) + timedelta(days=1) if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['tz_end_time']
                        payload['asset_runtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    if quantity_produced or quantity_rejected:
                        payload['quantity_produced'] = quantity_produced
                        payload['quantity_rejected'] = quantity_rejected
                        payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['end_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['asset_runtime'] = 0
                        payload['asset_downtime'] =0
                        eventSequence = insert_eventSequence(payload, conn)
                    return True if eventSequence else False

            elif asset_pre_status == 'down':
                print('down)')
                if payload.get('asset_status') == 'down':
                    print('chill')
                    payload['quantity_produced'] = 0
                    print('kill')
                    payload['quantity_rejected'] = 0
                    print('in')
                    payload['asset_status'] = 'down'
                    payload['downtime_reason_code'] = downtime_reason[1] if downtime_reason else None
                    payload['downtime_reason_id'] = downtime_reason[0] if downtime_reason else None
                    payload['fault_reason_code'] = fault_reason[1] if fault_reason else None
                    payload['fault_reason_id'] = fault_reason[0] if fault_reason else None
                    payload['asset_runtime'] = 0
                    if payload['tz_start_time'].date() == payload['tz_end_time'].date():
                        payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['end_time'] = None
                        payload['asset_downtime'] = (payload['start_time'] - payload['tz_start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    else:
                        payload['quantity_produced'] = 0
                        payload['quantity_rejected'] = 0
                        payload['downtime_reason_code'] = None
                        payload['downtime_reason_id'] = None
                        payload['fault_reason_code'] = None
                        payload['fault_reason_id'] = None
                        payload['start_time'] = payload['tz_start_time'] if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['start_time'].replace(hour=0, minute=0, second=0) + timedelta(days=1)
                        payload['asset_downtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                        payload['downtime_reason_code'] = downtime_reason_code
                        payload['downtime_reason_id'] = downtime_reason_id
                        payload['fault_reason_code'] = fault_reason_code
                        payload['fault_reason_id'] = fault_reason_id
                        payload['start_time'] = payload['tz_start_time'].replace(hour=0, minute=0,second=0) + timedelta(days=1) if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['tz_end_time']
                        payload['asset_downtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    if quantity_produced or quantity_rejected:
                        payload['quantity_produced'] = quantity_produced
                        payload['quantity_rejected'] = quantity_rejected
                        payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['end_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['asset_runtime'] = 0
                        payload['asset_downtime'] =0
                        eventSequence = insert_eventSequence(payload, conn)
                    return True if eventSequence else False
                elif payload.get('asset_status') == 'up':
                    print('up')
                    payload['quantity_produced'] = 0
                    payload['quantity_rejected'] = 0
                    payload['asset_status'] = 'down'
                    payload['downtime_reason_code'] = downtime_reason[1] if downtime_reason else None
                    payload['downtime_reason_id'] = downtime_reason[0] if downtime_reason else None
                    payload['fault_reason_code'] = fault_reason[1] if fault_reason else None
                    payload['fault_reason_id'] = fault_reason[0] if fault_reason else None
                    payload['asset_runtime'] = 0
                    print('chill')
                    if payload['tz_start_time'].date() == payload['tz_end_time'].date():
                        print('queue')
                        payload['start_time'] = payload['tz_start_time'] if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['asset_downtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    else:
                        payload['downtime_reason_code'] = None
                        payload['downtime_reason_id'] = None
                        payload['fault_reason_code'] = None
                        payload['fault_reason_id'] = None
                        payload['start_time'] = payload['tz_start_time'] if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['start_time'].replace(hour=0, minute=0, second=0) + timedelta(days=1)
                        payload['asset_downtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                        payload['downtime_reason_code'] = downtime_reason_code
                        payload['downtime_reason_id'] = downtime_reason_id
                        payload['fault_reason_code'] = fault_reason_code
                        payload['fault_reason_id'] = fault_reason_id
                        payload['start_time'] = payload['tz_start_time'].replace(hour=0, minute=0,second=0) + timedelta(days=1) if payload['tz_start_time'] else event_info[0]
                        payload['end_time'] = payload['tz_end_time']
                        payload['asset_downtime'] = (payload['end_time'] - payload['start_time']).total_seconds() / 60
                        eventSequence = insert_eventSequence(payload, conn)
                    payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                    payload['end_time'] = None
                    payload['asset_status'] = 'up'
                    payload['asset_runtime'] = 0
                    payload['asset_downtime'] = 0
                    eventSequence = insert_eventSequence(payload, conn)
                    if quantity_produced or quantity_rejected:
                        payload['quantity_produced'] = quantity_produced
                        payload['quantity_rejected'] = quantity_rejected
                        payload['start_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['end_time'] = payload['tz_end_time'] if payload['tz_end_time'] else datetime.strptime(payload['data_received_time'], "%Y-%m-%d %H:%M:%S.%f")
                        payload['asset_runtime'] = 0
                        payload['asset_downtime'] =0
                        eventSequence = insert_eventSequence(payload, conn)
                    return True if eventSequence else False

        else:
            print('oki')
            cursor.execute(f''' select timezone(case when sf_plant_tzone is not null then sf_plant_tzone else 'UTC' end, timezone('UTC','{payload['data_received_time']}'))::timestamp
                from sf_plant where fw_tenant_id = {payload['fw_tenant_id']} and sf_plant_id = {payload['sf_plant_id']}''')
            converted_time =cursor.fetchone()
            payload['start_time'] = converted_time[0] if converted_time else datetime.strptime(payload.get('data_received_time'), "%Y-%m-%d %H:%M:%S.%f")
            payload['asset_runtime'] = 0
            payload['downtime_reason_code'] = downtime_reason[1] if downtime_reason else None
            payload['downtime_reason_id'] = downtime_reason[0] if downtime_reason else None
            payload['fault_reason_code'] = fault_reason[1] if fault_reason else None
            payload['fault_reason_id'] = fault_reason[0] if fault_reason else None
            eventSequence = insert_eventSequence(payload, conn)
            return True if eventSequence else False
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {event_log.__name__}: {error_info}")
        return error_info


def insert_eventSequence(payload, conn):
    try:
        print(payload)
        cursor = conn.cursor()
        event_attri = [payload.get('fw_tenant_id'), payload.get('sf_asset_id'), payload.get('event_code'),datetime.strftime(payload.get('start_time'), "%Y-%m-%d %H:%M:%S.%f") if payload.get('start_time') else None
                       ,datetime.strftime(payload.get('end_time'), "%Y-%m-%d %H:%M:%S.%f") if payload.get('end_time') else None, payload.get('data_received_time'), payload.get('fw_application_id')
                       ,payload.get('fw_eui_id'), payload.get('asset_status'), payload.get('fault_reason_code'),
                       payload.get('downtime_reason_code'),payload.get('cycle_time'), payload.get('product_code'), payload.get('quantity_produced'),
                       payload.get('quantity_rejected'), payload.get('asset_runtime'), payload.get('fw_eui_id'), datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S.%f"),
                       payload.get('asset_downtime') if payload.get('asset_downtime') else 0]
        print(event_attri)
        cursor.execute(f'''INSERT INTO mm_eventsequence(fw_tenant_id, sf_asset_id, mm_eventtype_code, mm_event_starttime, mm_event_endtime,
                                 mm_event_date, fw_application_id, fw_eui_id, mm_asset_status, mm_fault_reason_code, mm_downtime_reason_code,
                                 mm_cycletime, mm_product_code, mm_quantity_produced, mm_quantity_rejected, mm_asset_runtime, is_active, created_by, created_bytime, mm_asset_downtime)
                                 VALUES  (%s, %s, %s ,%s ,%s,%s, %s, %s, %s,%s, %s, %s, %s,%s,%s, %s, TRUE, %s,%s, %s )''',event_attri)

        conn.commit()
        if payload.get('downtime_reason_barcode') and payload.get('asset_status') != 'up':
            print('hi')
            if payload['downtime_reason_id']:
                print('came')
                availablity_log = available_log(payload, conn)
                return True if availablity_log else False
        elif payload.get('fault_reason_barcode') and payload.get('asset_status') != 'up':
            print('hello')
            if payload['fault_reason_id']:
                print('came')
                availablity_log = available_log(payload, conn)
                return True if availablity_log else False
        elif payload.get('asset_status') == 'up':
            print('ray')
            availablity_log = available_log(payload, conn)
            return True if availablity_log else False
        else:
            print('chutya')
            if payload.get('quantity_produced') or payload.get('quantity_rejected') and payload.get('event_code') in (777, 888, 999):
                cursor.execute(f'''select True from mm_quantity_log where fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id ='{payload['sf_asset_id']}'
                                            and created_bytime::date = '{payload['data_received_time']}'::date and mm_timeslot = {payload['timeslot']}
                                            and case when updated_bytime is Null then created_bytime else updated_bytime end>= '{datetime.strftime(payload.get('asset_previous_stime'), "%Y-%m-%d %H:%M:%S.%f")}' and
                                            case when updated_bytime is Null then created_bytime else updated_bytime end <= '{payload['data_received_time']}' order by mm_available_log_id desc limit 1;''')
                exists_timeslot = cursor.fetchone()
                if exists_timeslot:
                    print('kiran')
                    query = f''', mm_quantity_produced = case when mm_quantity_produced is null then 0 else mm_quantity_produced end+ {payload['quantity_produced']} , 
                                mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end  + {payload['quantity_produced']} ''' if payload.get(
                        'quantity_produced') else ' '
                    query = query + f''', mm_quantity_rejected = case when mm_quantity_rejected is null then 0 else mm_quantity_rejected end + {payload['rejection_prduced']},
                                mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end  - {payload['rejection_prduced']} ''' if payload.get(
                        'rejection_prduced') else query
                    update = sameAsset_status_QTY(payload, query, conn)
                    return True if update else False
                else:
                    print('gattu')
                    QTY = qty_event_log(payload, conn)
                    return True if QTY else False
        return True
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {insert_eventSequence.__name__}: {error_info}")
        return error_info


def available_log(payload, conn):
    try:
        cursor = conn.cursor()
        payload['timeslot'] = int(payload['start_time'].hour) + 1
        print('2.1', payload['timeslot'])
        payload['previous_timeslot'] = int(payload.get('asset_previous_stime').hour) + 1 if payload.get('asset_previous_stime') else None
        print('2.2', payload['previous_timeslot'])
        cursor.execute(f'''select case when mm_runtime>0 then mm_runtime else null end , mm_downtime_reason_id, mm_fault_reason_id from mm_available_log where fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id ='{payload['sf_asset_id']}'
                            and created_bytime::date = '{payload['data_received_time']}'::date and mm_timeslot = {payload['timeslot']}
                            and case when updated_bytime is Null then created_bytime else updated_bytime end>= '{datetime.strftime(payload.get('asset_previous_stime'), "%Y-%m-%d %H:%M:%S.%f")}' and
                            case when updated_bytime is Null then created_bytime else updated_bytime end <= '{payload['data_received_time']}' order by mm_available_log_id desc limit 1;''')
        existing_timeslot = cursor.fetchone()
        print(existing_timeslot)
        cursor.execute(f'''select True from mm_quality_log where fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id ='{payload['sf_asset_id']}'
                                    and created_bytime::date = '{payload['data_received_time']}'::date and mm_timeslot = {payload['timeslot']} and
                                    case when updated_bytime is Null then created_bytime else updated_bytime end>= '{datetime.strftime(payload.get('asset_previous_stime'), "%Y-%m-%d %H:%M:%S.%f")}' and
                                    case when updated_bytime is Null then created_bytime else updated_bytime end <= '{payload['data_received_time']}' order by mm_quality_log_id desc limit 1;''')
        QTY_exists_timeslot = cursor.fetchone()
        print(QTY_exists_timeslot)
        cursor.execute(f'''select True from mm_perform_log where fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id ='{payload['sf_asset_id']}'
                            and created_bytime::date = '{payload['data_received_time']}'::date and mm_timeslot = {payload['timeslot']}
                            and case when updated_bytime is Null then created_bytime else updated_bytime end>= '{datetime.strftime(payload.get('asset_previous_stime'), "%Y-%m-%d %H:%M:%S.%f")}' and
                            case when updated_bytime is Null then created_bytime else updated_bytime end <= '{payload['data_received_time']}' order by mm_perform_log_id desc limit 1;''')

        existing_perf_timeslot = cursor.fetchone()
        runing = existing_timeslot[0] if existing_timeslot else None
        planning = existing_timeslot[1] if existing_timeslot else None
        unplanning = existing_timeslot[2] if existing_timeslot else None
        payload['run_min'] = 0
        payload['unplandown_min'] = 0
        payload['plandown_min'] = 0
        print(existing_timeslot,existing_perf_timeslot,existing_perf_timeslot, runing,planning,unplanning)
        if payload.get('asset_status') == 'down' and payload.get('asset_previous_status') == 'down':
            print("down, down")
            if payload.get('downtime_reason_id') and payload.get('previous_downtime_reason_code'):
                if existing_timeslot:
                    print('loki')
                    payload['plandown_min'] = (datetime.strptime(payload['data_received_time'],"%Y-%m-%d %H:%M:%S.%f") - payload['asset_previous_stime']).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False
                else:
                    print('paki')
                    plandown_date = payload.get('asset_previous_stime').replace(minute=0, second=0) + timedelta(hours=1)
                    payload['plandown_min'] = (plandown_date - payload.get('asset_previous_stime')).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    plandown_date = payload['start_time'].replace(minute=0, second=0)
                    payload['plandown_min'] = (payload['start_time'] - plandown_date).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False

            elif payload.get('fault_reason_id') and payload.get('previous_fault_reason_code'):
                print("fault fault")
                if existing_timeslot:
                    print('loki')
                    payload['unplandown_min'] = (payload['start_time'] - payload['asset_previous_stime']).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False

                else:
                    print('paki')
                    unplandown_date = payload.get('asset_previous_stime').replace(minute=0, second=0) + timedelta(hours=1)
                    payload['unplandown_min'] = (unplandown_date - payload.get('asset_previous_stime')).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    unplandown_date = payload['start_time'].replace(minute=0,second=0)
                    payload['unplandown_min'] = (payload['start_time'] - unplandown_date).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False

            elif payload.get('fault_reason_id') and payload.get('previous_downtime_reason_code'):
                print("fault,  down")
                if existing_timeslot:
                    print('loki')
                    payload['plandown_min'] = (datetime.strptime(payload['data_received_time'],"%Y-%m-%d %H:%M:%S.%f") - payload['asset_previous_stime']).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False

                else:
                    print('paki')
                    plandown_date = payload.get('asset_previous_stime').replace(minute=0, second=0) + timedelta(hours=1)
                    payload['plandown_min'] = (plandown_date - payload.get('asset_previous_stime')).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    plandown_date = payload['start_time'].replace(minute=0, second=0)
                    payload['plandown_min'] = (payload['start_time'] - plandown_date).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False

            elif payload.get('downtime_reason_id') and payload.get('previous_fault_reason_code'):
                print('down fault')
                if existing_timeslot:
                    print('loki')
                    payload['unplandown_min'] = (datetime.strptime(payload['data_received_time'],"%Y-%m-%d %H:%M:%S.%f") - payload['asset_previous_stime']).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False

                else:
                    print('paki')
                    plandown_date = payload.get('asset_previous_stime').replace(minute=0, second=0) + timedelta(hours=1)
                    payload['unplandown_min'] = (plandown_date - payload.get('asset_previous_stime')).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    plandown_date = payload['start_time'].replace(minute=0, second=0)
                    payload['unplandown_min'] = (payload['start_time'] - plandown_date).total_seconds()/60
                    availablity = availabl_event_log(payload, conn)
                    return True if availablity else False

        elif payload.get('asset_status') == 'up' and payload.get('asset_previous_status') == 'up':
            if existing_timeslot:
                run_min = (datetime.strptime(payload.get('data_received_time'), "%Y-%m-%d %H:%M:%S.%f") - payload['asset_previous_stime']).total_seconds()/ 60
                query = f''',mm_runtime= mm_runtime + {run_min} '''
                update = sameAsset_status_Availablity(True, payload, query, conn)
                if existing_perf_timeslot:
                    print('kaki')
                    query = query + f''', mm_quantity_produced =case when mm_quantity_produced is null then 0 else mm_quantity_produced end+ {payload['quantity_produced']}''' if payload.get('quantity_produced') else query
                    update = sameAsset_status_Performance(True, payload, query,conn)
                else:
                    payload['run_min'] = run_min
                    Performance = perf_event_log(payload, conn)
                if payload.get('quantity_produced'):
                    if QTY_exists_timeslot:
                        print('kiran')
                        query = f''', mm_quantity_produced = case when mm_quantity_produced is null then 0 else mm_quantity_produced end+ {payload['quantity_produced']} , 
                                    mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end  + {payload['quantity_produced']} ''' if payload.get('quantity_produced') else ' '
                        query = query + f''', mm_quantity_rejected = case when mm_quantity_rejected is null then 0 else mm_quantity_rejected end + {payload['rejection_prduced']},
                                    mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end  - {payload['rejection_prduced']} ''' if payload.get('rejection_prduced') else query

                        update = sameAsset_status_QTY(payload, query, conn)
                        cursor.execute(update)
                    else:
                        print('gattu')
                        QTY = qty_event_log(payload, conn)
                return True
            else:
                print('lokikiki')
                previourhour = payload.get('asset_previous_stime').replace(minute=0, second=0) + timedelta(hours=1)
                run_min = (previourhour - payload.get('asset_previous_stime')).total_seconds() / 60
                query = f''',mm_runtime= mm_runtime + {run_min} '''
                update = sameAsset_status_Availablity(False, payload, query,conn)
                query = query + f''', mm_quantity_produced =case when mm_quantity_produced is null then 0 else mm_quantity_produced end+ {payload['quantity_produced']}''' if payload.get('quantity_produced') else query
                update = sameAsset_status_Performance(False, payload, query, conn)
                if payload.get('quantity_produced'):
                    print('chut')
                    if QTY_exists_timeslot:
                        query = f''', mm_quantity_produced = case when mm_quantity_produced is null then 0 else mm_quantity_produced end+ {payload['quantity_produced']} , 
                                    mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end  + {payload['quantity_produced']} ''' if payload.get('quantity_produced') else ' '
                        query = query + f''', mm_quantity_rejected = case when mm_quantity_rejected is null then 0 else mm_quantity_rejected end + {payload['rejection_prduced']},
                                    mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end  - {payload['rejection_prduced']} ''' if payload.get('rejection_prduced') else query
                        update = sameAsset_status_QTY(payload, query,conn)
                    else:
                        QTY = qty_event_log(payload, conn)
                lasthour= payload['start_time'].replace(minute=0, second=0)
                payload['run_min'] = (payload['start_time'] - lasthour).total_seconds() / 60
                availablity = availabl_event_log(payload,conn)
                Performance = perf_event_log(payload, conn)
                return True if availablity and Performance else False

        elif payload.get('asset_status') == 'down' and payload.get('asset_previous_status') == 'up':
            print('down up')
            if existing_timeslot:
                print('loki')
                run_time =(datetime.strptime(payload.get('data_received_time'), "%Y-%m-%d %H:%M:%S.%f") - payload['asset_previous_stime']).total_seconds() /60
                payload['run_min'] = run_time
                availablity = availabl_event_log(payload, conn)
                Performance = perf_event_log(payload,conn)
                return True if availablity and Performance else False
            else:
                print('paki')
                previourhour = payload.get('asset_previous_stime').replace(minute=0, second=0) + timedelta(hours=1)
                run_min = (previourhour - payload.get('asset_previous_stime')).total_seconds()/60
                query = f''',mm_runtime= mm_runtime + {run_min} '''
                update = sameAsset_status_Availablity(False, payload, query)
                cursor.execute(update)
                query = query + f''', mm_quantity_produced =case when mm_quantity_produced is null then 0 else mm_quantity_produced end+ {payload['quantity_produced']}''' if payload.get('quantity_produced') else ' '
                update = sameAsset_status_Performance(False, payload, query)
                cursor.execute(update)
                lasthour = payload['start_time'].replace(minute=0, second=0)
                run_time = (payload['start_time'] - lasthour).total_seconds()/60
                payload['run_min'] = run_time
                availablity = availabl_event_log(payload, conn)
                Performance = perf_event_log(payload, conn)
                if payload.get('quantity_produced'):
                    if QTY_exists_timeslot:
                        query = f''', mm_quantity_produced = case when mm_quantity_produced is null then 0 else mm_quantity_produced end+ {payload['quantity_produced']} , 
                                    mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end + {payload['quantity_produced']} ''' if payload.get('quantity_produced') else ' '
                        query = query + f''', mm_quantity_rejected = case when mm_quantity_rejected is null then 0 else mm_quantity_rejected end + {payload['rejection_prduced']},
                                    mm_quality_produced = case when mm_quality_produced is null then 0 else mm_quality_produced end  - {payload['rejection_prduced']} ''' if payload.get('rejection_prduced') else query
                        update = sameAsset_status_QTY(payload, query, conn)
                    else:
                        QTY = qty_event_log(payload, conn)
                return True if availablity and Performance else False

        elif payload.get('asset_status') == 'up' and payload.get('asset_previous_status') == 'down':
            print('up', 'down')
            if existing_timeslot:
                print('loki')
                down_time = (datetime.strptime(payload.get('data_received_time'), "%Y-%m-%d %H:%M:%S.%f") - payload['asset_previous_stime']).total_seconds()/60
                payload['plandown_min'] = down_time if unplanning else 0
                payload['unplandown_min'] = down_time if planning else 0
                availablity = availabl_event_log(payload, conn)
                return True if availablity else False
            else:
                print('paki')
                previourhour = payload.get('asset_previous_stime').replace(minute=0, second=0) + timedelta(hours=1)
                down_time = (previourhour - payload.get('asset_previous_stime')).total_seconds()/60
                payload['plandown_min'] = down_time if unplanning else 0
                payload['unplandown_min'] = down_time if planning else 0
                availablity = availabl_event_log(payload, conn)
                lasthour = payload['start_time'].replace(minute=0, second=0)
                down_time = (lasthour - payload['start_time']).total_seconds()/60
                payload['plandown_min'] = down_time if unplanning else 0
                payload['unplandown_min'] = down_time if planning else 0
                availablity = availabl_event_log(payload, conn)
                return True if availablity else False
        else:
            print('govinda')
            if payload.get('asset_status') == 'up':
                print('chelo')
                availablity = availabl_event_log(payload,conn)
                print('goood to go')
                Performance = perf_event_log(payload,conn)
                return True if availablity and Performance else False
            else:
                availablity = availabl_event_log(payload,conn)
                return True if availablity else False
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {available_log.__name__}: {error_info}")
        return error_info


def sameAsset_status_Availablity(existing_timeslot,payload,query,conn):
    try:
        print(existing_timeslot,payload,query,conn)
        print('pololii')
        cursor = conn.cursor()
        if existing_timeslot:
            print('loki')
            data = f'''UPDATE public.mm_available_log
                       SET updated_by='{payload['fw_eui_id']}',updated_bytime='{payload['data_received_time']}'::timestamp'''
            data = data + query
            data = data + f'''WHERE fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id = {payload['sf_asset_id']} and
                created_bytime::date = '{payload['asset_previous_stime']}'::date and mm_timeslot = {payload['timeslot']}
                and case when updated_bytime is Null then created_bytime else updated_bytime end = '{payload['asset_previous_stime']}';'''
            cursor.execute(data)
            conn.commit()
            return True if data else False
        else:
            print('kaki')
            data = f'''UPDATE public.mm_available_log
                            SET updated_by='{payload['fw_eui_id']}',updated_bytime='{payload['data_received_time']}'::timestamp'''
            data = data + query
            data = data +f'''WHERE fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id = {payload['sf_asset_id']} and 
                created_bytime::date = '{payload['asset_previous_stime']}'::date and mm_timeslot = {payload['previous_timeslot']}
                and case when updated_bytime is Null then created_bytime else updated_bytime end = '{payload['asset_previous_stime']}';'''
            cursor.execute(data)
            conn.commit()
            return True if data else False
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {sameAsset_status_Availablity.__name__}: {error_info}")
        return error_info


def downtime(downtime_reason_barcode, conn):
    try:
        print('down',downtime_reason_barcode, conn )
        cursor = conn.cursor()
        cursor.execute(f'''select mm_downtime_reason_id, mm_downtime_reason_code from mm_downtime_reason where mm_downtime_reason_barcode = %s
         order by mm_downtime_reason_id desc limit 1;''',(downtime_reason_barcode,))
        down_info = cursor.fetchone()
        conn.commit()
        return down_info if down_info else None
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {downtime.__name__}: {error_info}")
        return error_info


def fault(fault_reason_barcode, conn):
    try:
        print('fault',fault_reason_barcode, conn)
        cursor = conn.cursor()
        cursor.execute(f'''select mm_fault_reason_id, mm_fault_reason_code from mm_fault_reason where mm_fault_reason_barcode = %s
         order by mm_fault_reason_id desc limit 1;''', (fault_reason_barcode,))
        fault_info = cursor.fetchone()
        conn.commit()
        return fault_info if fault_info else None
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {fault.__name__}: {error_info}")
        return error_info


def rejection_reason(rejection_reason_barcode, conn):
    try:
        print(rejection_reason_barcode, conn)
        cursor = conn.cursor()
        cursor.execute(f'''select mm_rejection_reason_id, mm_rejection_reason_code from mm_rejection_reason where mm_rejection_reason_barcode = %s
        order by mm_fault_reason_id desc limit 1;''', (rejection_reason_barcode,))
        fault_info = cursor.fetchone()
        conn.commit()
        return fault_info if fault_info else None
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {rejection_reason.__name__}: {error_info}")
        return error_info


def availabl_event_log(payload, conn):
    try:
        print('available', payload, conn)
        cursor = conn.cursor()
        event_attri = [payload.get('fw_tenant_id'), payload.get('sf_asset_id'), 60,
                       payload.get('run_min') if payload.get('run_min') else 0,
                       payload.get('unplandown_min') if payload.get('unplandown_min') else 0,
                       payload.get('plandown_min') if payload.get('plandown_min') else 0,
                       payload.get('downtime_reason_id')
            , payload.get('fault_reason_id'), payload.get('timeslot'), payload.get('fw_eui_id'), payload.get('data_received_time')]
        cursor.execute(f'''INSERT INTO public.mm_available_log(
        fw_tenant_id, sf_asset_id, mm_planprod_time, mm_runtime, mm_unplandown_time, mm_plandown_time, mm_downtime_reason_id, mm_fault_reason_id, mm_timeslot, is_active, created_by, created_bytime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, True, %s, %s);''',event_attri)
        conn.commit()
        return 'True'
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {availabl_event_log.__name__}: {error_info}")
        return error_info


def sameAsset_status_Performance(existing_timeslot, payload, query, conn):
    try:
        print('update', existing_timeslot, payload, query, conn)
        cursor = conn.cursor()
        if existing_timeslot:
            data = f'''UPDATE public.mm_perform_log
                       SET updated_by='{payload['fw_eui_id']}',updated_bytime='{payload['data_received_time']}'::timestamp'''
            data = data + query
            data = data + f'''WHERE fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id = {payload['sf_asset_id']} and
                created_bytime::date = '{payload['asset_previous_stime']}'::date and mm_timeslot = {payload['timeslot']}
                and case when updated_bytime is Null then created_bytime else updated_bytime end = '{payload['asset_previous_stime']}';'''
            print(data)
            cursor.execute(data)
            conn.commit()
            return 'data'
        else:
            data = f'''UPDATE public.mm_perform_log
                            SET updated_by='{payload['fw_eui_id']}',updated_bytime='{payload['data_received_time']}'::timestamp'''
            data = data + query
            data = data + f'''WHERE fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id = {payload['sf_asset_id']} and 
                created_bytime::date = '{payload['asset_previous_stime']}'::date and mm_timeslot = {payload['previous_timeslot']}
                and case when updated_bytime is Null then created_bytime else updated_bytime end = '{payload['asset_previous_stime']}';'''
            cursor.execute(data)
            conn.commit()
            return 'data'
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {sameAsset_status_Performance.__name__}: {error_info}")
        return error_info


def perf_event_log(payload,conn):
    try:
        print('perf',payload,conn)
        cursor =conn.cursor()
        event_attri = [payload.get('fw_tenant_id'), payload.get('sf_asset_id'), payload.get('mm_product_id'),
                       payload.get('cycle_time'), payload.get('quantity_produced'),
                       payload.get('run_min') if payload.get('run_min') else 0,
                       payload.get('timeslot'), payload.get('fw_eui_id'), payload.get('data_received_time')]
        cursor.execute(f'''INSERT INTO public.mm_perform_log(
        fw_tenant_id, sf_asset_id, mm_product_id, mm_cycle_time, mm_quantity_produced, mm_runtime, mm_timeslot, is_active, created_by, created_bytime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, True, %s, %s );''', event_attri)
        conn.commit()
        return 'True'
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {perf_event_log.__name__}: {error_info}")
        return error_info


def qty_event_log(payload, conn):
    try:
        print('payload, conn')
        cursor = conn.cursor()
        quality = payload.get('quantity_produced') if payload.get('quantity_produced') else 0 - payload.get('quantity_rejected') if payload.get('quantity_rejected') else 0
        event_attri = [payload.get('fw_tenant_id'), payload.get('sf_asset_id'), payload.get('mm_product_id'),
                       payload.get('cycle_time'), payload.get('quantity_produced') if payload.get('quantity_produced') else 0,
                       payload.get('quantity_rejected') if payload.get('quantity_rejected') else 0,
                       payload.get('mm_rejection_reason_code'), payload.get('timeslot'), payload.get('fw_eui_id'),
                       payload.get('data_received_time'), quality]
        cursor.execute(f'''INSERT INTO public.mm_quality_log(
        fw_tenant_id, sf_asset_id, mm_product_id, mm_cycle_time, mm_quantity_produced, mm_quantity_rejected, mm_rejection_reason_code, 
        mm_timeslot, is_active, created_by, created_bytime, mm_quality_produced)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, True, %s,%s, %s);''', event_attri)
        conn.commit()
        return 'True'
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {qty_event_log.__name__}: {error_info}")
        return error_info


def sameAsset_status_QTY(payload, query, conn):
    try:
        print('lokoijjk', payload, query, conn)
        cursor = conn.cursor()
        data = f'''UPDATE public.mm_quality_log
                   SET updated_by='{payload['fw_eui_id']}',updated_bytime='{payload['data_received_time']}'::timestamp'''
        data = data + query
        data = data + f'''WHERE fw_tenant_id = {payload['fw_tenant_id']} and sf_asset_id = {payload['sf_asset_id']} and
            created_bytime::date = '{payload['data_received_time']}'::date and mm_timeslot = {payload['timeslot']}
            and case when updated_bytime is Null then created_bytime else updated_bytime end = '{payload['asset_previous_stime']}';'''
        print(data)
        cursor.execute(data)
        return 'True' if data else 'False'
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {sameAsset_status_QTY.__name__}: {error_info}")
        return error_info


def event_listener():
    try:
        dlog_receiver()
        print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        event_receiver()
    except Exception as error_info:
        logger.error(f"An error occurred in {__file__}, function {event_listener.__name__}: {error_info}")
        return error_info


def add_dlog():
    mail_data = {'asset_status': 'up', 'cycle_time': 4, 'product_code': 'biscket', 'event_code': 777, 'fw_tenant_id': 1684, 'fw_eui_id': 'fizo125', 'fw_application_id': 1703,
                 'data_received_time': datetime.strftime(datetime.utcnow()+ timedelta(hours=2), "%Y-%m-%d %H:%M:%S.%f"), 'sf_asset_id': 1, 'task': 'event_log', 'quantity_produced': 4}
    event_receiver(mail_data)
    return {'succesfully': "sent mail"}


event_listener()
# , 'fault_reason_barcode': 12345
# event_receiver()