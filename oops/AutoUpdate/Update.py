import psycopg2
import random
import time
import json

def random2():
    conn = psycopg2.connect(
        host='dev-fogwing-asset.westindia.cloudapp.azure.com',
        user='fogwingasset',
        password='FogwingAsset',
        port=5432,
        dbname="assetplus"
    )
    cursor = conn.cursor()
    count = 0
    while True:
        # payload = json.dumps({"EUI_ID": 1001, "qty_produced": random.randint(10,100), "qty_rejected": random.randint(1,10),
        #            "asset_runtime": random.randint(1, 60), "asset_status": "Running"})
        #
        # query = '''INSERT INTO public.sf_asset_datalog(
        # fw_tenant_id, sf_plant_id, sf_work_centre_id, sf_asset_id, sf_asset_payload, is_active_data, created_by,
        # created_by_date, fw_data_received_time, sf_job_id)
        # VALUES (111,1112,3423, 123, '{}'::jsonb, true, 'random', now(), now(), 99);'''.format(payload)
        # payload = json.dumps({"EUI_ID": 1001, "qty_produced": random.randint(10,100), "qty_rejected": random.randint(1,10),
        #            "asset_runtime": random.randint(1, 60), "asset_status": "Running"})
        # '2023-06-0' + str(random.randint(1, 8)) + ' ' + str(random.randint(10, 23)) + ':' + str(
        #     random.randint(10, 59)) + ':' + str(random.randint(10, 59))
        payload = {"org_id": 1059, "tenant_id": 1684, "application_id": 1703, "gateway_id": 2136, "client_id": "1059-1684-1703-2136", "data_received_time": "2023-06-13 13:37:42.584986", "created_by": "f0a2c604752f4bf7",
                   "payload": {"temperature": 6.5, "humidity": 78, "door": "Open", "quality": 10, "Down": True},
                   "tag": "production", "dlog_uuid": "Bb2GKg5B4SNnARe3mp2i4L", "source": "Simulator", "is_gtw": True, "dlog_id": 131151}
        pay = json.dumps(payload['payload'])
        query = f'''INSERT INTO public.mm_datalog(
        fw_tenant_id, fw_org_id, fw_gateway_id, fw_application_id, client_id, device_eui_id, fw_datalog_uuid, mm_payload_attribute, tag, source, data_recived_time, is_active_data, created_by, created_by_date)
        VALUES ({payload['tenant_id']}, {payload['org_id']}, {payload['gateway_id']},{payload['application_id']}, '{payload['client_id']}','{payload['created_by']}', '{payload['dlog_uuid']}'
        , '{pay}', '{payload['tag']}', '{payload['source']}', now()::timestamp, True
        , '{payload['created_by']}', now()::timestamp);'''
        # query = '''INSERT INTO public.mm_asset_perfparam_by_shift(mm_tenant_id, oee, performance, availablity, is_active, created_by_date)
        # VALUES (22345,{}, {}, {}, True, now()::timestamp);'''.format(random.randint(0,100),random.randint(0,100),random.randint(0,100))
        count += 1
        print(query)
        cursor.execute(query)
        # Commit the transactionsss
        conn.commit()
        # Wait for 1 second
        time.sleep(10)



random2()


def good():
    from MMDatalogTrigger.task import dlog_receiver
    print(dlog_receiver())