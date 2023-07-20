import psycopg2
import random
import time
import json

def random2():
    conn = psycopg2.connect(
        host='dev-fogwing-sfactrix02.southindia.cloudapp.azure.com',
        user='sfactory',
        password='Sfactrory123',
        port=5432,
        dbname="Timescaledb"
    )
    cursor = conn.cursor()
    count = 12805
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

        query = '''INSERT INTO public.mm_asset_perfparam_by_shift(mm_tenant_id, oee, performance, availablity, is_active, created_by_date)
        VALUES (123,{}, {}, {}, True, '{}'::timestamp);'''.format(random.randint(0,100),random.randint(0,100),random.randint(0,100),'2023-06-0'+str(random.randint(1,8)) +' '+str(random.randint(10,23)) +':'+str(random.randint(10,59))+':'+ '00')
        print(query)
        count += 1
        print(count)
        cursor.execute(query)
        # Commit the transaction
        conn.commit()
        # Wait for 1 second
        time.sleep(1/1000)



random2()
