# -*- coding: utf-8 -*-
"""
Created on Sat Jan 22 13:56:16 2022

@author: Larissa
"""

import boto3
import json
from fake_web_events import Simulation
from credentials import ACCESS_KEY, SECRET_KEY

# estabelecendo a conexão com o serviço:
client=boto3.client(
    'firehose', 
    region_name='us-east-1',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
    )

def put_record(event: dict):
    # recebendo o evento:
    data=json.dumps(event)+"\n"
    # colocando os dados no firehose:
    response=client.put_record(
        DeliveryStreamName='egd-Larissa', # nome da instância criada na aws
        Record={"Data": data}
        )
    
    print(event)
    return response

simulation=Simulation(user_pool_size=100, sessions_per_day=100000)
events=simulation.run(duration_seconds=300)

for event in events:
    put_record(event)