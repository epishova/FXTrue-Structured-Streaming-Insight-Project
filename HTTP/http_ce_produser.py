import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# Specify your credentials to connect to TrueFX:
YOURUSERNAME = 
YOURPASSWORD = 

def establish_connection():
    r = requests.get("http://webrates.truefx.com/rates/connect.html?u="+YOURUSERNAME+"&p="+YOURPASSWORD"&q=ozrates&f=csv")
    if r.status_code == requests.codes.ok:
        print('HTTP request is OK')
        request_body = "http://webrates.truefx.com/rates/connect.html?id=" + r.text.strip()
    else:
        request_body = ""
    return(request_body)

# Define how many attempts of getting data from the provider to do before reconnecting: 
session_expired_limit = 500
session_expired = 0

# Define settings to connect to Kafka:
topic = 'currency_exchange'
broker = 'ec2-18-209-75-68.compute-1.amazonaws.com:9092, ec2-18-205-142-57.compute-1.amazonaws.com:9092, ec2-50-17-32-144.compute-1.amazonaws.com:9092'
producer = KafkaProducer(bootstrap_servers = broker)
print('Start sending messages')

request_body = establish_connection()
# While not terminated keep getting msgs from provider and sending them to Kafka
while True:
    rates = requests.get(request_body)
    if rates.status_code != requests.codes.ok:
        print("Failed to connect to data provider")
    else:
        jsons = ''
        for rate in rates.text.strip().split("\n"):
            values = rate.split(",")
            # Check schema of incomming msgs:
            if len(values) < 9:
                print("GET returned unexpected msg. Nothing to send to pipeline")
                session_expired += 1
                if session_expired > session_expired_limit:
                    request_body = establish_connection()
                    session_expired = 0
                continue
            msg = {'fx_marker': values[0], 'timestamp_ms': values[1], 'bid_big': values[2], 'bid_points': values[3], 'offer_big': values[4], 'offer_points': values[5], 'hight': values[6], 'low': values[7], 'open': values[8]}
            jsons += json.dumps(msg) + "\n"

        # It is more efficient to send one big msg then many small msgs:
        jsons = jsons.strip()
        if jsons == "":
            continue
        future = producer.send(topic, str.encode(jsons))
        try:
            record_metadata = future.get(timeout = 60) #ms
        except KafkaError:
            #log.exception()
            print('Message failed')
            pass
